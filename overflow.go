// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

package notify

// overflowEvent is an internal event used to signal that the underlying
// watcher lost an unknown number of events, e.g. due to an inotify kernel
// queue overflow (IN_Q_OVERFLOW). Watcher implementations send it with an
// empty path; when forwarded to user channels the path is set to the path
// of the node the receiving channel is registered at, so that the user can
// rescan that subtree.
type overflowEvent struct {
	path string
}

func (e overflowEvent) Event() Event         { return 0 }
func (e overflowEvent) Path() string         { return e.path }
func (e overflowEvent) Sys() interface{}     { return nil }
func (e overflowEvent) isDir() (bool, error) { return false, nil }

// scheduleOverflow requests asynchronous overflow handling. Multiple
// requests arriving while one is being handled coalesce into a single
// additional run.
func (t *nonrecursiveTree) scheduleOverflow() {
	select {
	case t.overflowC <- struct{}{}:
	default:
	}
}

// overflowLoop runs overflow handling outside the dispatch loop, so that
// event dispatching (and thus the watcher's send goroutine) is never
// blocked on the filesystem re-walk. It exits when overflowC is closed.
func (t *nonrecursiveTree) overflowLoop() {
	for range t.overflowC {
		t.handleOverflow()
	}
}

// handleOverflow recovers from lost watcher events in two steps:
//
// Directory creation events may have been among the lost ones, in which
// case the created directories were never watched and all future events
// below them would be lost as well. Therefore every outermost recursive
// watchpoint subtree is re-walked, adding watches for any directories
// missing from the tree (nd.AddDir with t.recFunc is a no-op for already
// watched directories).
//
// Since it is unknown which events were lost, every user channel is then
// notified with an overflowEvent carrying the path of the node it is
// registered at, prompting a rescan of that subtree.
func (t *nonrecursiveTree) handleOverflow() {
	type sub struct {
		ch   chan<- EventInfo
		path string
	}
	var subs []sub
	t.rw.Lock()
	err := t.walkWatchpoint(t.root.nd, func(min Event, nd node) error {
		for ch := range nd.Watch {
			if ch == nil || ch == t.rec {
				continue
			}
			subs = append(subs, sub{ch: ch, path: nd.Name})
		}
		if eset := nd.Watch[t.rec]; min&recursive == 0 && eset&recursive != 0 {
			if err := nd.AddDir(t.recFunc(eset), nil); err != nil {
				dbgprintf("overflow re-walk of %q failed: %v", nd.Name, err)
			}
		}
		return nil
	})
	t.rw.Unlock()
	if err != nil {
		dbgprintf("overflow tree walk error: %v", err)
	}
	for _, s := range subs {
		select {
		case s.ch <- overflowEvent{path: s.path}:
		default:
			dbgprintf("overflow notification dropped for %q: receiver too slow", s.path)
		}
	}
}
