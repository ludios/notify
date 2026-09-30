// Model-output: Claude Opus 5.5
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
// blocked on the filesystem re-walk. It exits when dispatch closes overflowC.
func (t *nonrecursiveTree) overflowLoop() {
	for range t.overflowC {
		t.handleOverflow()
	}
}

// handleOverflow recovers from lost watcher events in two steps:
//
// Directory creation events may have been among the lost ones, in which
// case the created directories were never watched and all future events
// below them would be lost as well. Therefore every recursive watch's tree
// is walked again with watchTree, watching every directory in it
// (rewatchFunc also covers directories recreated at the path of a node
// left behind by a removed one). Trees of recursive watches within others
// are walked twice, but a tree can have parts only an inner watch wants.
//
// Since it is unknown which events were lost, every user channel is then
// notified with an overflowEvent carrying the path of the node it is
// registered at, prompting a rescan of that subtree. That happens under
// t.rw, so that no channel is notified once Stop for it has returned.
func (t *nonrecursiveTree) handleOverflow() {
	roots := make(map[string]bool)
	t.rw.RLock()
	for rw := range t.filters {
		roots[rw.path] = true
	}
	t.rw.RUnlock()
	for root := range roots {
		t.watchTree(root)
	}

	t.rw.RLock()
	defer t.rw.RUnlock()
	if t.closed {
		return
	}
	t.walkWatchpoint(t.root.nd, func(_ Event, nd node) error {
		for ch := range nd.Watch {
			if ch == nil || ch == t.rec {
				continue
			}
			select {
			case ch <- overflowEvent{path: nd.Name}:
			default:
				dbgprintf("overflow notification dropped for %q: receiver too slow", nd.Name)
			}
		}
		return nil
	})
}
