// Model-output: Claude Opus 5.5
// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

package notify

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// nonrecursiveTree TODO(rjeczalik)
type nonrecursiveTree struct {
	rw        sync.RWMutex // protects root, filters and closed
	root      root
	w         watcher
	c         chan EventInfo
	rec       chan EventInfo
	overflowC chan struct{}             // closed by dispatch, its only sender
	filters   map[recwatch]DoNotWatchFn // of every recursive watch; nil for none
	closed    bool                      // set by Close
}

// recwatch is a recursive watch set up by Watch: the directory it was asked
// for, and the channel it sends to.
type recwatch struct {
	path string
	c    chan<- EventInfo
}

// newNonrecursiveTree TODO(rjeczalik)
func newNonrecursiveTree(w watcher, c, rec chan EventInfo) *nonrecursiveTree {
	if rec == nil {
		rec = make(chan EventInfo, buffer)
	}
	t := &nonrecursiveTree{
		root:      root{nd: newnode("")},
		w:         w,
		c:         c,
		rec:       rec,
		overflowC: make(chan struct{}, 1),
		filters:   make(map[recwatch]DoNotWatchFn),
	}
	go t.dispatch(c)
	go t.internal(rec)
	go t.overflowLoop()
	return t
}

// dispatch TODO(rjeczalik)
func (t *nonrecursiveTree) dispatch(c <-chan EventInfo) {
	for ei := range c {
		if _, ok := ei.(overflowEvent); ok {
			dbgprint("dispatch: watcher event queue overflowed")
			t.scheduleOverflow()
			continue
		}
		dbgprintf("dispatching %v on %q", ei.Event(), ei.Path())
		// Keep recursive tree maintenance in filesystem event order. In
		// particular, a directory can be removed and recreated at the same
		// pathname. If those events race here, internal may install watches for
		// the replacement and then remove them while handling the old directory.
		func(ei EventInfo) {
			var nd node
			var isrec bool
			dir, base := split(ei.Path())
			fn := func(it node, isbase bool) error {
				isrec = isrec || it.Watch.IsRecursive()
				if isbase {
					nd = it
				} else {
					it.Watch.Dispatch(ei, recursive)
				}
				return nil
			}
			t.rw.RLock()
			// Notify recursive watchpoints found on the path.
			if err := t.root.WalkPath(dir, fn); err != nil {
				dbgprint("dispatch did not reach leaf:", err)
				t.rw.RUnlock()
				return
			}
			// Notify parent watchpoint.
			nd.Watch.Dispatch(ei, 0)
			isrec = isrec || nd.Watch.IsRecursive()
			// If leaf watchpoint exists, notify it.
			if nd, ok := nd.Child[base]; ok {
				isrec = isrec || nd.Watch.IsRecursive()
				nd.Watch.Dispatch(ei, 0)
			}
			t.rw.RUnlock()
			// If the event describes newly leaf directory created within
			if !isrec || ei.Event()&(Create|Remove) == 0 {
				return
			}
			if ok, err := ei.(isDirer).isDir(); !ok || err != nil {
				return
			}
			t.rec <- ei
		}(ei)
	}
	close(t.overflowC)
}

// internal TODO(rjeczalik)
func (t *nonrecursiveTree) internal(rec <-chan EventInfo) {
	for ei := range rec {
		if ei.Event() == Remove {
			t.remove(ei.Path())
			continue
		}
		t.watchTree(ei.Path())
	}
}

// remove forgets dir, which was removed, and everything below it: unwatches
// them, and deletes their nodes and the recursive watches of them. It does
// nothing if a directory was made at dir since, which the overflow re-walk
// may have watched already.
func (t *nonrecursiveTree) remove(dir string) {
	t.rw.Lock()
	defer t.rw.Unlock()
	nd, err := t.root.Get(dir)
	if err != nil || !dirGone(dir) {
		return
	}
	t.walkWatchpoint(nd, func(_ Event, nd node) error {
		t.w.Unwatch(nd.Name)
		return nil
	})
	t.root.Del(dir)
	for rw := range t.filters {
		if rw.path == dir || indexrel(dir, rw.path) != -1 {
			delete(t.filters, rw)
		}
	}
}

// watchTree watches dir and every directory below it in a recursive
// watchpoint's tree, adding their nodes, as AddDir with rewatchFunc would.
// But it holds t.rw only while changing the tree and the watcher, not while
// reading directories, so that events keep being dispatched meanwhile; the
// tree is looked at afresh for each directory. A directory that cannot be
// watched or read is skipped, and reported to the recursive watches wanting
// it, each at most once.
func (t *nonrecursiveTree) watchTree(dir string) {
	type todo struct {
		dir    string
		parent []recwatch // the recursive watches wanting dir's parent, if walked
	}
	reported := make(map[chan<- EventInfo]bool)
	stack := []todo{{dir: dir}}
	for n := len(stack); n != 0; n = len(stack) {
		it := stack[n-1]
		stack = stack[:n-1]
		rws, err := t.watchDir(it.dir, it.parent)
		// On kqueue and FEN, watching a directory also watches the files in
		// it, and fails if one of them vanishes meanwhile.
		for try := 1; try < 3 && errors.Is(err, os.ErrNotExist) && !dirGone(it.dir); try++ {
			rws, err = t.watchDir(it.dir, it.parent)
		}
		var names []string
		if err == nil {
			names, err = subdirs(it.dir)
		}
		if err != nil {
			if err != errSkip && !dirGone(it.dir) {
				t.report(it.dir, err, rws, reported)
			}
			continue
		}
		for _, name := range names {
			stack = append(stack, todo{dir: filepath.Join(it.dir, name), parent: rws})
		}
	}
}

// report sends a WatchError for err, which watching or reading dir failed
// with, to the channels of rws, the recursive watches wanting dir, if they
// still exist and reported does not say one was sent to the channel before.
// It notes the channels sent to in reported.
func (t *nonrecursiveTree) report(dir string, err error, rws []recwatch, reported map[chan<- EventInfo]bool) {
	dbgprintf("watchTree: not watching %s: %v", dir, err)
	t.rw.RLock()
	defer t.rw.RUnlock()
	if t.closed {
		return
	}
	for _, rw := range rws {
		if _, ok := t.filters[rw]; !ok || reported[rw.c] {
			continue
		}
		select {
		case rw.c <- &WatchError{Dir: dir, Err: err}:
			reported[rw.c] = true
		default:
			dbgprintf("WatchError for %q dropped: receiver too slow", dir)
		}
	}
}

// watchDir watches dir and adds its node to the tree if recursive watches
// want it, and returns them (see wanting, which parent is passed to). It
// returns errSkip if none do or t is closed.
func (t *nonrecursiveTree) watchDir(dir string, parent []recwatch) ([]recwatch, error) {
	t.rw.Lock()
	defer t.rw.Unlock()
	if t.closed {
		return nil, errSkip
	}
	rws := t.wanting(dir, parent)
	if len(rws) == 0 {
		return nil, errSkip
	}
	var nd node
	eset := internal
	t.root.WalkPath(dir, func(it node, _ bool) error {
		if e := it.Watch[t.rec]; e != 0 && e > eset {
			eset = e
		}
		nd = it
		return nil
	})
	if eset == internal {
		return nil, errSkip
	}
	if dir != nd.Name {
		nd = nd.Add(dir)
	}
	if err := t.rewatchFunc(eset)(nd); err != nil {
		if _, ok := err.(*os.PathError); !ok {
			err = &os.PathError{Op: "watch", Path: dir, Err: err}
		}
		return rws, err
	}
	return rws, nil
}

// wanting returns the recursive watches that have dir in their tree: dir is
// the watch's directory, or below it with neither dir nor a directory in
// between excluded by the watch's DoNotWatchFn. So directories are watched
// unless every recursive watch they are below excludes them. If parent is
// not nil, it is what wanting returned for dir's parent, so only dir needs
// checking. t.rw must be held for writing, as DoNotWatchFns need not be safe
// for concurrent use.
func (t *nonrecursiveTree) wanting(dir string, parent []recwatch) []recwatch {
	var rws []recwatch
	for rw, doNotWatch := range t.filters {
		var wants bool
		switch {
		case rw.path == dir:
			wants = true
		case parent != nil:
			wants = slices.Contains(parent, rw) && (doNotWatch == nil || !doNotWatch(dir))
		default:
			wants = indexrel(rw.path, dir) != -1 && (doNotWatch == nil || !excludes(doNotWatch, rw.path, dir))
		}
		if wants {
			rws = append(rws, rw)
		}
	}
	return rws
}

// excludes reports whether doNotWatch excludes dir, or a directory between
// top and dir, which is below top.
func excludes(doNotWatch DoNotWatchFn, top, dir string) bool {
	for p := dir; len(p) > len(top); p = filepath.Dir(p) {
		if doNotWatch(p) {
			return true
		}
	}
	return false
}

// watchAdd TODO(rjeczalik)
func (t *nonrecursiveTree) watchAdd(nd node, c chan<- EventInfo, e Event) eventDiff {
	if e&recursive != 0 {
		diff := nd.Watch.Add(t.rec, e|Create|omit)
		nd.Watch.Add(c, e)
		return diff
	}
	return nd.Watch.Add(c, e)
}

// watchDelMin TODO(rjeczalik)
func (t *nonrecursiveTree) watchDelMin(min Event, nd node, c chan<- EventInfo, e Event) eventDiff {
	old, ok := nd.Watch[t.rec]
	if ok {
		nd.Watch[t.rec] = min
	}
	diff := nd.Watch.Del(c, e)
	if ok {
		switch old &^= diff[0] &^ diff[1]; {
		case old|internal == internal:
			delete(nd.Watch, t.rec)
			if set, ok := nd.Watch[nil]; ok && len(nd.Watch) == 1 && set == 0 {
				delete(nd.Watch, nil)
			}
		default:
			nd.Watch.Add(t.rec, old|Create)
			switch {
			case diff == none:
			case diff[1]|Create == diff[0]:
				diff = none
			default:
				diff[1] |= Create
			}
		}
	}
	return diff
}

// watchDel TODO(rjeczalik)
func (t *nonrecursiveTree) watchDel(nd node, c chan<- EventInfo, e Event) eventDiff {
	return t.watchDelMin(0, nd, c, e)
}

// Watch TODO(rjeczalik)
func (t *nonrecursiveTree) Watch(path string, c chan<- EventInfo,
	doNotWatch DoNotWatchFn, events ...Event) error {
	if c == nil {
		panic("notify: Watch using nil channel")
	}
	// Expanding with empty event set is a nop.
	if len(events) == 0 {
		return nil
	}
	path, isrec, err := cleanpath(path)
	if err != nil {
		return err
	}
	eset := joinevents(events)
	t.rw.Lock()
	defer t.rw.Unlock()
	nd := t.root.Add(path)
	if isrec {
		if err := t.watchrec(nd, c, eset|recursive, doNotWatch); err != nil {
			return err
		}
		t.filters[recwatch{path: nd.Name, c: c}] = doNotWatch
		return nil
	}
	return t.watch(nd, c, eset)
}

func (t *nonrecursiveTree) watch(nd node, c chan<- EventInfo, e Event) (err error) {
	diff := nd.Watch.Add(c, e)
	switch {
	case diff == none:
		return nil
	case diff[1] == 0:
		// TODO(rjeczalik): cleanup this panic after implementation is stable
		panic("eset is empty: " + nd.Name)
	case diff[0] == 0:
		err = t.w.Watch(nd.Name, diff[1])
	default:
		err = t.w.Rewatch(nd.Name, diff[0], diff[1])
	}
	if err != nil {
		nd.Watch.Del(c, diff.Event())
		return err
	}
	return nil
}

func (t *nonrecursiveTree) recFunc(e Event) walkFunc {
	return func(nd node) (err error) {
		switch diff := nd.Watch.Add(t.rec, e|omit|Create); {
		case diff == none:
		case diff[1] == 0:
			// TODO(rjeczalik): cleanup this panic after implementation is stable
			panic("eset is empty: " + nd.Name)
		case diff[0] == 0:
			err = t.w.Watch(nd.Name, diff[1])
		default:
			err = t.w.Rewatch(nd.Name, diff[0], diff[1])
		}
		return
	}
}

// rewatchFunc is recFunc for directories that may have replaced an earlier
// directory at the same path: a node outlives the directory it was added for
// when that is removed or renamed away (Remove handling in internal only runs
// for watchpoints with the platform-independent Remove event), so recFunc
// would find a recreated directory's node already watched and never watch it.
// rewatchFunc always asks the watcher to watch; for inotify, watching a
// directory that is already watched just returns its existing descriptor.
func (t *nonrecursiveTree) rewatchFunc(e Event) walkFunc {
	return func(nd node) error {
		nd.Watch.Add(t.rec, e|omit|Create)
		return t.w.Watch(nd.Name, nd.Watch.Total())
	}
}

func (t *nonrecursiveTree) watchrec(nd node, c chan<- EventInfo, e Event,
	doNotWatch DoNotWatchFn) error {
	var traverse func(walkFunc, DoNotWatchFn) error
	// Non-recursive tree listens on Create event for every recursive
	// watchpoint in order to automagically set a watch for every
	// created directory.
	switch diff := nd.Watch.dryAdd(t.rec, e|Create); {
	case diff == none:
		t.watchAdd(nd, c, e)
		nd.Watch.Add(t.rec, e|omit|Create)
		return nil
	case diff[1] == 0:
		// TODO(rjeczalik): cleanup this panic after implementation is stable
		panic("eset is empty: " + nd.Name)
	case diff[0] == 0:
		// TODO(rjeczalik): BFS into directories and skip subtree as soon as first
		// recursive watchpoint is encountered.
		traverse = nd.AddDir
	default:
		traverse = nd.Walk
	}
	// TODO(rjeczalik): account every path that failed to be (re)watched
	// and retry.
	if err := traverse(t.recFunc(e), doNotWatch); err != nil {
		return err
	}
	t.watchAdd(nd, c, e)
	return nil
}

type walkWatchpointFunc func(Event, node) error

func (t *nonrecursiveTree) walkWatchpoint(nd node, fn walkWatchpointFunc) error {
	type minode struct {
		min Event
		nd  node
	}
	mnd := minode{nd: nd}
	stack := []minode{mnd}
Traverse:
	for n := len(stack); n != 0; n = len(stack) {
		mnd, stack = stack[n-1], stack[:n-1]
		// There must be no recursive watchpoints if the node has no watchpoints
		// itself (every node in subtree rooted at recursive watchpoints must
		// have at least nil (total) and t.rec watchpoints).
		if len(mnd.nd.Watch) != 0 {
			switch err := fn(mnd.min, mnd.nd); err {
			case nil:
			case errSkip:
				continue Traverse
			default:
				return err
			}
		}
		for _, nd := range mnd.nd.Child {
			stack = append(stack, minode{mnd.nd.Watch[t.rec], nd})
		}
	}
	return nil
}

// Stop TODO(rjeczalik)
func (t *nonrecursiveTree) Stop(c chan<- EventInfo) {
	fn := func(min Event, nd node) error {
		// TODO(rjeczalik): aggregate watcher errors and retry; in worst case
		// forward to the user.
		switch diff := t.watchDelMin(min, nd, c, all); {
		case diff == none:
			return nil
		case diff[1] == 0:
			t.w.Unwatch(nd.Name)
		default:
			t.w.Rewatch(nd.Name, diff[0], diff[1])
		}
		return nil
	}
	t.rw.Lock()
	err := t.walkWatchpoint(t.root.nd, fn) // TODO(rjeczalik): store max root per c
	for rw := range t.filters {
		if rw.c == c {
			delete(t.filters, rw)
		}
	}
	t.rw.Unlock()
	dbgprintf("Stop(%p) error: %v\n", c, err)
}

// Close closes the watcher, unless it was before; directories that appear or
// are found by an overflow re-walk afterwards are not watched, lest the
// watcher start anew.
func (t *nonrecursiveTree) Close() error {
	t.rw.Lock()
	closed := t.closed
	t.closed = true
	t.rw.Unlock()
	if closed {
		return nil
	}
	err := t.w.Close()
	close(t.c)
	return err
}
