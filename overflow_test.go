// Model-output: Claude Opus 5.5
// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

package notify

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// faultySpy is a Spy watcher, which records Watch calls and never delivers
// filesystem events, that also fails to watch some paths.
type faultySpy struct {
	*Spy
	full      map[string]bool // Watch fails with ENOSPC
	vanishing map[string]int  // Watch fails with ENOENT so many times, as on kqueue when a file vanishes
}

func (s faultySpy) Watch(p string, e Event) error {
	if s.full[p] {
		return syscall.ENOSPC
	}
	if s.vanishing[p] > 0 {
		s.vanishing[p]--
		return syscall.ENOENT
	}
	return s.Spy.Watch(p, e)
}

// watched reports whether s was asked to watch path.
func (s faultySpy) watched(path string) bool {
	for _, call := range *s.Spy {
		if call.F == FuncWatch && call.P == path {
			return true
		}
	}
	return false
}

// newSpyTree returns a non-recursive tree over a faultySpy, and the channel
// the watcher would send to. As the Spy delivers no events, directories
// created after watching are as if their events were lost in a queue
// overflow.
func newSpyTree(t *testing.T) (*nonrecursiveTree, faultySpy, chan EventInfo) {
	spy := faultySpy{Spy: &Spy{}, full: make(map[string]bool), vanishing: make(map[string]int)}
	c := make(chan EventInfo, buffer)
	tr := newNonrecursiveTree(spy, c, nil)
	t.Cleanup(func() { tr.Close() })
	return tr, spy, c
}

// overflow simulates the watcher's event queue overflowing by sending to c,
// and returns what user got up to and including its overflow notification.
func overflow(t *testing.T, c, user chan EventInfo) []EventInfo {
	t.Helper()
	c <- overflowEvent{}
	var got []EventInfo
	timeout := time.After(5 * time.Second)
	for {
		select {
		case ei := <-user:
			got = append(got, ei)
			if _, ok := ei.(overflowEvent); ok {
				return got
			}
		case <-timeout:
			t.Fatalf("timed out waiting for overflow notification; got %v", got)
		}
	}
}

func mkdirs(t *testing.T, root string, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(root, d), 0777); err != nil {
			t.Fatal(err)
		}
	}
}

// TestOverflow verifies recovery from a watcher event queue overflow
// (e.g. inotify IN_Q_OVERFLOW): directories created while events were
// being lost must get watched retroactively, and every user channel must
// receive an overflowEvent for the subtree it is registered at, so that
// callers can rescan.
func TestOverflow(t *testing.T) {
	tmp := t.TempDir()
	mkdirs(t, tmp, "a")
	tr, spy, c := newSpyTree(t)
	userCh := make(chan EventInfo, 16)
	if err := tr.Watch(tmp+"/...", userCh, nil, All); err != nil {
		t.Fatalf("Watch(%q)=%v", tmp, err)
	}

	// Created after the initial walk, so as if its mkdir event was lost.
	lost := filepath.Join(tmp, "b")
	mkdirs(t, tmp, "b")
	if spy.watched(lost) {
		t.Fatalf("%q watched before overflow handling; test is broken", lost)
	}

	got := overflow(t, c, userCh)
	if len(got) != 1 || got[0].Path() != tmp {
		t.Fatalf("got %v; want only an overflow notification for %q", got, tmp)
	}
	// The overflow notification is sent after the re-walk completed, so
	// the repair watch must have been recorded by now.
	if !spy.watched(lost) {
		t.Fatalf("%q was not watched during overflow recovery; calls: %v", lost, *spy.Spy)
	}
}

// The overflow re-walk must leave out what the recursive watch's DoNotWatchFn
// excludes, as watching did, but not what another recursive watch wants.
func TestOverflowKeepsFilter(t *testing.T) {
	tmp := t.TempDir()
	tr, spy, c := newSpyTree(t)
	userCh := make(chan EventInfo, 16)
	doNotWatch := func(p string) bool { return filepath.Base(p) == "ign" }
	if err := tr.Watch(tmp+"/...", userCh, doNotWatch, All); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(tmp, "other")
	mkdirs(t, tmp, "ign/deep", "keep", "other")
	if err := tr.Watch(other+"/...", make(chan EventInfo, 16), nil, All); err != nil {
		t.Fatal(err)
	}
	mkdirs(t, tmp, "other/ign")

	overflow(t, c, userCh)
	for _, d := range []string{"ign", "ign/deep"} {
		if p := filepath.Join(tmp, d); spy.watched(p) {
			t.Errorf("%q is excluded but was watched", p)
		}
	}
	for _, d := range []string{"keep", "other/ign"} {
		if p := filepath.Join(tmp, d); !spy.watched(p) {
			t.Errorf("%q was not watched", p)
		}
	}
}

// Directories that cannot be watched or read in the overflow re-walk are
// skipped, and reported to the watch with one WatchError, while the walk goes
// on.
func TestOverflowReportsWatchErrors(t *testing.T) {
	tmp := t.TempDir()
	full, locked := filepath.Join(tmp, "full"), filepath.Join(tmp, "locked")
	tr, spy, c := newSpyTree(t)
	spy.full[full] = true
	userCh := make(chan EventInfo, 16)
	if err := tr.Watch(tmp+"/...", userCh, nil, All); err != nil {
		t.Fatal(err)
	}
	mkdirs(t, tmp, "full/sub", "locked", "keep")
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(locked, 0755)

	got := overflow(t, c, userCh)
	if len(got) != 2 {
		t.Fatalf("got %v; want one WatchError, then the overflow notification", got)
	}
	werr, ok := got[0].(*WatchError)
	switch {
	case !ok:
		t.Fatalf("got %v; want a WatchError", got[0])
	case werr.Path() == full && errors.Is(werr, syscall.ENOSPC):
	case werr.Path() == locked && errors.Is(werr, os.ErrPermission):
	default:
		t.Fatalf("got %v", werr)
	}
	if spy.watched(filepath.Join(full, "sub")) {
		t.Error("the directory that failed to be watched was walked")
	}
	if !spy.watched(filepath.Join(tmp, "keep")) {
		t.Error("the walk stopped at a failing directory")
	}
}

// Closing the tree while the watcher's channel still holds overflow events
// must not panic, and must stop overflow handling from watching directories
// or notifying channels.
func TestCloseDuringOverflow(t *testing.T) {
	tmp := t.TempDir()
	for i := 0; i < 100; i++ {
		c := make(chan EventInfo, buffer)
		for len(c) < cap(c) {
			c <- overflowEvent{}
		}
		newNonrecursiveTree(&Spy{}, c, nil).Close()
	}

	tr, spy, _ := newSpyTree(t)
	userCh := make(chan EventInfo, 16)
	if err := tr.Watch(tmp+"/...", userCh, nil, All); err != nil {
		t.Fatal(err)
	}
	mkdirs(t, tmp, "new")
	tr.Close()
	tr.handleOverflow()
	if spy.watched(filepath.Join(tmp, "new")) {
		t.Error("a directory was watched after Close")
	}
	if len(userCh) != 0 {
		t.Errorf("got %v after Close", <-userCh)
	}
}

// The trees of recursive watches within others are walked again too, also
// where the outer watch excludes the directory they are in.
func TestOverflowWalksInnerWatches(t *testing.T) {
	tmp := t.TempDir()
	mkdirs(t, tmp, "ign/deep")
	tr, spy, c := newSpyTree(t)
	watch := func(dir string, doNotWatch DoNotWatchFn) chan EventInfo {
		t.Helper()
		ch := make(chan EventInfo, 16)
		if err := tr.Watch(filepath.Join(tmp, dir, "..."), ch, doNotWatch, All); err != nil {
			t.Fatal(err)
		}
		return ch
	}
	userCh := watch("", func(p string) bool { return filepath.Base(p) == "ign" })
	// Stopping this watch leaves ign with the tree's recursive watchpoint.
	tr.Stop(watch("ign", nil))
	watch("ign/deep", nil)
	mkdirs(t, tmp, "ign/deep/new")

	overflow(t, c, userCh)
	if p := filepath.Join(tmp, "ign/deep/new"); !spy.watched(p) {
		t.Errorf("%q was not watched", p)
	}
}

// A walk that starts below a directory a recursive watch excludes, e.g. for
// a directory created there, leaves it out of that watch's tree.
func TestWalkBelowExcludedDir(t *testing.T) {
	tmp := t.TempDir()
	mkdirs(t, tmp, "ign/new/sub")
	tr, spy, _ := newSpyTree(t)
	doNotWatch := func(p string) bool { return filepath.Base(p) == "ign" }
	if err := tr.Watch(filepath.Join(tmp, "..."), make(chan EventInfo, 16), doNotWatch, All); err != nil {
		t.Fatal(err)
	}
	tr.watchTree(filepath.Join(tmp, "ign/new"))
	for _, d := range []string{"ign/new", "ign/new/sub"} {
		if p := filepath.Join(tmp, d); spy.watched(p) {
			t.Errorf("%q is below an excluded directory but was watched", p)
		}
	}
}

// Watching a directory is tried again when it fails with ENOENT although the
// directory exists, as it does on kqueue when a file in it vanishes.
func TestOverflowRetriesVanishedFile(t *testing.T) {
	tmp := t.TempDir()
	tr, spy, c := newSpyTree(t)
	userCh := make(chan EventInfo, 16)
	if err := tr.Watch(filepath.Join(tmp, "..."), userCh, nil, All); err != nil {
		t.Fatal(err)
	}
	mkdirs(t, tmp, "busy/sub")
	spy.vanishing[filepath.Join(tmp, "busy")] = 2

	if got := overflow(t, c, userCh); len(got) != 1 {
		t.Fatalf("got %v; want only the overflow notification", got)
	}
	if p := filepath.Join(tmp, "busy/sub"); !spy.watched(p) {
		t.Errorf("%q was not watched", p)
	}
}
