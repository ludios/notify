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

// failingSpy is a Spy whose Watch fails with ENOSPC for the paths in fail.
type failingSpy struct {
	*Spy
	fail map[string]bool
}

func (s failingSpy) Watch(p string, e Event) error {
	if s.fail[p] {
		return syscall.ENOSPC
	}
	return s.Spy.Watch(p, e)
}

// newSpyTree returns a non-recursive tree over a Spy watcher, which fails to
// watch the paths in fail, and the channel the watcher would send to. The Spy
// records Watch calls and never delivers filesystem events, which
// deterministically simulates the events for directories created after
// watching being lost in a queue overflow.
func newSpyTree(t *testing.T, fail ...string) (*nonrecursiveTree, *Spy, chan EventInfo) {
	spy := failingSpy{Spy: &Spy{}, fail: make(map[string]bool)}
	for _, p := range fail {
		spy.fail[p] = true
	}
	c := make(chan EventInfo, buffer)
	tr := newNonrecursiveTree(spy, c, nil)
	t.Cleanup(func() { tr.Close() })
	return tr, spy.Spy, c
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

// spyWatched reports whether spy was asked to watch path.
func spyWatched(spy *Spy, path string) bool {
	for _, call := range *spy {
		if call.F == FuncWatch && call.P == path {
			return true
		}
	}
	return false
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
	if spyWatched(spy, lost) {
		t.Fatalf("%q watched before overflow handling; test is broken", lost)
	}

	got := overflow(t, c, userCh)
	if len(got) != 1 || got[0].Path() != tmp {
		t.Fatalf("got %v; want only an overflow notification for %q", got, tmp)
	}
	// The overflow notification is sent after the re-walk completed, so
	// the repair watch must have been recorded by now.
	if !spyWatched(spy, lost) {
		t.Fatalf("%q was not watched during overflow recovery; calls: %v", lost, *spy)
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
	mkdirs(t, tmp, "ign/deep", "keep", "other/ign")
	if err := tr.Watch(other+"/...", make(chan EventInfo, 16), nil, All); err != nil {
		t.Fatal(err)
	}

	overflow(t, c, userCh)
	for _, d := range []string{"ign", "ign/deep"} {
		if p := filepath.Join(tmp, d); spyWatched(spy, p) {
			t.Errorf("%q is excluded but was watched", p)
		}
	}
	for _, d := range []string{"keep", "other/ign"} {
		if p := filepath.Join(tmp, d); !spyWatched(spy, p) {
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
	tr, spy, c := newSpyTree(t, full)
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
	case werr.Path() == full && errors.Is(werr.Err, syscall.ENOSPC):
	case werr.Path() == locked && errors.Is(werr.Err, os.ErrPermission):
	default:
		t.Fatalf("got WatchError %v", werr.Err)
	}
	if spyWatched(spy, filepath.Join(full, "sub")) {
		t.Error("the directory that failed to be watched was walked")
	}
	if !spyWatched(spy, filepath.Join(tmp, "keep")) {
		t.Error("the walk stopped at a failing directory")
	}
}
