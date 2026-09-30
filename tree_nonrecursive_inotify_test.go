// Model-output: Claude Opus 5.5
// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

//go:build linux
// +build linux

package notify

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// syncthingMask is the event set Syncthing watches with on Linux: only
// inotify-specific events, so no platform-independent Remove or Rename.
const syncthingMask = InCreate | InMovedTo | InDelete | InDeleteSelf | InModify |
	InMovedFrom | InMoveSelf | InAttrib

// recreateFixture is a recursive watch on a temporary directory through the
// non-recursive tree and a real inotify watcher, as Syncthing uses them.
type recreateFixture struct {
	t    *testing.T
	root string
	tree *nonrecursiveTree
	w    *inotify
	c    chan EventInfo // the user channel
}

// newRecreateFixture creates dirs in the temporary directory and watches it,
// leaving out what doNotWatch (if not nil) excludes.
func newRecreateFixture(t *testing.T, doNotWatch DoNotWatchFn, dirs ...string) *recreateFixture {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(root, d), 0755); err != nil {
			t.Fatal(err)
		}
	}
	c := make(chan EventInfo, buffer)
	w := newWatcher(c).(*inotify)
	tree := newNonrecursiveTree(w, c, nil)
	t.Cleanup(func() { tree.Close() })
	f := &recreateFixture{t: t, root: root, tree: tree, w: w, c: make(chan EventInfo, 512)}
	if err := tree.Watch(filepath.Join(root, "..."), f.c, doNotWatch, syncthingMask); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *recreateFixture) path(rel string) string {
	return filepath.Join(f.root, rel)
}

// deadline bounds how long the tests wait for the watcher and the tree's
// recursive bookkeeping, which run asynchronously.
const deadline = 5 * time.Second

// writeUntilEvent writes rel until an event for it arrives, failing if none
// does before the deadline: a directory created moments ago may not be
// watched yet, but it must be eventually.
func (f *recreateFixture) writeUntilEvent(rel string) {
	f.t.Helper()
	want := f.path(rel)
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		f.write(rel)
		wait := time.After(100 * time.Millisecond)
	Drain:
		for {
			select {
			case ei := <-f.c:
				if ei.Path() == want {
					return
				}
			case <-wait:
				break Drain
			}
		}
	}
	f.t.Fatalf("no event for %s", rel)
}

// waitFor fails unless cond becomes true before the deadline.
func (f *recreateFixture) waitFor(what string, cond func() bool) {
	f.t.Helper()
	for end := time.Now().Add(deadline); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			f.t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func (f *recreateFixture) do(err error) {
	f.t.Helper()
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *recreateFixture) write(rel string) {
	f.t.Helper()
	f.do(os.WriteFile(f.path(rel), []byte("x"), 0644))
}

// watchedPaths returns how many inotify descriptors are recorded for each path.
func (f *recreateFixture) watchedPaths() map[string]int {
	f.w.RLock()
	defer f.w.RUnlock()
	paths := make(map[string]int)
	for _, wd := range f.w.m {
		paths[wd.path]++
	}
	return paths
}

// Git's gc deletes emptied .git/objects/xx directories; the next object
// written there recreates the directory. Everything written into the
// recreated directory must still be reported.
func TestRecreatedDirIsWatched(t *testing.T) {
	f := newRecreateFixture(t, nil, "objects/ab")
	f.do(os.RemoveAll(f.path("objects/ab")))
	f.do(os.Mkdir(f.path("objects/ab"), 0755))
	f.writeUntilEvent("objects/ab/obj")
}

// A subdirectory whose path existed before must be watched as well when its
// recreated parent is walked.
func TestRecreatedTreeIsWatched(t *testing.T) {
	f := newRecreateFixture(t, nil, "a/b/c")
	f.do(os.RemoveAll(f.path("a")))
	f.do(os.MkdirAll(f.path("a/b/c"), 0755))
	f.writeUntilEvent("a/b/c/file")
}

// A directory renamed within the tree keeps its watch, now under its new
// path; a directory later created at its old path must be watched too.
func TestRenamedDirPathIsRewatched(t *testing.T) {
	f := newRecreateFixture(t, nil, "old/sub")
	f.do(os.Rename(f.path("old"), f.path("new")))
	f.writeUntilEvent("new/sub/file")
	f.do(os.Mkdir(f.path("old"), 0755))
	f.writeUntilEvent("old/file")
}

// Removed directories must not leave their descriptors behind.
func TestRemovedDirForgotten(t *testing.T) {
	f := newRecreateFixture(t, nil, "gone/deeper")
	f.do(os.RemoveAll(f.path("gone")))
	f.waitFor("descriptors of removed directories to be dropped", func() bool {
		paths := f.watchedPaths()
		return paths[f.path("gone")] == 0 && paths[f.path("gone/deeper")] == 0
	})
}

// A directory renamed out of the tree keeps its watch under its old path
// (notify cannot follow it there), and a directory created in its place gets
// another. Stop must remove both.
func TestStopUnwatchesAllDescriptorsForPath(t *testing.T) {
	outside, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := newRecreateFixture(t, nil, "d")
	f.do(os.Rename(f.path("d"), filepath.Join(outside, "d")))
	f.do(os.Mkdir(f.path("d"), 0755))
	f.waitFor("the recreated d to be watched", func() bool { return f.watchedPaths()[f.path("d")] == 2 })
	f.tree.Stop(f.c)
	if n := f.watchedPaths()[f.path("d")]; n != 0 {
		t.Fatalf("got %d descriptors for d after Stop, want 0", n)
	}
}

// Directories created in the tree that the watch's DoNotWatchFn excludes
// must not be watched, nor anything in them.
func TestNewDirKeepsFilter(t *testing.T) {
	f := newRecreateFixture(t, func(p string) bool { return filepath.Base(p) == "ign" })
	f.do(os.MkdirAll(f.path("ign/deep"), 0755))
	// Directories are handled in the order they were created in.
	f.do(os.Mkdir(f.path("keep"), 0755))
	f.writeUntilEvent("keep/file")
	paths := f.watchedPaths()
	if paths[f.path("ign")] != 0 || paths[f.path("ign/deep")] != 0 {
		t.Fatalf("excluded directories are watched: %v", paths)
	}
}
