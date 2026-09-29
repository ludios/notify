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
	c    chan EventInfo // the user channel
}

func newRecreateFixture(t *testing.T, dirs ...string) *recreateFixture {
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
	tree := newNonrecursiveTree(newWatcher(c), c, nil)
	t.Cleanup(func() { tree.Close() })
	f := &recreateFixture{t: t, root: root, tree: tree, c: make(chan EventInfo, 512)}
	if err := tree.Watch(filepath.Join(root, "..."), f.c, nil, syncthingMask); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *recreateFixture) path(rel string) string {
	return filepath.Join(f.root, rel)
}

// settle waits for the watcher and the tree's recursive bookkeeping to
// catch up, then discards the events delivered so far.
func (f *recreateFixture) settle() {
	time.Sleep(200 * time.Millisecond)
	for {
		select {
		case <-f.c:
		default:
			return
		}
	}
}

// expectEvent fails unless an event for rel arrives.
func (f *recreateFixture) expectEvent(rel string) {
	f.t.Helper()
	want := f.path(rel)
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ei := <-f.c:
			if ei.Path() == want {
				return
			}
		case <-deadline:
			f.t.Fatalf("no event for %s", rel)
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

// Git's gc deletes emptied .git/objects/xx directories; the next object
// written there recreates the directory. Everything written into the
// recreated directory must still be reported.
func TestRecreatedDirIsWatched(t *testing.T) {
	f := newRecreateFixture(t, "objects/ab")
	f.do(os.RemoveAll(f.path("objects/ab")))
	f.settle()
	f.do(os.Mkdir(f.path("objects/ab"), 0755))
	f.settle()
	f.write("objects/ab/obj")
	f.expectEvent("objects/ab/obj")
}

// A subdirectory whose path existed before must be watched as well when its
// recreated parent is walked.
func TestRecreatedTreeIsWatched(t *testing.T) {
	f := newRecreateFixture(t, "a/b/c")
	f.do(os.RemoveAll(f.path("a")))
	f.settle()
	f.do(os.MkdirAll(f.path("a/b/c"), 0755))
	f.settle()
	f.write("a/b/c/file")
	f.expectEvent("a/b/c/file")
}

// A directory renamed away keeps its watch; a directory later created at its
// old path must get one too, and events must carry the renamed path.
func TestRenamedAwayDirPathIsRewatched(t *testing.T) {
	f := newRecreateFixture(t, "old/sub")
	f.do(os.Rename(f.path("old"), f.path("new")))
	f.settle()
	f.write("new/sub/file")
	f.expectEvent("new/sub/file")
	f.do(os.Mkdir(f.path("old"), 0755))
	f.settle()
	f.write("old/file")
	f.expectEvent("old/file")
}
