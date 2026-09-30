// Model-output: Claude Opus 5.5
// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

package notify

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// AddDir must keep walking when directories disappear between reading their
// parent and watching them, as git's gc does to .git/objects/xx.
func TestAddDirSkipsVanishedEntries(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"keep/a", "keep/b", "gone/sub", "later"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0755); err != nil {
			t.Fatal(err)
		}
	}

	// doNotWatch runs after the parent is read, so removing the directory it
	// is asked about makes fn fail on it, as watching it would.
	gone := filepath.Join(root, "gone")
	doNotWatch := func(p string) bool {
		if p == gone {
			if err := os.RemoveAll(p); err != nil {
				t.Fatal(err)
			}
		}
		return false
	}
	var visited []string
	fn := func(nd node) error {
		if _, err := os.Lstat(nd.Name); err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, nd.Name)
		visited = append(visited, filepath.ToSlash(rel))
		return nil
	}
	if err := newnode(root).AddDir(fn, doNotWatch); err != nil {
		t.Fatalf("AddDir()=%v", err)
	}
	sort.Strings(visited)
	want := []string{".", "keep", "keep/a", "keep/b", "later"}
	if len(visited) != len(want) {
		t.Fatalf("visited %v, want %v", visited, want)
	}
	for i := range want {
		if visited[i] != want[i] {
			t.Fatalf("visited %v, want %v", visited, want)
		}
	}
}

// A subdirectory removed after its parent was read is skipped when fn fails
// on it, but not a subdirectory that still exists (on kqueue, fn fails when
// one of the directory's files vanishes), and the directory AddDir was called
// for must exist.
func TestAddDirVanishedSubdirAndRoot(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	notFound := &os.PathError{Op: "watch", Path: sub, Err: os.ErrNotExist}
	mkdir := func() {
		if err := os.MkdirAll(sub, 0755); err != nil {
			t.Fatal(err)
		}
	}

	mkdir()
	removeSub := func(nd node) error {
		if nd.Name == sub {
			if err := os.Remove(sub); err != nil {
				t.Fatal(err)
			}
			return notFound
		}
		return nil
	}
	if err := newnode(root).AddDir(removeSub, nil); err != nil {
		t.Fatalf("AddDir()=%v for vanished subdirectory", err)
	}

	mkdir()
	failSub := func(nd node) error {
		if nd.Name == sub {
			return notFound
		}
		return nil
	}
	if err := newnode(root).AddDir(failSub, nil); err == nil {
		t.Fatal("AddDir()=nil although fn failed on an existing subdirectory")
	}

	if err := newnode(filepath.Join(root, "missing")).AddDir(func(node) error { return nil }, nil); err == nil {
		t.Fatal("AddDir()=nil for a missing directory")
	}
}
