// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

package notify

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestOverflow verifies recovery from a watcher event queue overflow
// (e.g. inotify IN_Q_OVERFLOW): directories created while events were
// being lost must get watched retroactively, and every user channel must
// receive an overflowEvent for the subtree it is registered at, so that
// callers can rescan.
//
// A Spy watcher is used instead of a real one: it records Watch calls and
// never delivers filesystem events, which deterministically simulates the
// "mkdir event was lost" scenario for the directory created in step 3.
func TestOverflow(t *testing.T) {
	tmp, err := os.MkdirTemp("", "overflow")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	if err := os.Mkdir(filepath.Join(tmp, "a"), 0777); err != nil {
		t.Fatal(err)
	}

	spy := &Spy{}
	c := make(chan EventInfo, buffer)
	tr := newNonrecursiveTree(spy, c, nil)
	defer tr.Close()

	userCh := make(chan EventInfo, 16)
	if err := tr.Watch(tmp+"/...", userCh, nil, All); err != nil {
		t.Fatalf("Watch(%q)=%v", tmp, err)
	}

	// Created after the initial walk; the Spy delivers no event for it,
	// simulating a mkdir notification lost in a queue overflow.
	lost := filepath.Join(tmp, "b")
	if err := os.Mkdir(lost, 0777); err != nil {
		t.Fatal(err)
	}
	for _, call := range *spy {
		if call.F == FuncWatch && call.P == lost {
			t.Fatalf("%q watched before overflow handling; test is broken", lost)
		}
	}

	c <- overflowEvent{}

	select {
	case ei := <-userCh:
		if _, ok := ei.(overflowEvent); !ok {
			t.Fatalf("want overflowEvent; got %v on %q", ei.Event(), ei.Path())
		}
		if ei.Path() == "" {
			t.Fatal("overflow notification carries no path")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for overflow notification")
	}

	// The overflow notification is sent after the re-walk completed, so
	// the repair watch must have been recorded by now.
	watched := false
	for _, call := range *spy {
		if call.F == FuncWatch && call.P == lost {
			watched = true
			break
		}
	}
	if !watched {
		t.Fatalf("%q was not watched during overflow recovery; calls: %v", lost, *spy)
	}
}
