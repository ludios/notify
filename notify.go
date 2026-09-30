// Model-output: Claude Opus 5.5
// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

// BUG(rjeczalik): Notify does not collect watchpoints, when underlying watches
// were removed by their os-specific watcher implementations. Instead users are
// advised to listen on persistent paths to have guarantee they receive events
// for the whole lifetime of their applications (to discuss see #69).

// BUG(ppknap): Linux (inotify) does not support watcher behavior masks like
// InOneshot, InOnlydir etc. Instead users are advised to perform the filtering
// themselves (to discuss see #71).

// BUG(ppknap): Notify  was not tested for short path name support under Windows
// (ReadDirectoryChangesW).

// BUG(ppknap): Windows (ReadDirectoryChangesW) cannot recognize which notification
// triggers FileActionModified event. (to discuss see #75).

package notify

var defaultTree = newTree()

type DoNotWatchFn func(string) bool

// Watch sets up a watchpoint on path listening for events given by the events
// argument.
//
// File or directory given by the path must exist, otherwise Watch will fail
// with non-nil error. Notify resolves, for its internal purpose, any symlinks
// the provided path may contain, so it may fail if the symlinks form a cycle.
// It does so, since not all watcher implementations treat passed paths as-is.
// E.g. FSEvents reports a real path for every event, setting a watchpoint
// on /tmp will report events with paths rooted at /private/tmp etc.
//
// The c almost always is a buffered channel. Watch will not block sending to c
// - the caller must ensure that c has sufficient buffer space to keep up with
// the expected event rate.
//
// It is allowed to pass the same channel multiple times with different event
// list or different paths. Calling Watch with different event lists for a single
// watchpoint expands its event set. The only way to shrink it, is to call
// Stop on its channel.
//
// Calling Watch with empty event list does not expand nor shrink watchpoint's
// event set. If c is the first channel to listen for events on the given path,
// Watch will seamlessly create a watch on the filesystem.
//
// Notify dispatches copies of single filesystem event to all channels registered
// for each path. If a single filesystem event contains multiple coalesced events,
// each of them is dispatched separately. E.g. the following filesystem change:
//
//	~ $ echo Hello > Notify.txt
//
// dispatches two events - notify.Create and notify.Write. However, it may depend
// on the underlying watcher implementation whether OS reports both of them.
//
// # Windows and recursive watches
//
// If a directory which path was used to create recursive watch under Windows
// gets deleted, the OS will not report such event. It is advised to keep in
// mind this limitation while setting recursive watchpoints for your application,
// e.g. use persistent paths like %userprofile% or watch additionally parent
// directory of a recursive watchpoint in order to receive delete events for it.
//
// # Directories that cannot be watched later
//
// Where the OS watches directories one by one (e.g. inotify), a recursive
// watchpoint watches directories that appear in its tree after Watch returned.
// If one cannot be watched or read, e.g. because the inotify watch limit was
// reached, c is sent a *WatchError (unless c is full), and changes below that
// directory may go unreported.
func Watch(path string, c chan<- EventInfo, events ...Event) error {
	return defaultTree.Watch(path, c, nil, events...)
}

// This function works the same way as Watch. In addition it does not watch
// files or directories based on the return value of the argument function
// doNotWatch. Given a path as argument doNotWatch should return true if the
// file or directory should not be watched.
//
// Nothing below a directory doNotWatch excludes is watched. doNotWatch is
// kept until Stop, for directories that appear later, and then called from
// notify's goroutines, one call at a time, while it blocks notify's event
// dispatching; so it must be quick and must not call into notify.
func WatchWithFilter(path string, c chan<- EventInfo,
	doNotWatch func(string) bool, events ...Event) error {
	return defaultTree.Watch(path, c, doNotWatch, events...)
}

// WatchError is sent, in place of an event, on the channel of a recursive
// watchpoint when a directory in its tree could not be watched or read after
// Watch returned. Changes below the directory may go unreported from then on.
// Such a channel is sent one WatchError at most for each time directories
// are looked for in the tree (see Watch).
type WatchError struct {
	Dir string // the directory not watched, nor anything below it
	Err error  // why, e.g. an *os.PathError from watching Dir
}

func (e *WatchError) Event() Event  { return 0 }
func (e *WatchError) Path() string  { return e.Dir }
func (e *WatchError) Sys() any      { return nil }
func (e *WatchError) Error() string { return "notify: not watching " + e.Dir + ": " + e.Err.Error() }
func (e *WatchError) Unwrap() error { return e.Err }

// Stop removes all watchpoints registered for c. All underlying watches are
// also removed, for which c was the last channel listening for events.
//
// Stop does not close c. When Stop returns, it is guaranteed that c will
// receive no more signals.
func Stop(c chan<- EventInfo) {
	defaultTree.Stop(c)
}
