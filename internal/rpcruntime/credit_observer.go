package rpcruntime

import "sync/atomic"

// creditWaitObserver is a process-local, test-only observation hook fired the
// moment a sender finds the credit window empty and is about to park on it
// (stream-protocol.md §4.5's admission gate). It is nil in every production
// build, where the cost is one atomic load on a path that is already about to
// block.
//
// It exists because a blocked sender is otherwise unobservable, and "the send
// has not returned yet" is not the same claim as "the send is waiting for
// credit". A cross-process test of the one-credit window has to distinguish
// them: without this hook, deleting the credit gate entirely leaves a test that
// merely samples an unstarted goroutine and still passes.
var creditWaitObserver atomic.Pointer[func()]

// SetCreditWaitObserverForTest installs fn, invoked each time a sender parks
// waiting for credit. Passing nil clears it. Safe to call while senders are
// running: the store is atomic. Process-local, so a test that installs one must
// restore the previous value.
func SetCreditWaitObserverForTest(fn func()) {
	if fn == nil {
		creditWaitObserver.Store(nil)

		return
	}
	creditWaitObserver.Store(&fn)
}

// notifyCreditWait invokes the installed observer, if any. The nested nil check
// tolerates a hook cleared by storing a nil func.
func notifyCreditWait() {
	if fn := creditWaitObserver.Load(); fn != nil && *fn != nil {
		(*fn)()
	}
}
