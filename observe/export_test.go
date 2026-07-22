package observe

// SetAfterRLockHook installs a test-only seam that runs inside Submit just after
// the read lock is acquired and before the stopped check. It lets a test park a
// submit while it holds the read lock, so the producer cutoff's write lock
// provably blocks behind that submit — the structural barrier the shutdown race
// test needs instead of a timing sleep.
func (d *Dispatcher[T]) SetAfterRLockHook(fn func()) { d.afterRLock = fn }
