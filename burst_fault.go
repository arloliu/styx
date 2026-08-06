package styx

import (
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
)

// burstFaultReporter is the optional capability a transport implements when it
// carries the connection over more than one underside and can therefore fail as a
// whole while one of them is still healthy. It is probed for exactly like the
// other optional transport capabilities the reader loops use
// (transport.ViewReceiver, transport.ReservingReceiver): a transport that does
// not implement it reports no failure and every owner behaves as it did before
// the burst path existed.
//
// It is declared here rather than in internal/transport because it is not a
// property a transport implementation is free to offer — it is the composite's
// own connection-level state, and a second implementer would mean a second
// answer to "did the data plane die" for the same connection.
//
// It is deliberately ONE method. The answer has two parts that must agree — the
// class an owner escalates under and the cause it reports — and they are decided
// by two racing facts: the terminal transition, which a peer close or an I/O
// fault an underside's watcher observed can take, and a poison filed a moment
// later, which outranks that class because it is the connection's first fatal
// error and what every operation on it is already reporting. Read as two
// separate answers, a poison landing between them leaves an owner holding half
// of one outcome and half of the other: a plugin exiting quietly on a peer close
// while its callers are being told the data plane is desynced.
//
// The answer is also deliberately silent about a poison that landed after this
// side's own close took the transition: that close is the connection's outcome,
// and a teardown reported as a desync would manufacture a restart out of every
// shutdown and hot reload that raced a fault. The transport applies both rules,
// so both owners below get one answer from one place.
type burstFaultReporter interface {
	// DataPlaneOutcome reports how the connection's data plane ended and with what
	// cause, as one read: (BurstFailureNone, nil) while it has not failed, and for
	// a connection this side closed.
	DataPlaneOutcome() (rpcruntime.BurstFailureClass, error)
}

// The composite is the one implementation, pinned at compile time: the owners
// below reach it only through the probe, so a rename or a signature change on
// either side would otherwise degrade silently into "this transport cannot fail",
// which is exactly the gap the probe exists to close.
var _ burstFaultReporter = (*rpcruntime.BurstTransport)(nil)

// dataPlaneFailure reports how tr's data plane failed as a whole, and with what
// cause, for the owners that must escalate such a failure to teardown. It answers
// BurstFailureNone for a transport that cannot report one, for one that has not
// failed, and — deliberately — for one this side is closing: an owner's own
// teardown is not a fault, and treating it as one would manufacture a restart out
// of every ordinary shutdown and hot reload.
//
// The whole answer comes from one read, for the reason the reporter's own
// documentation gives: a poison that lands mid-probe must not be able to leave
// the owner escalating under a class its callers are not being told about.
func dataPlaneFailure(tr transport.Transport) (rpcruntime.BurstFailureClass, error) {
	reporter, ok := tr.(burstFaultReporter)
	if !ok {
		return rpcruntime.BurstFailureNone, nil
	}

	return reporter.DataPlaneOutcome()
}
