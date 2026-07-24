package rpcruntime

import "context"

// CallDescriptor is the minimal per-call data RetryIdempotent needs to mint a
// retry attempt: the call ID and the application's dedup key.
// DedupKey is a plain string here (not the public styx.DedupKey) because this
// package must not import the styx package.
// The styx package converts between the two at its boundary, exactly as it does
// elsewhere for its transport-agnostic mirror types.
// The dedup key is copied onto a retry attempt but stays host-local: no
// data-plane frame carries it (transport.Frame has no per-call metadata field),
// so the plugin's handler cannot observe it today and the framework itself never
// deduplicates on it.
// Delivering the key to the plugin end-to-end awaits a wire-level carrier
// decision; until then a minted attempt's key is meaningful only within this
// process.
type CallDescriptor struct {
	CallID   uint64
	DedupKey string
}

// RetryIdempotent mints a NEW attempt of an idempotent call: it allocates a
// fresh call ID from tbl's monotonic, never-reused ID space and carries orig's
// DedupKey unchanged.
// The copied key is host-local and preparatory: no data-plane frame carries it,
// so the plugin's handler cannot observe it today and cannot yet use it to
// recognize the retry as the same logical operation.
// Delivering the key end-to-end awaits the wire carrier noted on CallDescriptor.
// Generated code calls this only for a method its idempotency declaration marks
// safe to execute more than once.
// RetryIdempotent does NOT decide whether to retry — that is the generated
// client's decision, gated on the method's idempotency declaration and
// IsRetryable(err).
// It only mints the new attempt's descriptor correctly.
// It also does not register the attempt as a live call (no Submit) or emit any
// frame: issuing the retry over the connection is the caller's separate step.
// It returns ctx.Err() without minting when ctx is already done, so a caller
// that has already given up does not consume a call ID.
func RetryIdempotent(ctx context.Context, tbl *Table, orig CallDescriptor) (CallDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return CallDescriptor{}, err
	}

	return CallDescriptor{CallID: tbl.NextID(), DedupKey: orig.DedupKey}, nil
}
