// Package panics holds the one operation every recover boundary in this
// repository needs and none of them can safely write inline: turning a recovered
// panic value into text.
//
// It is its own package rather than a helper inside one of its callers because it
// belongs to no layer. The transport recovers panics from consume and payload-fill
// callbacks; the plugin server recovers them from request decodes and from
// application handlers. Those packages share nothing else, and a copy in each is
// how the guarantee below drifts out of one of them — which is the way this defect
// was introduced the first time.
package panics

import "fmt"

// Text renders a recovered panic value as text, without trusting the value.
//
// Rendering a panic value is not the safe operation it looks like. The value came
// from code this process does not control — a handler, a codec, a callback — and
// fmt renders it by calling its own String or Error method. fmt guards that call
// once: a panic inside String is caught and printed as a placeholder. What it does
// NOT survive is the nested case. When rendering the value that the first panic
// produced panics in turn, fmt re-raises, and that second panic is raised from
// inside whatever deferred recover was rendering — where there is nothing left to
// catch it. The Go runtime then calls String one more time while preparing to print
// the panic it cannot render, and answers with
//
//	fatal error: panic while printing panic value
//
// which is a runtime throw, not a panic: no recover intercepts it and the process
// dies. A recover boundary that renders unguarded therefore converts the exact
// failure it exists to contain into an unrecoverable crash.
//
// So the render runs under its own recover, and the fallback names only the value's
// dynamic type. Verb %T is read off the type descriptor by reflection and invokes no
// method on the value, so the fallback cannot fail the way the render it replaces
// did.
//
// The returned string is always owned: fmt builds it, so it never shares memory with
// whatever the panicking code was holding — which matters where a panic value can
// carry a slice of a buffer the caller is about to reclaim.
func Text(r any) (text string) {
	defer func() {
		if rr := recover(); rr != nil {
			text = fmt.Sprintf("<unrenderable %T>", r)
		}
	}()

	return fmt.Sprint(r)
}
