package panics_test

import (
	"errors"
	"testing"

	"github.com/arloliu/styx/internal/panics"
	"github.com/stretchr/testify/require"
)

// renderPanics panics when rendered, with a plain string — the single-level case
// fmt already survives, caught and printed as a placeholder.
type renderPanics struct{}

func (renderPanics) String() string { panic("render boom") }

// renderPanicsNested panics when rendered, WITH ITSELF, so rendering the value the
// first panic produced panics too. That second level is what fmt cannot survive: it
// re-raises, and an unguarded render inside a deferred recover has nothing left to
// catch it.
type renderPanicsNested struct{}

func (renderPanicsNested) String() string { panic(renderPanicsNested{}) }

// panicsNestedError is the error-shaped twin: fmt reaches Error rather than String,
// which is the method a consume callback's returned error is rendered through.
type panicsNestedError struct{}

func (panicsNestedError) Error() string { panic(panicsNestedError{}) }

// Test Text rendering ordinary values exactly as fmt does, so the guard costs
// nothing on the path every real panic takes.
func TestText_RendersOrdinaryValuesUnchanged(t *testing.T) {
	require.Equal(t, "boom", panics.Text("boom"))
	require.Equal(t, "7", panics.Text(7))
	require.Equal(t, "some failure", panics.Text(errors.New("some failure")))
	require.Equal(t, "<nil>", panics.Text(nil))
}

// Test Text surviving a value whose rendering panics, at both depths.
//
// The nested case is the one that matters: an unguarded render re-raises out of the
// deferred recover it runs inside, and the runtime answers a panic it cannot print
// with a throw no recover intercepts — so the failure mode being pinned here is a
// dead process, not a wrong string.
func TestText_SurvivesAValueWhoseRenderingPanics(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  any
		want string
	}{
		{name: "String panics once", val: renderPanics{}, want: "%!v(PANIC=String method: render boom)"},
		{name: "String panics while rendering its own panic", val: renderPanicsNested{},
			want: "<unrenderable panics_test.renderPanicsNested>"},
		{name: "Error panics while rendering its own panic", val: panicsNestedError{},
			want: "<unrenderable panics_test.panicsNestedError>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			require.NotPanics(t, func() { got = panics.Text(tc.val) })
			require.Equal(t, tc.want, got)
		})
	}
}

// Test the fallback naming the value's type without invoking anything on it, so a
// reader can still tell what panicked when the value itself cannot be shown.
func TestText_FallbackNamesTheTypeItCouldNotRender(t *testing.T) {
	require.Contains(t, panics.Text(renderPanicsNested{}), "renderPanicsNested")
}
