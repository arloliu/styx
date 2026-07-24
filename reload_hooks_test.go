package styx

import (
	"context"
	"testing"

	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

// fakeMutator is a comparable no-op lifecycle.Mutator, distinguished by name
// so a test can assert registration order by identity.
type fakeMutator struct{ name string }

func (fakeMutator) Freeze(context.Context) error { return nil }
func (fakeMutator) Resume(context.Context) error { return nil }

type fakeReloadStateSaver struct{}

func (fakeReloadStateSaver) SaveState(context.Context) ([]byte, error) { return nil, nil }

type fakeReloadStateRestorer struct{}

func (fakeReloadStateRestorer) RestoreState(context.Context, uint32, []byte) error { return nil }

// Test RegisterMutator appending every registered Mutator to
// PluginReloadHooks.Mutators in the order it was registered, since
// ServeReload's Freeze/Resume ordering guarantee depends on it
func TestPluginServer_RegisterMutator_PreserveRegistrationOrder(t *testing.T) {
	// Given
	s := NewPluginServer(PluginServerConfig{})
	first := fakeMutator{name: "first"}
	second := fakeMutator{name: "second"}

	// When
	s.RegisterMutator(first)
	s.RegisterMutator(second)

	// Then
	require.Equal(t, []lifecycle.Mutator{first, second}, s.reloadHooks.Mutators)
}

// Test RegisterStateSaver storing the registered StateSaver for ServeReload to use
func TestPluginServer_RegisterStateSaver_StoreSaver(t *testing.T) {
	// Given
	s := NewPluginServer(PluginServerConfig{})
	saver := fakeReloadStateSaver{}

	// When
	s.RegisterStateSaver(saver)

	// Then
	require.Equal(t, lifecycle.StateSaver(saver), s.reloadHooks.Saver)
}

// Test RegisterStateRestorer storing the registered StateRestorer for ServeRestore to use
func TestPluginServer_RegisterStateRestorer_StoreRestorer(t *testing.T) {
	// Given
	s := NewPluginServer(PluginServerConfig{})
	restorer := fakeReloadStateRestorer{}

	// When
	s.RegisterStateRestorer(restorer)

	// Then
	require.Equal(t, lifecycle.StateRestorer(restorer), s.reloadHooks.Restorer)
}

// Test PluginServer starting with no reload hooks registered, so a plugin
// that opts out of hot-reload support needs to call none of the Register*
// methods
func TestPluginServer_ReloadHooks_EmptyByDefault(t *testing.T) {
	// Given / When
	s := NewPluginServer(PluginServerConfig{})

	// Then
	require.Empty(t, s.reloadHooks.Mutators)
	require.Nil(t, s.reloadHooks.Saver)
	require.Nil(t, s.reloadHooks.Restorer)
}
