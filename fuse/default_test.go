package fuse

// Make sure library supplied FileSystems support the
// required interface.

import (
	"testing"
)

func TestRawFs(t *testing.T) {
	var iface RawFileSystem

	_ = iface
}
