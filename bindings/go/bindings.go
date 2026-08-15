package turso

import (
	"fmt"
	"sync"

	turso_libs "github.com/tursodatabase/turso-go-platform-libs"
)

var initLibrary sync.Once

// nativeCallMu serializes every entry into the embedded Turso library. The
// library shares process-global native state, so separate Go database handles
// cannot safely call it at the same time.
var nativeCallMu sync.Mutex

func withNativeCall[T any](call func() T) T {
	nativeCallMu.Lock()
	defer nativeCallMu.Unlock()
	return call()
}

func withNativeCallVoid(call func()) {
	nativeCallMu.Lock()
	defer nativeCallMu.Unlock()
	call()
}

func InitLibrary(strategy turso_libs.LoadTursoLibraryConfig) {
	initLibrary.Do(func() {
		library, err := turso_libs.LoadTursoLibrary(strategy)
		if err != nil {
			panic(fmt.Errorf("unable to load turso library: %w", err))
		}
		registerTursoDb(library)
		registerTursoSync(library)
	})
}
