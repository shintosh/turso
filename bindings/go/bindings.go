package turso

import (
	"fmt"
	"sync"
	"time"

	turso_libs "github.com/tursodatabase/turso-go-platform-libs"
)

var initLibrary sync.Once

func withNativeCall[T any](call func() T) T {
	end := beginNativeCall()
	defer end()
	started := time.Now()
	result := call()
	recordNativeCallExecution(time.Since(started))
	return result
}

func withNativeCallVoid(call func()) {
	end := beginNativeCall()
	defer end()
	started := time.Now()
	call()
	recordNativeCallExecution(time.Since(started))
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
