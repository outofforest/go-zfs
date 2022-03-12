package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/outofforest/build"
	me "github.com/outofforest/go-zfs/v3/build"
	"github.com/outofforest/ioc/v2"
	"github.com/outofforest/run"
	"github.com/ridge/must"
)

func main() {
	run.Tool("build", nil, func(ctx context.Context, c *ioc.Container) error {
		exec := build.NewIoCExecutor(me.Commands, c)
		if build.Autocomplete(exec) {
			return nil
		}

		changeWorkingDir()
		return build.Do(ctx, "Digest", exec)
	})
}

func changeWorkingDir() {
	must.OK(os.Chdir(filepath.Dir(filepath.Dir(must.String(filepath.EvalSymlinks(must.String(os.Executable())))))))
}