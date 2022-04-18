package build

import (
	"context"

	"github.com/outofforest/build"
	"github.com/outofforest/buildgo"
)

func setup(ctx context.Context, deps build.DepsFunc) {
	deps(buildgo.InstallAll)
}
