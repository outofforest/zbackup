package build

import (
	"github.com/outofforest/build"
	"github.com/outofforest/buildgo"
)

// Commands is a definition of commands available in build system
var Commands = map[string]build.Command{
	"setup": {Fn: setup, Description: "Installs tools required by development environment"},
	"build": {Fn: buildApp, Description: "Builds zbackup binary"},
	"run":   {Fn: runApp, Description: "Builds and runs zbackup binary"},
}

func init() {
	buildgo.AddCommands(Commands)
}
