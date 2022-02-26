package build

import "github.com/outofforest/buildgo"

// Commands is a definition of commands available in build system
var Commands = map[string]interface{}{
	"tools/build": buildMe,
}

func init() {
	buildgo.AddCommands(Commands)
}
