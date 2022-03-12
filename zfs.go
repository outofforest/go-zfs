// Package zfs provides wrappers around the ZFS command line tools.
package zfs

// InodeType is the type of inode as reported by Diff
type InodeType int

// Types of Inodes
const (
	_                     = iota // 0 == unknown type
	BlockDevice InodeType = iota
	CharacterDevice
	Directory
	Door
	NamedPipe
	SymbolicLink
	EventPort
	Socket
	File
)

// ChangeType is the type of inode change as reported by Diff
type ChangeType int

// Types of Changes
const (
	_                  = iota // 0 == unknown type
	Removed ChangeType = iota
	Created
	Modified
	Renamed
)

// DestroyFlag is the options flag passed to Destroy
type DestroyFlag int

// Valid destroy options
const (
	DestroyDefault         DestroyFlag = 1 << iota
	DestroyRecursive                   = 1 << iota
	DestroyRecursiveClones             = 1 << iota
	DestroyDeferDeletion               = 1 << iota
	DestroyForceUmount                 = 1 << iota
)

// Logger can be used to log commands/actions
type Logger interface {
	Log(cmd []string)
}

type defaultLogger struct{}

func (*defaultLogger) Log(cmd []string) {

}

var logger Logger = &defaultLogger{}

// SetLogger set a log handler to log all commands including arguments before
// they are executed
func SetLogger(l Logger) {
	if l != nil {
		logger = l
	}
}

// zfs is a helper function to wrap typical calls to zfs.
func zfs(arg ...string) ([][]string, error) {
	c := command{Command: "zfs"}
	return c.Run(arg...)
}
