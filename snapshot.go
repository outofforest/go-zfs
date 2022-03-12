// Package zfs provides wrappers around the ZFS command line tools.
package zfs

import (
	"io"
	"math"
)

const datasetSnapshot = "snapshot"

// Snapshots returns a slice of ZFS snapshots.
// A filter argument may be passed to select a snapshot with the matching name,
// or empty string ("") may be used to select all snapshots.
func Snapshots() ([]*Snapshot, error) {
	return snapshots("", math.MaxUint16)
}

func snapshots(filter string, depth uint16) ([]*Snapshot, error) {
	infos, err := info(datasetSnapshot, filter, depth)
	if err != nil {
		return nil, err
	}
	snapshots := []*Snapshot{}
	for _, info := range infos {
		snapshots = append(snapshots, &Snapshot{Info: info})
	}
	return snapshots, nil
}

// GetSnapshot retrieves a single ZFS snapshot by name
func GetSnapshot(name string) (*Snapshot, error) {
	info, err := info(datasetSnapshot, name, 0)
	if err != nil {
		return nil, err
	}

	return &Snapshot{Info: info[0]}, nil
}

// ReceiveSnapshot receives a ZFS stream from the input io.Reader, creates a
// new snapshot with the specified name, and streams the input data into the
// newly-created snapshot.
func ReceiveSnapshot(input io.ReadCloser, name string) (*Snapshot, error) {
	defer input.Close()

	c := command{Command: "zfs", Stdin: input}
	if _, err := c.Run("receive", name); err != nil {
		return nil, err
	}
	return GetSnapshot(name)
}

// Snapshot is a ZFS snapshot
type Snapshot struct {
	Info Info
}

// Clone clones a ZFS snapshot and returns the cloned filesystem.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Snapshot) Clone(dest string) (*Filesystem, error) {
	if _, err := zfs("clone", d.Info.Name, dest); err != nil {
		return nil, err
	}
	return GetFilesystem(dest)
}

// Send sends a ZFS stream of a snapshot to the input io.Writer.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Snapshot) Send(output io.WriteCloser) error {
	defer output.Close()
	c := command{Command: "zfs", Stdout: output}
	_, err := c.Run("send", d.Info.Name)
	return err
}

// IncrementalSend sends a ZFS stream of a snapshot to the input io.Writer
// using the baseSnapshot as the starting point.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Snapshot) IncrementalSend(base *Snapshot, output io.WriteCloser) error {
	defer output.Close()
	c := command{Command: "zfs", Stdout: output}
	_, err := c.Run("send", "-i", base.Info.Name, d.Info.Name)
	return err
}

// Destroy destroys a ZFS dataset. If the destroy bit flag is set, any
// descendents of the dataset will be recursively destroyed, including snapshots.
// If the deferred bit flag is set, the snapshot is marked for deferred
// deletion.
func (d *Snapshot) Destroy(flags DestroyFlag) error {
	return destroy(d.Info.Name, flags)
}

// SetProperty sets a ZFS property on the receiving dataset.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (d *Snapshot) SetProperty(key, val string) error {
	return setProperty(d.Info.Name, key, val)
}

// GetProperty returns the current value of a ZFS property from the
// receiving dataset.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (d *Snapshot) GetProperty(key string) (string, error) {
	return getProperty(d.Info.Name, key)
}

// Rollback rolls back the receiving ZFS filesystem to a previous snapshot.
// Intermediate snapshots can be destroyed.
func (d *Snapshot) Rollback() error {
	_, err := zfs("rollback", "-r", d.Info.Name)
	return err
}
