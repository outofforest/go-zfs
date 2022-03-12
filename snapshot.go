// Package zfs provides wrappers around the ZFS command line tools.
package zfs

import (
	"context"
	"io"
	"math"
)

const datasetSnapshot = "snapshot"

// Snapshots returns a slice of ZFS snapshots.
// A filter argument may be passed to select a snapshot with the matching name,
// or empty string ("") may be used to select all snapshots.
func Snapshots(ctx context.Context) ([]*Snapshot, error) {
	return snapshots(ctx, "", math.MaxUint16)
}

func snapshots(ctx context.Context, filter string, depth uint16) ([]*Snapshot, error) {
	infos, err := info(ctx, datasetSnapshot, filter, depth)
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
func GetSnapshot(ctx context.Context, name string) (*Snapshot, error) {
	info, err := info(ctx, datasetSnapshot, name, 0)
	if err != nil {
		return nil, err
	}

	return &Snapshot{Info: info[0]}, nil
}

// ReceiveSnapshot receives a ZFS stream from the input io.Reader, creates a
// new snapshot with the specified name, and streams the input data into the
// newly-created snapshot.
func ReceiveSnapshot(ctx context.Context, input io.ReadCloser, name string) (*Snapshot, error) {
	defer input.Close()
	if _, err := zfsStdin(ctx, input, "receive", name); err != nil {
		return nil, err
	}
	return GetSnapshot(ctx, name)
}

// Snapshot is a ZFS snapshot
type Snapshot struct {
	Info Info
}

// Clone clones a ZFS snapshot and returns the cloned filesystem.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Snapshot) Clone(ctx context.Context, dest string) (*Filesystem, error) {
	if _, err := zfs(ctx, "clone", d.Info.Name, dest); err != nil {
		return nil, err
	}
	return GetFilesystem(ctx, dest)
}

// Holds returns holds on snapshot
func (d *Snapshot) Holds(ctx context.Context) ([]string, error) {
	holds, err := zfs(ctx, "holds", "-H", d.Info.Name)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(holds))
	for _, h := range holds {
		out = append(out, h[1])
	}
	return out, nil
}

// Hold holds the snapshot
func (d *Snapshot) Hold(ctx context.Context, tag string) error {
	_, err := zfs(ctx, "hold", tag, d.Info.Name)
	return err
}

// Release releases the snapshot
func (d *Snapshot) Release(ctx context.Context, tag string) error {
	_, err := zfs(ctx, "release", tag, d.Info.Name)
	return err
}

// Send sends a ZFS stream of a snapshot to the input io.Writer.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Snapshot) Send(ctx context.Context, output io.WriteCloser) error {
	defer output.Close()
	return zfsStdout(ctx, output, "send", d.Info.Name)
}

// IncrementalSend sends a ZFS stream of a snapshot to the input io.Writer
// using the baseSnapshot as the starting point.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Snapshot) IncrementalSend(ctx context.Context, base *Snapshot, output io.WriteCloser) error {
	defer output.Close()
	return zfsStdout(ctx, output, "send", "-i", base.Info.Name, d.Info.Name)
}

// Destroy destroys a ZFS dataset. If the destroy bit flag is set, any
// descendents of the dataset will be recursively destroyed, including snapshots.
// If the deferred bit flag is set, the snapshot is marked for deferred
// deletion.
func (d *Snapshot) Destroy(ctx context.Context, flags DestroyFlag) error {
	return destroy(ctx, d.Info.Name, flags)
}

// SetProperty sets a ZFS property on the receiving dataset.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (d *Snapshot) SetProperty(ctx context.Context, key, val string) error {
	return setProperty(ctx, d.Info.Name, key, val)
}

// GetProperty returns the current value of a ZFS property from the
// receiving dataset.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (d *Snapshot) GetProperty(ctx context.Context, key string) (string, error) {
	return getProperty(ctx, d.Info.Name, key)
}

// Rollback rolls back the receiving ZFS filesystem to a previous snapshot.
// Intermediate snapshots can be destroyed.
func (d *Snapshot) Rollback(ctx context.Context) error {
	_, err := zfs(ctx, "rollback", "-r", d.Info.Name)
	return err
}
