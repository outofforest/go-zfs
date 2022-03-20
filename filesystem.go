// Package zfs provides wrappers around the ZFS command line tools.
package zfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
)

const datasetFilesystem = "filesystem"

// Filesystems returns a slice of ZFS filesystems.
// A filter argument may be passed to select a filesystem with the matching name,
// or empty string ("") may be used to select all filesystems.
func Filesystems(ctx context.Context) ([]*Filesystem, error) {
	infos, err := info(ctx, datasetFilesystem, "", math.MaxUint16)
	if err != nil {
		return nil, err
	}
	filesystems := []*Filesystem{}
	for _, info := range infos {
		filesystems = append(filesystems, &Filesystem{Info: info})
	}
	return filesystems, nil
}

// GetFilesystem retrieves a single ZFS filesystem by name
func GetFilesystem(ctx context.Context, name string) (*Filesystem, error) {
	info, err := info(ctx, datasetFilesystem, name, 0)
	if err != nil {
		return nil, err
	}

	return &Filesystem{Info: info[0]}, nil
}

// CreateFilesystem creates a new ZFS filesystem with the specified name and
// properties.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func CreateFilesystem(ctx context.Context, name string, properties map[string]string) (*Filesystem, error) {
	args := make([]string, 1, 4)
	args[0] = "create"
	password, exists := properties["password"]
	delete(properties, "password")
	if len(properties) > 0 {
		args = append(args, propsSlice(properties)...)
	}
	var stdin io.Reader
	if exists {
		args = append(args, "-o", "encryption=on", "-o", "keylocation=prompt", "-o", "keyformat=passphrase")
		stdin = bytes.NewReader([]byte(password + "\n" + password))
	}
	args = append(args, name)
	if _, err := zfsStdin(ctx, stdin, args...); err != nil {
		return nil, err
	}
	return GetFilesystem(ctx, name)
}

// Filesystem is a ZFS filesystem
type Filesystem struct {
	Info Info
}

// Destroy destroys a ZFS dataset. If the destroy bit flag is set, any
// descendents of the dataset will be recursively destroyed, including snapshots.
// If the deferred bit flag is set, the snapshot is marked for deferred
// deletion.
func (d *Filesystem) Destroy(ctx context.Context, flags DestroyFlag) error {
	return destroy(ctx, d.Info.Name, flags)
}

// SetProperty sets a ZFS property on the receiving dataset.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (d *Filesystem) SetProperty(ctx context.Context, key, val string) error {
	return setProperty(ctx, d.Info.Name, key, val)
}

// GetProperty returns the current value of a ZFS property from the
// receiving dataset.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (d *Filesystem) GetProperty(ctx context.Context, key string) (string, bool, error) {
	return getProperty(ctx, d.Info.Name, key)
}

// Snapshots returns a slice of all ZFS snapshots of a given dataset.
func (d *Filesystem) Snapshots(ctx context.Context) ([]*Snapshot, error) {
	return snapshots(ctx, d.Info.Name, 1)
}

// Snapshot creates a new ZFS snapshot of the receiving dataset, using the
// specified name.  Optionally, the snapshot can be taken recursively, creating
// snapshots of all descendent filesystems in a single, atomic operation.
func (d *Filesystem) Snapshot(ctx context.Context, name string) (*Snapshot, error) {
	snapName := fmt.Sprintf("%s@%s", d.Info.Name, name)
	_, err := zfs(ctx, "snapshot", snapName)
	if err != nil {
		return nil, err
	}
	return GetSnapshot(ctx, snapName)
}

// Children returns a slice of children of the receiving ZFS dataset.
func (d *Filesystem) Children(ctx context.Context) ([]*Filesystem, error) {
	infos, err := info(ctx, datasetFilesystem, d.Info.Name, 1)
	if err != nil {
		return nil, err
	}

	filesystems := []*Filesystem{}
	for _, info := range infos[1:] {
		filesystems = append(filesystems, &Filesystem{Info: info})
	}
	return filesystems, nil
}

// Mount mounts ZFS filesystem
func (d *Filesystem) Mount(ctx context.Context) error {
	_, err := zfs(ctx, "mount", d.Info.Name)
	return err
}

// Unmount unmounts ZFS filesystem
func (d *Filesystem) Unmount(ctx context.Context) error {
	_, err := zfs(ctx, "umount", d.Info.Name)
	return err
}

// LoadKey loads encryption key for dataset
func (d *Filesystem) LoadKey(ctx context.Context, password string) error {
	_, err := zfsStdin(ctx, bytes.NewReader([]byte(password)), "load-key", d.Info.Name)
	return err
}

// UnloadKey unloads encryption key for dataset
func (d *Filesystem) UnloadKey(ctx context.Context) error {
	_, err := zfs(ctx, "unload-key", d.Info.Name)
	return err
}
