// Package zfs provides wrappers around the ZFS command line tools.
package zfs

import (
	"bytes"
	"fmt"
)

const datasetFilesystem = "filesystem"

// Filesystems returns a slice of ZFS filesystems.
// A filter argument may be passed to select a filesystem with the matching name,
// or empty string ("") may be used to select all filesystems.
func Filesystems() ([]*Filesystem, error) {
	infos, err := info(datasetFilesystem, "", false)
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
func GetFilesystem(name string) (*Filesystem, error) {
	info, err := info(datasetFilesystem, name, false)
	if err != nil {
		return nil, err
	}

	return &Filesystem{Info: info[0]}, nil
}

// CreateFilesystem creates a new ZFS filesystem with the specified name and
// properties.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func CreateFilesystem(name string, properties map[string]string) (*Filesystem, error) {
	args := make([]string, 1, 4)
	args[0] = "create"

	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}

	args = append(args, name)
	_, err := zfs(args...)
	if err != nil {
		return nil, err
	}
	return GetFilesystem(name)
}

// Filesystem is a ZFS filesystem
type Filesystem struct {
	Info Info
}

// Destroy destroys a ZFS dataset. If the destroy bit flag is set, any
// descendents of the dataset will be recursively destroyed, including snapshots.
// If the deferred bit flag is set, the snapshot is marked for deferred
// deletion.
func (d *Filesystem) Destroy(flags DestroyFlag) error {
	return destroy(d.Info.Name, flags)
}

// SetProperty sets a ZFS property on the receiving dataset.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (d *Filesystem) SetProperty(key, val string) error {
	return setProperty(d.Info.Name, key, val)
}

// GetProperty returns the current value of a ZFS property from the
// receiving dataset.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (d *Filesystem) GetProperty(key string) (string, error) {
	return getProperty(d.Info.Name, key)
}

// Snapshots returns a slice of all ZFS snapshots of a given dataset.
func (d *Filesystem) Snapshots() ([]*Snapshot, error) {
	return snapshots(d.Info.Name)
}

// Snapshot creates a new ZFS snapshot of the receiving dataset, using the
// specified name.  Optionally, the snapshot can be taken recursively, creating
// snapshots of all descendent filesystems in a single, atomic operation.
func (d *Filesystem) Snapshot(name string, recursive bool) (*Snapshot, error) {
	args := make([]string, 1, 4)
	args[0] = "snapshot"
	if recursive {
		args = append(args, "-r")
	}
	snapName := fmt.Sprintf("%s@%s", d.Info.Name, name)
	args = append(args, snapName)
	_, err := zfs(args...)
	if err != nil {
		return nil, err
	}
	return GetSnapshot(snapName)
}

// Children returns a slice of children of the receiving ZFS dataset.
func (d *Filesystem) Children() ([]*Filesystem, error) {
	infos, err := info(datasetFilesystem, d.Info.Name, true)
	if err != nil {
		return nil, err
	}

	filesystems := []*Filesystem{}
	for _, info := range infos[1:] {
		filesystems = append(filesystems, &Filesystem{Info: info})
	}
	return filesystems, nil
}

// LoadKey loads encryption key for dataset
func (d *Filesystem) LoadKey(password string) error {
	c := command{Command: "zfs", Stdin: bytes.NewReader([]byte(password))}
	_, err := c.Run("load-key", d.Info.Name)
	return err
}

// UnloadKey unloads encryption key for dataset
func (d *Filesystem) UnloadKey() error {
	_, err := zfs("unload-key", d.Info.Name)
	return err
}
