package zfs

import (
	"context"
	"io"
	"io/ioutil"
	"os/exec"
	"sort"
	"testing"

	"github.com/outofforest/parallel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	Name string
	Fn   func(t *testing.T, ctx context.Context)
}

var zfsTests = []testCase{
	{
		Name: "TestCreateAndDestroyFilesystem",
		Fn: func(t *testing.T, ctx context.Context) {
			const name = "gozfs/fs"

			fs, err := CreateFilesystem(ctx, name, CreateFilesystemOptions{})
			require.NoError(t, err)
			assert.Equal(t, name, fs.Info.Name)
			assert.Equal(t, "/"+name, fs.Info.Mountpoint)

			require.NoError(t, fs.Destroy(ctx, DestroyDefault))
			_, err = GetFilesystem(ctx, name)
			assert.Error(t, err)
		},
	},
	{
		Name: "TestFilesystemProperties",
		Fn: func(t *testing.T, ctx context.Context) {
			fs, err := CreateFilesystem(ctx, "gozfs/fs", CreateFilesystemOptions{
				Properties: map[string]string{"test:prop1": "value1", "test:prop2": "value2"},
			})
			require.NoError(t, err)

			v1, exists1, err := fs.GetProperty(ctx, "test:prop1")
			require.NoError(t, err)
			v2, exists2, err := fs.GetProperty(ctx, "test:prop2")
			require.NoError(t, err)
			_, exists3, err := fs.GetProperty(ctx, "test:prop3")
			require.NoError(t, err)

			assert.True(t, exists1)
			assert.True(t, exists2)
			assert.False(t, exists3)

			assert.Equal(t, "value1", v1)
			assert.Equal(t, "value2", v2)

			require.NoError(t, fs.SetProperty(ctx, "test:prop2", "value22"))
			require.NoError(t, fs.SetProperty(ctx, "test:prop3", "value3"))

			v2, exists2, err = fs.GetProperty(ctx, "test:prop2")
			require.NoError(t, err)
			v3, exists3, err := fs.GetProperty(ctx, "test:prop3")
			require.NoError(t, err)

			assert.True(t, exists2)
			assert.True(t, exists3)

			assert.Equal(t, "value22", v2)
			assert.Equal(t, "value3", v3)
		},
	},
	{
		Name: "TestCreateAndDestroySnapshot",
		Fn: func(t *testing.T, ctx context.Context) {
			const fsName = "gozfs/fs"
			const sName = "test"

			fs, err := CreateFilesystem(ctx, fsName, CreateFilesystemOptions{})
			require.NoError(t, err)

			s, err := fs.Snapshot(ctx, sName)
			require.NoError(t, err)
			assert.Equal(t, fsName+"@"+sName, s.Info.Name)

			require.NoError(t, s.Destroy(ctx, DestroyDefault))
			_, err = GetSnapshot(ctx, fsName+"@"+sName)
			assert.Error(t, err)
		},
	},
	{
		Name: "TestSnapshotProperties",
		Fn: func(t *testing.T, ctx context.Context) {
			fs, err := CreateFilesystem(ctx, "gozfs/fs", CreateFilesystemOptions{})
			require.NoError(t, err)

			s, err := fs.Snapshot(ctx, "test")
			require.NoError(t, err)

			_, exists, err := s.GetProperty(ctx, "test:prop")
			require.NoError(t, err)

			assert.False(t, exists)

			require.NoError(t, s.SetProperty(ctx, "test:prop", "value"))

			v, exists, err := s.GetProperty(ctx, "test:prop")
			require.NoError(t, err)

			assert.True(t, exists)
			assert.Equal(t, "value", v)
		},
	},
	{
		Name: "TestClone",
		Fn: func(t *testing.T, ctx context.Context) {
			fs, err := CreateFilesystem(ctx, "gozfs/fs", CreateFilesystemOptions{})
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile("/gozfs/fs/content", []byte("test"), 0o600))

			s, err := fs.Snapshot(ctx, "image")
			require.NoError(t, err)

			fsClone, err := s.Clone(ctx, "gozfs/fsclone", CloneOptions{
				Properties: map[string]string{"test:prop": "value"},
			})
			require.NoError(t, err)

			assert.Equal(t, "gozfs/fsclone", fsClone.Info.Name)

			value, exists, err := fsClone.GetProperty(ctx, "test:prop")
			require.NoError(t, err)
			assert.True(t, exists)
			assert.Equal(t, "value", value)

			content, err := ioutil.ReadFile("/gozfs/fsclone/content")
			require.NoError(t, err)

			assert.Equal(t, "test", string(content))
		},
	},
	{
		Name: "TestRollback",
		Fn: func(t *testing.T, ctx context.Context) {
			const file = "/gozfs/fs/content"

			fs, err := CreateFilesystem(ctx, "gozfs/fs", CreateFilesystemOptions{})
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(file, []byte("test"), 0o600))

			s, err := fs.Snapshot(ctx, "image")
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(file, []byte("test2"), 0o600))

			_, err = fs.Snapshot(ctx, "image2")
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(file, []byte("test3"), 0o600))

			require.NoError(t, s.Rollback(ctx))
			content, err := ioutil.ReadFile(file)
			require.NoError(t, err)

			assert.Equal(t, "test", string(content))
		},
	},
	{
		Name: "TestListing",
		Fn: func(t *testing.T, ctx context.Context) {
			// gozfs
			// gozfs/A
			// gozfs/A@1
			// gozfs/A@2
			// gozfs/AA
			// gozfs/AA@1
			// gozfs/AA@2
			// gozfs/AB
			// gozfs/AB@1
			// gozfs/AB@2
			// gozfs/B
			// gozfs/B@1
			// gozfs/B@2
			// gozfs/BA
			// gozfs/BA@1
			// gozfs/BA@2
			// gozfs/BB
			// gozfs/BB@1
			// gozfs/BB@2

			fs, err := GetFilesystem(ctx, "gozfs")
			require.NoError(t, err)

			fsA, err := CreateFilesystem(ctx, "gozfs/A", CreateFilesystemOptions{})
			require.NoError(t, err)

			fsAA, err := CreateFilesystem(ctx, "gozfs/A/A", CreateFilesystemOptions{})
			require.NoError(t, err)

			fsAB, err := CreateFilesystem(ctx, "gozfs/A/B", CreateFilesystemOptions{})
			require.NoError(t, err)

			fsB, err := CreateFilesystem(ctx, "gozfs/B", CreateFilesystemOptions{})
			require.NoError(t, err)

			fsBA, err := CreateFilesystem(ctx, "gozfs/B/A", CreateFilesystemOptions{})
			require.NoError(t, err)

			fsBB, err := CreateFilesystem(ctx, "gozfs/B/B", CreateFilesystemOptions{})
			require.NoError(t, err)

			sA1, err := fsA.Snapshot(ctx, "1")
			require.NoError(t, err)

			sA2, err := fsA.Snapshot(ctx, "2")
			require.NoError(t, err)

			sAA1, err := fsAA.Snapshot(ctx, "1")
			require.NoError(t, err)

			sAA2, err := fsAA.Snapshot(ctx, "2")
			require.NoError(t, err)

			sAB1, err := fsAB.Snapshot(ctx, "1")
			require.NoError(t, err)

			sAB2, err := fsAB.Snapshot(ctx, "2")
			require.NoError(t, err)

			sB1, err := fsB.Snapshot(ctx, "1")
			require.NoError(t, err)

			sB2, err := fsB.Snapshot(ctx, "2")
			require.NoError(t, err)

			sBA1, err := fsBA.Snapshot(ctx, "1")
			require.NoError(t, err)

			sBA2, err := fsBA.Snapshot(ctx, "2")
			require.NoError(t, err)

			sBB1, err := fsBB.Snapshot(ctx, "1")
			require.NoError(t, err)

			sBB2, err := fsBB.Snapshot(ctx, "2")
			require.NoError(t, err)

			fss, err := Filesystems(ctx)
			require.NoError(t, err)
			require.Len(t, fss, 7)
			assert.Equal(t, fs.Info.Name, fss[0].Info.Name)
			assert.Equal(t, fsA.Info.Name, fss[1].Info.Name)
			assert.Equal(t, fsAA.Info.Name, fss[2].Info.Name)
			assert.Equal(t, fsAB.Info.Name, fss[3].Info.Name)
			assert.Equal(t, fsB.Info.Name, fss[4].Info.Name)
			assert.Equal(t, fsBA.Info.Name, fss[5].Info.Name)
			assert.Equal(t, fsBB.Info.Name, fss[6].Info.Name)

			ss, err := Snapshots(ctx)
			require.NoError(t, err)
			require.Len(t, ss, 12)
			assert.Equal(t, sA1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sA2.Info.Name, ss[1].Info.Name)
			assert.Equal(t, sAA1.Info.Name, ss[2].Info.Name)
			assert.Equal(t, sAA2.Info.Name, ss[3].Info.Name)
			assert.Equal(t, sAB1.Info.Name, ss[4].Info.Name)
			assert.Equal(t, sAB2.Info.Name, ss[5].Info.Name)
			assert.Equal(t, sB1.Info.Name, ss[6].Info.Name)
			assert.Equal(t, sB2.Info.Name, ss[7].Info.Name)
			assert.Equal(t, sBA1.Info.Name, ss[8].Info.Name)
			assert.Equal(t, sBA2.Info.Name, ss[9].Info.Name)
			assert.Equal(t, sBB1.Info.Name, ss[10].Info.Name)
			assert.Equal(t, sBB2.Info.Name, ss[11].Info.Name)

			fss, err = fs.Children(ctx)
			require.NoError(t, err)
			require.Len(t, fss, 2)
			assert.Equal(t, fsA.Info.Name, fss[0].Info.Name)
			assert.Equal(t, fsB.Info.Name, fss[1].Info.Name)

			fss, err = fsA.Children(ctx)
			require.NoError(t, err)
			require.Len(t, fss, 2)
			assert.Equal(t, fsAA.Info.Name, fss[0].Info.Name)
			assert.Equal(t, fsAB.Info.Name, fss[1].Info.Name)

			fss, err = fsB.Children(ctx)
			require.NoError(t, err)
			require.Len(t, fss, 2)
			assert.Equal(t, fsBA.Info.Name, fss[0].Info.Name)
			assert.Equal(t, fsBB.Info.Name, fss[1].Info.Name)

			fss, err = fsAA.Children(ctx)
			require.NoError(t, err)
			require.Len(t, fss, 0)

			fss, err = fsAB.Children(ctx)
			require.NoError(t, err)
			require.Len(t, fss, 0)

			fss, err = fsBA.Children(ctx)
			require.NoError(t, err)
			require.Len(t, fss, 0)

			fss, err = fsBB.Children(ctx)
			require.NoError(t, err)
			require.Len(t, fss, 0)

			ss, err = fs.Snapshots(ctx)
			require.NoError(t, err)
			require.Len(t, ss, 0)

			ss, err = fsA.Snapshots(ctx)
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sA1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sA2.Info.Name, ss[1].Info.Name)

			ss, err = fsAA.Snapshots(ctx)
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sAA1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sAA2.Info.Name, ss[1].Info.Name)

			ss, err = fsAB.Snapshots(ctx)
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sAB1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sAB2.Info.Name, ss[1].Info.Name)

			ss, err = fsB.Snapshots(ctx)
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sB1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sB2.Info.Name, ss[1].Info.Name)

			ss, err = fsBA.Snapshots(ctx)
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sBA1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sBA2.Info.Name, ss[1].Info.Name)

			ss, err = fsBB.Snapshots(ctx)
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sBB1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sBB2.Info.Name, ss[1].Info.Name)
		},
	},
	{
		Name: "TestMount",
		Fn: func(t *testing.T, ctx context.Context) {
			const file = "/gozfs/fs/content"

			fs, err := CreateFilesystem(ctx, "gozfs/fs", CreateFilesystemOptions{})
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(file, []byte("test"), 0o600))
			require.NoError(t, fs.Unmount(ctx))
			_, err = ioutil.ReadFile(file)
			assert.Error(t, err)

			require.NoError(t, fs.Mount(ctx))
			content, err := ioutil.ReadFile(file)
			require.NoError(t, err)
			assert.Equal(t, "test", string(content))
		},
	},
	{
		Name: "TestEncryption",
		Fn: func(t *testing.T, ctx context.Context) {
			const file = "/gozfs/fs/content"
			const password = "supersecret"

			fs, err := CreateFilesystem(ctx, "gozfs/fs", CreateFilesystemOptions{Password: password})
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(file, []byte("test"), 0o600))
			require.NoError(t, fs.Unmount(ctx))
			require.NoError(t, fs.UnloadKey(ctx))
			_, err = ioutil.ReadFile(file)
			assert.Error(t, err)
			assert.Error(t, fs.Mount(ctx))

			require.NoError(t, fs.LoadKey(ctx, password))
			require.NoError(t, fs.Mount(ctx))
			content, err := ioutil.ReadFile(file)
			require.NoError(t, err)
			assert.Equal(t, "test", string(content))
		},
	},
	{
		Name: "TestSend",
		Fn: func(t *testing.T, ctx context.Context) {
			fs, err := CreateFilesystem(ctx, "gozfs/fs", CreateFilesystemOptions{})
			require.NoError(t, err)

			require.NoError(t, ioutil.WriteFile("/gozfs/fs/content", []byte("test1"), 0o600))
			s1, err := fs.Snapshot(ctx, "image1")
			require.NoError(t, err)
			require.NoError(t, s1.SetProperty(ctx, "test:prop", "value1"))

			require.NoError(t, ioutil.WriteFile("/gozfs/fs/content", []byte("test2"), 0o600))
			s2, err := fs.Snapshot(ctx, "image2")
			require.NoError(t, err)
			require.NoError(t, s2.SetProperty(ctx, "test:prop", "value2"))

			var sr1 *Snapshot
			r, w := io.Pipe()
			require.NoError(t, parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("send", parallel.Continue, func(ctx context.Context) error {
					return s1.Send(ctx, SendOptions{}, w)
				})
				spawn("receive", parallel.Exit, func(ctx context.Context) error {
					var err error
					sr1, err = ReceiveSnapshot(ctx, r, "gozfs/copy@received1")
					return err
				})
				return nil
			}))

			content, err := ioutil.ReadFile("/gozfs/copy/content")
			require.NoError(t, err)
			assert.Equal(t, "test1", string(content))

			_, exists, err := sr1.GetProperty(ctx, "test:prop")
			require.NoError(t, err)
			assert.False(t, exists)

			require.NoError(t, sr1.Rollback(ctx))

			var sr2 *Snapshot
			r, w = io.Pipe()
			require.NoError(t, parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("send", parallel.Continue, func(ctx context.Context) error {
					return s2.Send(ctx, SendOptions{IncrementFrom: s1, Properties: true}, w)
				})
				spawn("receive", parallel.Exit, func(ctx context.Context) error {
					var err error
					sr2, err = ReceiveSnapshot(ctx, r, "gozfs/copy@received2")
					return err
				})
				return nil
			}))

			content, err = ioutil.ReadFile("/gozfs/copy/content")
			require.NoError(t, err)
			assert.Equal(t, "test2", string(content))

			value, exists, err := sr2.GetProperty(ctx, "test:prop")
			require.NoError(t, err)
			assert.True(t, exists)
			assert.Equal(t, "value2", value)
		},
	},
	{
		Name: "TestSendRawEncrypted",
		Fn: func(t *testing.T, ctx context.Context) {
			const password = "supersecret"

			fs, err := CreateFilesystem(ctx, "gozfs/fs", CreateFilesystemOptions{Password: password})
			require.NoError(t, err)

			require.NoError(t, ioutil.WriteFile("/gozfs/fs/content", []byte("test"), 0o600))
			s, err := fs.Snapshot(ctx, "image")
			require.NoError(t, err)

			r, w := io.Pipe()
			require.NoError(t, parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("send", parallel.Continue, func(ctx context.Context) error {
					return s.Send(ctx, SendOptions{Raw: true}, w)
				})
				spawn("receive", parallel.Exit, func(ctx context.Context) error {
					_, err := ReceiveSnapshot(ctx, r, "gozfs/copy@received")
					return err
				})
				return nil
			}))

			_, err = ioutil.ReadFile("/gozfs/copy/content")
			require.Error(t, err)

			fsCopy, err := GetFilesystem(ctx, "gozfs/copy")
			require.NoError(t, err)
			require.Error(t, fsCopy.Mount(ctx))
			require.NoError(t, fsCopy.LoadKey(ctx, password))
			require.NoError(t, fsCopy.Mount(ctx))

			content, err := ioutil.ReadFile("/gozfs/copy/content")
			require.NoError(t, err)
			assert.Equal(t, "test", string(content))
		},
	},
	{
		Name: "TestHolds",
		Fn: func(t *testing.T, ctx context.Context) {
			fs, err := CreateFilesystem(ctx, "gozfs/fs", CreateFilesystemOptions{})
			require.NoError(t, err)

			s, err := fs.Snapshot(ctx, "image")
			require.NoError(t, err)

			require.NoError(t, s.Hold(ctx, "tag1"))
			require.NoError(t, s.Hold(ctx, "tag2"))

			holds, err := s.Holds(ctx)
			sort.Strings(holds)
			require.NoError(t, err)
			require.Len(t, holds, 2)
			assert.Equal(t, "tag1", holds[0])
			assert.Equal(t, "tag2", holds[1])

			require.NoError(t, s.Release(ctx, "tag1"))

			holds, err = s.Holds(ctx)
			require.NoError(t, err)
			require.Len(t, holds, 1)
			assert.Equal(t, "tag2", holds[0])

			require.NoError(t, s.Release(ctx, "tag2"))

			holds, err = s.Holds(ctx)
			require.NoError(t, err)
			require.Len(t, holds, 0)
		},
	},
}

func TestZFS(t *testing.T) {
	t.Cleanup(cleanZFS)
	cleanZFS()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, exec.Command("modprobe", "brd", "rd_nr=1", "rd_size=102400").Run())
	for _, test := range zfsTests {
		test := test
		require.NoError(t, exec.Command("zpool", "create", "gozfs", "/dev/ram0").Run())

		t.Run(test.Name, func(t *testing.T) {
			test.Fn(t, ctx)
		})

		require.NoError(t, exec.Command("zpool", "destroy", "gozfs").Run())
	}
	require.NoError(t, exec.Command("rmmod", "brd").Run())
}

func cleanZFS() {
	_ = exec.Command("zpool", "destroy", "gozfs").Run()
	_ = exec.Command("rmmod", "brd").Run()
}
