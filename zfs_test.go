package zfs

import (
	"context"
	"io"
	"io/ioutil"
	"os/exec"
	"testing"

	"github.com/ridge/parallel"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	Name string
	Fn   func(t *testing.T)
}

var tests = []testCase{
	{
		Name: "TestCreateAndDestroyFilesystem",
		Fn: func(t *testing.T) {
			const name = "gozfs/fs"

			fs, err := CreateFilesystem(name, nil)
			require.NoError(t, err)
			assert.Equal(t, name, fs.Info.Name)
			assert.Equal(t, "/"+name, fs.Info.Mountpoint)

			require.NoError(t, fs.Destroy(DestroyDefault))
			_, err = GetFilesystem(name)
			assert.Error(t, err)
		},
	},
	{
		Name: "TestFilesystemProperties",
		Fn: func(t *testing.T) {
			fs, err := CreateFilesystem("gozfs/fs", map[string]string{"test:prop1": "value1", "test:prop2": "value2"})
			require.NoError(t, err)

			v1, err := fs.GetProperty("test:prop1")
			require.NoError(t, err)
			v2, err := fs.GetProperty("test:prop2")
			require.NoError(t, err)
			v3, err := fs.GetProperty("test:prop3")
			require.NoError(t, err)

			assert.Equal(t, "value1", v1)
			assert.Equal(t, "value2", v2)
			assert.Equal(t, "-", v3)

			require.NoError(t, fs.SetProperty("test:prop2", "value22"))
			require.NoError(t, fs.SetProperty("test:prop3", "value3"))

			v2, err = fs.GetProperty("test:prop2")
			require.NoError(t, err)
			v3, err = fs.GetProperty("test:prop3")
			require.NoError(t, err)

			assert.Equal(t, "value22", v2)
			assert.Equal(t, "value3", v3)
		},
	},
	{
		Name: "TestCreateAndDestroySnapshot",
		Fn: func(t *testing.T) {
			const fsName = "gozfs/fs"
			const sName = "test"

			fs, err := CreateFilesystem(fsName, nil)
			require.NoError(t, err)

			s, err := fs.Snapshot(sName)
			require.NoError(t, err)
			assert.Equal(t, fsName+"@"+sName, s.Info.Name)

			require.NoError(t, s.Destroy(DestroyDefault))
			_, err = GetSnapshot(fsName + "@" + sName)
			assert.Error(t, err)
		},
	},
	{
		Name: "TestSnapshotProperties",
		Fn: func(t *testing.T) {
			fs, err := CreateFilesystem("gozfs/fs", nil)
			require.NoError(t, err)

			s, err := fs.Snapshot("test")
			require.NoError(t, err)

			v, err := s.GetProperty("test:prop")
			require.NoError(t, err)

			assert.Equal(t, "-", v)

			require.NoError(t, s.SetProperty("test:prop", "value"))

			v, err = s.GetProperty("test:prop")
			require.NoError(t, err)

			assert.Equal(t, "value", v)
		},
	},
	{
		Name: "TestClone",
		Fn: func(t *testing.T) {
			fs, err := CreateFilesystem("gozfs/fs", nil)
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile("/gozfs/fs/content", []byte("test"), 0o600))

			s, err := fs.Snapshot("image")
			require.NoError(t, err)

			fsClone, err := s.Clone("gozfs/fsclone")
			require.NoError(t, err)

			assert.Equal(t, "gozfs/fsclone", fsClone.Info.Name)

			content, err := ioutil.ReadFile("/gozfs/fsclone/content")
			require.NoError(t, err)

			assert.Equal(t, "test", string(content))
		},
	},
	{
		Name: "TestRollback",
		Fn: func(t *testing.T) {
			const file = "/gozfs/fs/content"

			fs, err := CreateFilesystem("gozfs/fs", nil)
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(file, []byte("test"), 0o600))

			s, err := fs.Snapshot("image")
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(file, []byte("test2"), 0o600))

			_, err = fs.Snapshot("image2")
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(file, []byte("test3"), 0o600))

			require.NoError(t, s.Rollback())
			content, err := ioutil.ReadFile(file)
			require.NoError(t, err)

			assert.Equal(t, "test", string(content))
		},
	},
	{
		Name: "TestListing",
		Fn: func(t *testing.T) {
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

			fs, err := GetFilesystem("gozfs")
			require.NoError(t, err)

			fsA, err := CreateFilesystem("gozfs/A", nil)
			require.NoError(t, err)

			fsAA, err := CreateFilesystem("gozfs/A/A", nil)
			require.NoError(t, err)

			fsAB, err := CreateFilesystem("gozfs/A/B", nil)
			require.NoError(t, err)

			fsB, err := CreateFilesystem("gozfs/B", nil)
			require.NoError(t, err)

			fsBA, err := CreateFilesystem("gozfs/B/A", nil)
			require.NoError(t, err)

			fsBB, err := CreateFilesystem("gozfs/B/B", nil)
			require.NoError(t, err)

			sA1, err := fsA.Snapshot("1")
			require.NoError(t, err)

			sA2, err := fsA.Snapshot("2")
			require.NoError(t, err)

			sAA1, err := fsAA.Snapshot("1")
			require.NoError(t, err)

			sAA2, err := fsAA.Snapshot("2")
			require.NoError(t, err)

			sAB1, err := fsAB.Snapshot("1")
			require.NoError(t, err)

			sAB2, err := fsAB.Snapshot("2")
			require.NoError(t, err)

			sB1, err := fsB.Snapshot("1")
			require.NoError(t, err)

			sB2, err := fsB.Snapshot("2")
			require.NoError(t, err)

			sBA1, err := fsBA.Snapshot("1")
			require.NoError(t, err)

			sBA2, err := fsBA.Snapshot("2")
			require.NoError(t, err)

			sBB1, err := fsBB.Snapshot("1")
			require.NoError(t, err)

			sBB2, err := fsBB.Snapshot("2")
			require.NoError(t, err)

			fss, err := Filesystems()
			require.NoError(t, err)
			require.Len(t, fss, 7)
			assert.Equal(t, fs.Info.Name, fss[0].Info.Name)
			assert.Equal(t, fsA.Info.Name, fss[1].Info.Name)
			assert.Equal(t, fsAA.Info.Name, fss[2].Info.Name)
			assert.Equal(t, fsAB.Info.Name, fss[3].Info.Name)
			assert.Equal(t, fsB.Info.Name, fss[4].Info.Name)
			assert.Equal(t, fsBA.Info.Name, fss[5].Info.Name)
			assert.Equal(t, fsBB.Info.Name, fss[6].Info.Name)

			ss, err := Snapshots()
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

			fss, err = fs.Children()
			require.NoError(t, err)
			require.Len(t, fss, 2)
			assert.Equal(t, fsA.Info.Name, fss[0].Info.Name)
			assert.Equal(t, fsB.Info.Name, fss[1].Info.Name)

			fss, err = fsA.Children()
			require.NoError(t, err)
			require.Len(t, fss, 2)
			assert.Equal(t, fsAA.Info.Name, fss[0].Info.Name)
			assert.Equal(t, fsAB.Info.Name, fss[1].Info.Name)

			fss, err = fsB.Children()
			require.NoError(t, err)
			require.Len(t, fss, 2)
			assert.Equal(t, fsBA.Info.Name, fss[0].Info.Name)
			assert.Equal(t, fsBB.Info.Name, fss[1].Info.Name)

			fss, err = fsAA.Children()
			require.NoError(t, err)
			require.Len(t, fss, 0)

			fss, err = fsAB.Children()
			require.NoError(t, err)
			require.Len(t, fss, 0)

			fss, err = fsBA.Children()
			require.NoError(t, err)
			require.Len(t, fss, 0)

			fss, err = fsBB.Children()
			require.NoError(t, err)
			require.Len(t, fss, 0)

			ss, err = fs.Snapshots()
			require.NoError(t, err)
			require.Len(t, ss, 0)

			ss, err = fsA.Snapshots()
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sA1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sA2.Info.Name, ss[1].Info.Name)

			ss, err = fsAA.Snapshots()
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sAA1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sAA2.Info.Name, ss[1].Info.Name)

			ss, err = fsAB.Snapshots()
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sAB1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sAB2.Info.Name, ss[1].Info.Name)

			ss, err = fsB.Snapshots()
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sB1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sB2.Info.Name, ss[1].Info.Name)

			ss, err = fsBA.Snapshots()
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sBA1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sBA2.Info.Name, ss[1].Info.Name)

			ss, err = fsBB.Snapshots()
			require.NoError(t, err)
			require.Len(t, ss, 2)
			assert.Equal(t, sBB1.Info.Name, ss[0].Info.Name)
			assert.Equal(t, sBB2.Info.Name, ss[1].Info.Name)
		},
	},
	{
		Name: "TestMount",
		Fn: func(t *testing.T) {
			const file = "/gozfs/fs/content"

			fs, err := CreateFilesystem("gozfs/fs", nil)
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(file, []byte("test"), 0o600))
			require.NoError(t, fs.Unmount())
			_, err = ioutil.ReadFile(file)
			assert.Error(t, err)

			require.NoError(t, fs.Mount())
			content, err := ioutil.ReadFile(file)
			require.NoError(t, err)
			assert.Equal(t, "test", string(content))
		},
	},
	{
		Name: "TestEncryption",
		Fn: func(t *testing.T) {
			const file = "/gozfs/fs/content"
			const password = "supersecret"

			fs, err := CreateFilesystem("gozfs/fs", map[string]string{"password": password})
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(file, []byte("test"), 0o600))
			require.NoError(t, fs.Unmount())
			require.NoError(t, fs.UnloadKey())
			_, err = ioutil.ReadFile(file)
			assert.Error(t, err)
			assert.Error(t, fs.Mount())

			require.NoError(t, fs.LoadKey(password))
			require.NoError(t, fs.Mount())
			content, err := ioutil.ReadFile(file)
			require.NoError(t, err)
			assert.Equal(t, "test", string(content))
		},
	},
	{
		Name: "TestSend",
		Fn: func(t *testing.T) {
			fs, err := CreateFilesystem("gozfs/fs", nil)
			require.NoError(t, err)

			require.NoError(t, ioutil.WriteFile("/gozfs/fs/content", []byte("test1"), 0o600))
			s1, err := fs.Snapshot("image1")
			require.NoError(t, err)

			require.NoError(t, ioutil.WriteFile("/gozfs/fs/content", []byte("test2"), 0o600))
			s2, err := fs.Snapshot("image2")
			require.NoError(t, err)

			var sr1 *Snapshot
			r, w := io.Pipe()
			require.NoError(t, parallel.Run(context.Background(), func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("send", parallel.Continue, func(ctx context.Context) error {
					return s1.Send(w)
				})
				spawn("receive", parallel.Exit, func(ctx context.Context) error {
					var err error
					sr1, err = ReceiveSnapshot(r, "gozfs/copy@received1")
					return err
				})
				return nil
			}))

			content, err := ioutil.ReadFile("/gozfs/copy/content")
			require.NoError(t, err)
			assert.Equal(t, "test1", string(content))

			require.NoError(t, sr1.Rollback())

			r, w = io.Pipe()
			require.NoError(t, parallel.Run(context.Background(), func(ctx context.Context, spawn parallel.SpawnFn) error {
				spawn("send", parallel.Continue, func(ctx context.Context) error {
					return s2.IncrementalSend(s1, w)
				})
				spawn("receive", parallel.Exit, func(ctx context.Context) error {
					_, err := ReceiveSnapshot(r, "gozfs/copy@received2")
					return err
				})
				return nil
			}))

			content, err = ioutil.ReadFile("/gozfs/copy/content")
			require.NoError(t, err)
			assert.Equal(t, "test2", string(content))
		},
	},
}

func TestZFS(t *testing.T) {
	clean()
	defer clean()

	require.NoError(t, exec.Command("modprobe", "brd", "rd_nr=1", "rd_size=102400").Run())
	for _, test := range tests {
		require.NoError(t, exec.Command("zpool", "create", "gozfs", "/dev/ram0").Run())

		t.Run(test.Name, test.Fn)

		require.NoError(t, exec.Command("zpool", "destroy", "gozfs").Run())
	}
	require.NoError(t, exec.Command("rmmod", "brd").Run())
}

func clean() {
	_ = exec.Command("zpool", "destroy", "gozfs").Run()
	_ = exec.Command("rmmod", "brd").Run()
}
