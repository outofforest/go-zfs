package zfs

import (
	"context"
	"os/exec"
	"testing"

	"github.com/outofforest/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var zpoolTests = []testCase{
	{
		Name: "TestPools",
		Fn: func(t *testing.T, ctx context.Context) {
			pools, err := Pools(ctx)
			require.NoError(t, err)
			require.Len(t, pools, 2)
			assert.Equal(t, "gozpool1", pools[0].Name)
			assert.Equal(t, "gozpool2", pools[1].Name)

			pool1, err := GetPool(ctx, "gozpool1")
			require.NoError(t, err)
			assert.Equal(t, "gozpool1", pool1.Name)

			pool2, err := GetPool(ctx, "gozpool2")
			require.NoError(t, err)
			assert.Equal(t, "gozpool2", pool2.Name)

			require.NoError(t, pool1.Export(ctx))
			pools, err = Pools(ctx)
			require.NoError(t, err)
			require.Len(t, pools, 1)
			assert.Equal(t, "gozpool2", pools[0].Name)

			require.NoError(t, pool2.Export(ctx))
			pools, err = Pools(ctx)
			require.NoError(t, err)
			require.Len(t, pools, 0)

			pool1, err = ImportPool(ctx, "gozpool1")
			require.NoError(t, err)
			assert.Equal(t, "gozpool1", pool1.Name)

			pool2, err = ImportPool(ctx, "gozpool2")
			require.NoError(t, err)
			assert.Equal(t, "gozpool2", pool2.Name)

			pools, err = Pools(ctx)
			require.NoError(t, err)
			require.Len(t, pools, 2)
			assert.Equal(t, "gozpool1", pools[0].Name)
			assert.Equal(t, "gozpool2", pools[1].Name)
		},
	},
}

func TestZPool(t *testing.T) {
	t.Cleanup(cleanZPool)
	cleanZPool()

	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	t.Cleanup(cancel)

	require.NoError(t, exec.Command("modprobe", "zfs").Run())
	require.NoError(t, exec.Command("modprobe", "brd", "rd_nr=2", "rd_size=102400").Run())
	for _, test := range zpoolTests {
		test := test
		require.NoError(t, exec.Command("zpool", "create", "gozpool1", "/dev/ram0").Run())
		require.NoError(t, exec.Command("zpool", "create", "gozpool2", "/dev/ram1").Run())

		t.Run(test.Name, func(t *testing.T) {
			test.Fn(t, ctx)
		})

		require.NoError(t, exec.Command("zpool", "destroy", "gozpool1").Run())
		require.NoError(t, exec.Command("zpool", "destroy", "gozpool2").Run())
	}
	require.NoError(t, exec.Command("rmmod", "brd").Run())
}

func cleanZPool() {
	_ = exec.Command("zpool", "destroy", "gozpool1").Run()
	_ = exec.Command("zpool", "destroy", "gozpool2").Run()
	_ = exec.Command("rmmod", "brd").Run()
}
