package zbackup

import (
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

type testCase struct {
	Name string
	Fn   func(t *testing.T, ctx context.Context)
}

var tests = []testCase{}

func TestBackup(t *testing.T) {
	t.Cleanup(clean)
	clean()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, exec.Command("modprobe", "brd", "rd_nr=2", "rd_size=102400").Run())
	for _, test := range tests {
		test := test
		require.NoError(t, exec.Command("zpool", "create", "zbackupsrc", "/dev/ram0").Run())
		require.NoError(t, exec.Command("zpool", "create", "zbackupdst", "/dev/ram1").Run())

		t.Run(test.Name, func(t *testing.T) {
			test.Fn(t, ctx)
		})

		require.NoError(t, exec.Command("zpool", "destroy", "zbackupsrc").Run())
		require.NoError(t, exec.Command("zpool", "destroy", "zbackupdst").Run())
	}
	require.NoError(t, exec.Command("rmmod", "brd").Run())
}

func clean() {
	_ = exec.Command("zpool", "destroy", "zbackupsrc").Run()
	_ = exec.Command("zpool", "destroy", "zbackupdst").Run()
	_ = exec.Command("rmmod", "brd").Run()
}
