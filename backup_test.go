package zbackup

import (
	"context"
	"os/exec"
	"testing"

	"github.com/outofforest/go-zfs/v3"
	"github.com/outofforest/libexec"
	"github.com/outofforest/logger"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	Name string
	Fn   func(t *testing.T, ctx context.Context)
}

var config = Config{
	BackupPool: "zbackupdst",
	Password:   "secretsecret",
}

var tests = []testCase{
	{
		Name: "TestBackupTurnedOff",
		Fn: func(t *testing.T, ctx context.Context) {
			filesystem, err := zfs.CreateFilesystem(ctx, "zbackupsrc/filesystem", zfs.CreateFilesystemOptions{})
			require.NoError(t, err)
			_, err = filesystem.Snapshot(ctx, "auto_test")
			require.NoError(t, err)

			require.NoError(t, Backup(ctx, config))
		},
	},
}

func TestBackup(t *testing.T) {
	t.Cleanup(clean)
	clean()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ctx = logger.WithLogger(ctx, logger.New(logger.ToolDefaultConfig))

	require.NoError(t, libexec.Exec(ctx, exec.Command("modprobe", "brd", "rd_nr=2", "rd_size=102400")))
	for _, test := range tests {
		test := test

		require.NoError(t, libexec.Exec(ctx, exec.Command("zpool", "create", "zbackupsrc", "/dev/ram0")))
		require.NoError(t, libexec.Exec(ctx, exec.Command("zpool", "create", "zbackupdst", "/dev/ram1")))

		backupFS, err := zfs.GetFilesystem(ctx, "zbackupdst")
		require.NoError(t, err)
		require.NoError(t, backupFS.SetProperty(ctx, "co.exw.zbackup:backup:pool", "a"))

		require.NoError(t, libexec.Exec(ctx, exec.Command("zpool", "export", "zbackupdst")))

		t.Run(test.Name, func(t *testing.T) {
			test.Fn(t, ctx)
		})

		require.NoError(t, libexec.Exec(ctx, exec.Command("zpool", "import", "zbackupdst")))
		require.NoError(t, libexec.Exec(ctx, exec.Command("zpool", "destroy", "zbackupsrc")))
		require.NoError(t, libexec.Exec(ctx, exec.Command("zpool", "destroy", "zbackupdst")))
	}
	require.NoError(t, exec.Command("rmmod", "brd").Run())
}

func clean() {
	_ = exec.Command("zpool", "import", "zbackupdst")
	_ = exec.Command("zpool", "destroy", "zbackupsrc").Run()
	_ = exec.Command("zpool", "destroy", "zbackupdst").Run()
	_ = exec.Command("rmmod", "brd").Run()
}
