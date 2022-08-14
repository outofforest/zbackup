package zbackup

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/outofforest/go-zfs/v3"
	"github.com/outofforest/parallel"
	"github.com/pkg/errors"
)

// Config is the config for backup
type Config struct {
	BackupPool string
	Password   string
}

// Backup backups ZFS filesystems
func Backup(ctx context.Context, config Config) error {
	var (
		backupRoot         = config.BackupPool + "/backup"
		prefix             = "auto"
		holdTagLastPrefix  = "backup:last"
		holdTagNew         = "backup:new"
		namespace          = "co.exw.zbackup"
		propertyBackup     = "backup"
		propertyPool       = propertyBackup + ":pool"
		propertyKeep       = propertyBackup + ":keep"
		suffixLastSnapshot = "last:snapshot"
		suffixLastTime     = "last:time"
	)

	pool, err := zfs.ImportPool(ctx, config.BackupPool)
	if err != nil {
		if pool, err = zfs.GetPool(ctx, config.BackupPool); err != nil {
			return err
		}
	}
	defer func() {
		_ = pool.Export(ctx)
	}()

	poolFS, err := zfs.GetFilesystem(ctx, config.BackupPool)
	if err != nil {
		return err
	}

	backupPoolID, _, err := poolFS.GetProperty(ctx, namespace+":"+propertyPool)
	if err != nil {
		return err
	}
	if backupPoolID == "" {
		return errors.Errorf("pool name not set for backup pool: %s", config.BackupPool)
	}
	backupPoolID = strings.ToLower(backupPoolID)

	_, err = zfs.GetFilesystem(ctx, backupRoot)
	if err != nil {
		rootFS, err := zfs.CreateFilesystem(ctx, backupRoot, zfs.CreateFilesystemOptions{
			Password: config.Password,
			Properties: map[string]string{
				"compression": "lz4",
				"mountpoint":  "none",
			},
		})
		if err != nil {
			return err
		}
		if err := rootFS.UnloadKey(ctx); err != nil {
			return err
		}
	}

	now := time.Now()
	propertyLastSnapshot := propertyBackup + ":" + backupPoolID + ":" + suffixLastSnapshot
	propertyLastTime := propertyBackup + ":" + backupPoolID + ":" + suffixLastTime
	holdTagLast := holdTagLastPrefix + ":" + backupPoolID

	filesystems, err := zfs.Filesystems(ctx)
	if err != nil {
		return err
	}
	for _, fs := range filesystems {
		doBackup, _, err := fs.GetProperty(ctx, namespace+":"+propertyBackup)
		if err != nil {
			return err
		}
		if doBackup != "true" {
			continue
		}

		snapshotBackupNew, err := latestSnapshot(ctx, fs.Info.Name, prefix)
		if err != nil {
			return err
		}
		if snapshotBackupNew == nil {
			continue
		}

		snapshotBackupLast, err := latestHoldSnapshot(ctx, fs.Info.Name, prefix, holdTagLast)
		if err != nil {
			return err
		}

		encrypted, _, err := fs.GetProperty(ctx, "encryption")
		if err != nil {
			return err
		}

		nameParts := strings.Split(fs.Info.Name, "/")
		targetObjectName := backupRoot + "/" + strings.Join(nameParts[1:], "/")
		targetSnapshotName := targetObjectName + "@" + strings.Split(snapshotBackupNew.Info.Name, "@")[1]

		targetParent, err := zfs.GetFilesystem(ctx, backupRoot+"/"+strings.Join(nameParts[1:len(nameParts)-1], "/"))
		if err != nil {
			return err
		}

		encRoot := ""

		var targetObject *zfs.Filesystem
		if snapshotBackupLast != nil {
			if snapshotBackupNew.Info.Name == snapshotBackupLast.Info.Name {
				continue
			}
			if encrypted == "off" {
				var err error

				targetObject, err = zfs.GetFilesystem(ctx, targetObjectName)
				if err != nil {
					return err
				}

				encRoot, _, err = targetObject.GetProperty(ctx, "encryptionroot")
				if err != nil {
					return err
				}
			}
		} else {
			var err error
			encRoot, _, err = targetParent.GetProperty(ctx, "encryptionroot")
			if err != nil {
				return err
			}
		}

		fmt.Println(fs.Info.Name)
		if err := releaseHolds(ctx, fs.Info.Name, prefix, holdTagNew); err != nil {
			return err
		}
		if err := snapshotBackupNew.Hold(ctx, holdTagNew); err != nil {
			return err
		}

		var encRootFS *zfs.Filesystem
		if encRoot != "" {
			var err error
			encRootFS, err = zfs.GetFilesystem(ctx, encRoot)
			if err != nil {
				return err
			}

			if err := encRootFS.LoadKey(ctx, config.Password); err != nil {
				return err
			}

			var targetSnapshot *zfs.Snapshot
			err = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				r, w := io.Pipe()
				spawn("send", parallel.Continue, func(ctx context.Context) error {
					return snapshotBackupNew.Send(ctx, zfs.SendOptions{
						Raw:           encrypted != "off",
						IncrementFrom: snapshotBackupLast,
					}, w)
				})
				spawn("receive", parallel.Exit, func(ctx context.Context) error {
					var err error
					targetSnapshot, err = zfs.ReceiveSnapshot(ctx, r, targetSnapshotName)
					return err
				})
				return nil
			})
			if err != nil {
				return err
			}

			if encRootFS != nil {
				if err := encRootFS.UnloadKey(ctx); err != nil {
					return err
				}
			}

			if err := targetSnapshot.Hold(ctx, holdTagNew); err != nil {
				return err
			}

			if err := releaseHolds(ctx, fs.Info.Name, prefix, holdTagLast); err != nil {
				return err
			}
			if err := snapshotBackupNew.Hold(ctx, holdTagLast); err != nil {
				return err
			}
			if err := releaseHolds(ctx, fs.Info.Name, prefix, holdTagNew); err != nil {
				return err
			}

			if targetObject == nil {
				var err error
				targetObject, err = zfs.GetFilesystem(ctx, targetObjectName)
				if err != nil {
					return err
				}
			}

			if err := releaseHolds(ctx, targetObject.Info.Name, prefix, holdTagLast); err != nil {
				return err
			}
			if err := targetSnapshot.Hold(ctx, holdTagLast); err != nil {
				return err
			}
			if err := releaseHolds(ctx, targetObject.Info.Name, prefix, holdTagNew); err != nil {
				return err
			}

			if err := fs.SetProperty(ctx, propertyLastSnapshot, snapshotBackupNew.Info.Name); err != nil {
				return err
			}
			if err := fs.SetProperty(ctx, propertyLastTime, now.String()); err != nil {
				return err
			}

			keep, _, err := fs.GetProperty(ctx, namespace+":"+propertyKeep)
			if err != nil {
				return err
			}
			if keep != "" {
				keepInt, err := strconv.ParseUint(keep, 10, 64)
				if err != nil {
					return err
				}
				if err := removeOldBackups(ctx, targetObject.Info.Name, prefix, keepInt); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func getSnapshots(ctx context.Context, filesystem string, prefix string) ([]*zfs.Snapshot, error) {
	fs, err := zfs.GetFilesystem(ctx, filesystem)
	if err != nil {
		return nil, err
	}

	snapshots, err := fs.Snapshots(ctx)
	if err != nil {
		return nil, err
	}
	pattern := filesystem + "@" + prefix + "_"
	out := []*zfs.Snapshot{}
	for _, snapshot := range snapshots {
		if strings.HasPrefix(snapshot.Info.Name, pattern) {
			out = append(out, snapshot)
		}
	}
	return out, nil
}

func getSnapshotsWithHoldTag(ctx context.Context, filesystem, prefix, tag string) ([]*zfs.Snapshot, error) {
	snapshots, err := getSnapshots(ctx, filesystem, prefix)
	if err != nil {
		return nil, err
	}

	out := []*zfs.Snapshot{}
	for _, s := range snapshots {
		holds, err := s.Holds(ctx)
		if err != nil {
			return nil, err
		}
		for _, h := range holds {
			if h == tag {
				out = append(out, s)
			}
		}
	}
	return out, err
}

func releaseHolds(ctx context.Context, filesystem, prefix, tag string) error {
	snapshots, err := getSnapshotsWithHoldTag(ctx, filesystem, prefix, tag)
	if err != nil {
		return err
	}
	for _, s := range snapshots {
		if err := s.Release(ctx, tag); err != nil {
			return err
		}
	}
	return nil
}

func latestSnapshot(ctx context.Context, filesystem, prefix string) (*zfs.Snapshot, error) {
	snapshots, err := getSnapshots(ctx, filesystem, prefix)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, nil
	}
	return snapshots[len(snapshots)-1], nil
}

func latestHoldSnapshot(ctx context.Context, filesystem, prefix, tag string) (*zfs.Snapshot, error) {
	snapshots, err := getSnapshotsWithHoldTag(ctx, filesystem, prefix, tag)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, nil
	}
	return snapshots[len(snapshots)-1], nil
}

func removeOldBackups(ctx context.Context, filesystem, prefix string, keep uint64) error {
	snapshots, err := getSnapshots(ctx, filesystem, prefix)
	if err != nil {
		return err
	}

	if uint64(len(snapshots)) <= keep {
		return nil
	}

	for i := uint64(len(snapshots)) - keep; i > 0; i-- {
		if err := snapshots[i-1].Destroy(ctx, zfs.DestroyDeferDeletion); err != nil {
			return err
		}
	}
	return nil
}
