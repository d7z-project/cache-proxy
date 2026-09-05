package storeio

import (
	"context"
	"errors"
	"io/fs"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

const (
	responseCleanupInterval     = 6 * time.Hour
	responseCleanupContinuation = 2 * time.Second
)

func RegisterResponseCleanup(taskScheduler *scheduler.Scheduler, instance, tenant string, store *blobfs.Store, opts config.CleanupConfig) {
	if taskScheduler == nil {
		return
	}
	cleaner := &responseCleaner{store: store, tenant: tenant, opts: opts}
	taskScheduler.Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(instance, scheduler.TypeExpireCleanup, tenant),
		Interval: responseCleanupInterval,
		Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
			more, err := cleaner.cleanBatch(ctx)
			if err != nil {
				return nil, err
			}
			outcome := &scheduler.TaskOutcome{Result: "success"}
			if more {
				outcome.ContinueAfter = responseCleanupContinuation
			}
			return outcome, nil
		},
	})
}

// responseCleaner and its cursor belong to one serial scheduler task.
type responseCleaner struct {
	store  *blobfs.Store
	tenant string
	opts   config.CleanupConfig
	cursor string
}

func (c *responseCleaner) cleanBatch(ctx context.Context) (bool, error) {
	filesystem := c.store.TenantFS(c.tenant)
	first, err := fs.ReadDir(filesystem, "responses")
	if errors.Is(err, fs.ErrNotExist) {
		c.cursor = ""
		return false, nil
	}
	if err != nil {
		return false, err
	}
	inspected := 0
	now := time.Now()
	for _, firstEntry := range first {
		if !firstEntry.IsDir() {
			continue
		}
		firstPath := "responses/" + firstEntry.Name()
		second, readErr := fs.ReadDir(filesystem, firstPath)
		if readErr != nil {
			return false, readErr
		}
		for _, secondEntry := range second {
			if !secondEntry.IsDir() {
				continue
			}
			secondPath := firstPath + "/" + secondEntry.Name()
			objects, readErr := fs.ReadDir(filesystem, secondPath)
			if readErr != nil {
				return false, readErr
			}
			for _, entry := range objects {
				objectPath := secondPath + "/" + entry.Name()
				if entry.IsDir() || c.cursor != "" && objectPath <= c.cursor {
					continue
				}
				if err := ctx.Err(); err != nil {
					return false, err
				}
				inspected++
				info, statErr := c.store.StatObject(ctx, c.tenant, objectPath)
				if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
					return false, statErr
				}
				if statErr == nil {
					metadata, decodeErr := decodeResponseMetadata(info.Options["metadata"])
					corrupt := decodeErr != nil || responsePath(metadata.LogicalKey) != objectPath
					if (corrupt || !now.Before(metadata.DeleteAt)) && !c.opts.DryRun {
						if deleteErr := c.store.DeleteObject(ctx, c.tenant, objectPath); deleteErr != nil && !errors.Is(deleteErr, fs.ErrNotExist) {
							return false, deleteErr
						}
					}
				}
				c.cursor = objectPath
				if c.opts.BatchSize > 0 && inspected >= c.opts.BatchSize {
					return true, nil
				}
			}
		}
	}
	c.cursor = ""
	return false, nil
}
