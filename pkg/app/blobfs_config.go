package app

import "gopkg.d7z.net/blobfs"

func appBlobFSConfig() blobfs.Config {
	cfg := blobfs.DefaultConfig()
	cfg.MaxOpenWriteSessions = 128
	cfg.Chunking.MaxSize = 4 << 20
	if cfg.Chunking.AvgSize > cfg.Chunking.MaxSize {
		cfg.Chunking.AvgSize = cfg.Chunking.MaxSize
	}
	if cfg.Chunking.MinSize > cfg.Chunking.AvgSize {
		cfg.Chunking.MinSize = cfg.Chunking.AvgSize
	}
	return cfg
}
