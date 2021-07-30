package util

import (
	"awesomeGFS/gfs"
	"time"
)

type NamespaceLogAction struct {
	T time.Time
	Type int
	Serial int
	Path1 string
	Path2 string
	Length int64
	Chunks int64
}

type ChunkLogAction struct {
	T time.Time
	Type int
	Serial int
	Path1 gfs.Path
	Path2 gfs.Path
	ChunkId gfs.ChunkId
	Version  gfs.ChunkVersion
	ChunkNum gfs.ChunkId
}

// log path
const (
	LogPath           = "gfs/log"
	LogPathMasterMeta = LogPath + "/gfs-master.meta"
	LogPathChunkServerMeta = LogPath + "/gfs-chunk-server.meta"
	LogPathNamespace  = LogPath + "/namespace.log"
	LogPathChunk      = LogPath + "/chunk.log"
	LogPerm			  = 0755
)

const COMMIT = 0

// namespace manager actions
const (
	CREATE = 1
	MKDIR = 2
	DELETE = 3
	RENAME = 4
	UPDATE = 5
)

// chunk manager actions
const (
	UpdateChunk = 1
	RenameChunk = 2
	CreateChunk = 3
)
