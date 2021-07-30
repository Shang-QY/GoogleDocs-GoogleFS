package gfs

import (
	"time"
)

/*
 *  ChunkServer
 */

// handshake
type CheckVersionArg struct {
	Handle  ChunkId
	Version ChunkVersion
}
type CheckVersionReply struct {
	Version ChunkVersion
	Stale bool
}

// chunk IO

type ForwardDataArg struct {
	DataID     DataBufferID
	Data       []byte
	ChainOrder []ServerAddress
}
type ForwardDataReply struct {
	ErrorCode ErrorCode
}

type CreateChunkArg struct {
	Handle ChunkId
}
type CreateChunkReply struct {
	ErrorCode ErrorCode
}

type WriteChunkArg struct {
	DataID      DataBufferID
	Offset      Offset
	Secondaries []ServerAddress
}
type WriteChunkReply struct {
	ErrorCode ErrorCode
}

type ApplyWriteChunkArg struct {
	DataID DataBufferID
	Offset Offset
}
type ApplyWriteChunkReply struct {
	ErrorCode ErrorCode
}

type PadChunkArg struct {
	Handle ChunkId
}
type PadChunkReply struct {
	ErrorCode ErrorCode
}

type ReadChunkArg struct {
	Handle ChunkId
	Offset Offset
	Length int
}
type ReadChunkReply struct {
	Data      []byte
	Length    int
	ErrorCode ErrorCode
}

// re-replication
type SendCopyArg struct {
	Handle  ChunkId
	Address ServerAddress
}
type SendCopyReply struct {
	ErrorCode ErrorCode
}

type ApplyCopyArg struct {
	Handle  ChunkId
	Data    []byte
	Version ChunkVersion
}
type ApplyCopyReply struct {
	ErrorCode ErrorCode
}

// no use argument
type Nouse struct{}

/*
 *  Master
 */

// handshake
type HeartbeatArg struct {
	Address          ServerAddress // chunkServer address
}
type HeartbeatReply struct {
	Garbage []ChunkId
}

type ReportSelfArg struct {
}
type ReportSelfReply struct {
	Chunks []PersistentChunkInfo
}

// chunk info
type GetPrimaryAndSecondariesArg struct {
	Handle ChunkId
}
type GetPrimaryAndSecondariesReply struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress
}

type ExtendLeaseArg struct {
	Handle  ChunkId
	Address ServerAddress
}
type ExtendLeaseReply struct {
	Expire time.Time
}

type GetReplicasArg struct {
	Handle ChunkId
}
type GetReplicasReply struct {
	Locations []ServerAddress
}

type SetFileInfoArg struct {
	Path Path
	Length int64
	Chunks int64
}
type SetFileInfoReply struct {}

type GetFileInfoArg struct {
	Path Path
}
type GetFileInfoReply struct {
	IsDir  bool
	Length int64
	Chunks int64
}

type GetChunkHandleArg struct {
	Path  Path
	Index ChunkIndex
}
type GetChunkHandleReply struct {
	Handle ChunkId
}

type BackupMasterCreateChunkArg struct {
	Path Path
	Addresses []ServerAddress
}
type BackupMasterCreateChunkReply struct {
	Handle ChunkId
}

// namespace operation
type OpenFileArg struct {
	Path Path
}
type OpenFileReply struct{}

type CreateFileArg struct {
	Path Path
}
type CreateFileReply struct{}

type DeleteFileArg struct {
	Path Path
}
type DeleteFileReply struct{}

type RenameFileArg struct {
	Source Path
	Target Path
}
type RenameFileReply struct{}

type MkdirArg struct {
	Path Path
}
type MkdirReply struct{}

type ListArg struct {
	Path Path
}
type ListReply struct {
	Files []PathInfo
}
