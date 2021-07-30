package chunkserver

import (
	"encoding/gob"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	"awesomeGFS/gfs"
	"awesomeGFS/gfs/util"
)

// ChunkServer struct
type ChunkServer struct {
	lock     sync.RWMutex
	address  gfs.ServerAddress // chunkServer address
	master   gfs.ServerAddress // master address
	rootDir  string            // path to data storage
	l        net.Listener
	shutdown chan struct{}

	dl                     *downloadBuffer            // expiring download buffer
	chunk                  map[gfs.ChunkId]*chunkInfo // chunk information
	dead                   bool                       // set to ture if server is shutdown
	pendingLeaseExtensions *util.ArraySet             // pending lease extension
	garbage                []gfs.ChunkId              // garbage
}

type chunkInfo struct {
	sync.RWMutex
	length    gfs.Offset
	version   gfs.ChunkVersion // version number of the chunk in disk
	checksum  gfs.Checksum
	abandoned bool                           // unrecoverable error
}

const (
	MetaFileName = "gfs-server.meta"
	FilePerm     = 0755
)

// NewAndServe starts a chunkServer and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, rootDir string) *ChunkServer {
	cs := &ChunkServer{
		address:  addr,
		shutdown: make(chan struct{}),
		master:   masterAddr,
		rootDir:  rootDir,
		dl:       newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingLeaseExtensions: new(util.ArraySet),
		chunk: make(map[gfs.ChunkId]*chunkInfo),
	}

	var err error
	rps := rpc.NewServer()
	err = rps.Register(cs)
	if err != nil {
		log.Fatal("chunkServer rpc register error:", err)
	}
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("chunkServer listen error:", e)
	}
	cs.l = l

	// Mkdir
	_, err = os.Stat(rootDir)
	if err != nil { 							// rootDir not exist
		err = os.Mkdir(rootDir, FilePerm)
		if err != nil {
			log.Fatal("error in mkdir ", err)
		}
	}

	err = cs.loadMeta()
	if err != nil {
		log.Warning("Error in load metadata: ", err)
	}

	// RPC Handler
	go func() {
		connZookeeper := util.GetConnect([]string {util.ZkList})
		defer connZookeeper.Close()

		GetPrimaryMaster(connZookeeper, cs)

		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			conn, _err := cs.l.Accept()
			if _err == nil {
				go func() {
					rps.ServeConn(conn)
					_ = conn.Close()
				}()
			} else {
				if !cs.dead {
					log.Fatal("chunkServer accept error: ", _err)
				}
			}
		}
	}()

	// Background Activity
	// heartbeat, store persistent meta, garbage collection ...
	go func() {
		heartbeatTicker := time.Tick(gfs.HeartbeatInterval)
		storeTicker := time.Tick(gfs.ServerStoreInterval)
		garbageTicker := time.Tick(gfs.GarbageCollectionInt)
		quickStart := make(chan bool, 1) // send first heartbeat right away..
		quickStart <- true
		for {
			var _err error
			var branch string
			select {
			case <-cs.shutdown:
				return
			case <-quickStart:
				branch = "heartbeat"
				_err = cs.heartbeat()
			case <-heartbeatTicker:
				branch = "heartbeat"
				_err = cs.heartbeat()
			case <-storeTicker:
				branch = "storeMeta"
				_err = cs.storeMeta()
			case <-garbageTicker:
				branch = "garbageCollection"
				_err = cs.garbageCollection()
			}

			if _err != nil {
				log.Errorf("%v background(%v) error %v", cs.address, branch, _err)
			}
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, rootDir, masterAddr)

	return cs
}

func GetPrimaryMaster(conn *zk.Conn, server *ChunkServer) {
	go func() {
		for {
			// when chunkServer dead (in emulation), we need to let chunkServer end watching by ending this go routing
			select {
			case <-server.shutdown:
				return
			default:
			}

			monitorPath := "/master"
			children, _, _err := conn.Children(monitorPath)
			if _err != nil {
				log.Error(_err)
				continue
			}

			if len(children) > 0 {
				sort.Slice(children, func(i, j int) bool {
					iNum, _ := strconv.Atoi(children[i][len(children[i])-10:])
					jNum, _ := strconv.Atoi(children[j][len(children[j])-10:])
					return iNum < jNum
				})
				pAddr, _, err_ := conn.Get("/master/" + children[0])
				if err_ != nil {
					log.Error(err_)
					continue
				}
				server.master = gfs.ServerAddress(pAddr)
				fmt.Printf("I( %s ) find primary master '%s' \n", server.address, pAddr)

				_, _, events, _err := conn.ExistsW("/master/" + children[0])
				if _err != nil {
					log.Error(_err)
					continue
				}
				for {
					event := <- events
					fmt.Println("path:", event.Path)
					fmt.Println("type:", event.Type.String())
					if event.Type.String() == "EventNodeDeleted" {
						fmt.Println("[EventNodeDeleted]")
						break
					}
					// abnormal situation need to handle, otherwise thread can't exit the loop and end the go routing
					if event.Type.String() == "EventNotWatching" {
						fmt.Println("[EventNotWatching]")
						break
					}
				}
			}
		}
	}()
}


// heartbeat calls master regularly to report chunkServer's status
func (cs *ChunkServer) heartbeat() error {
	pe := cs.pendingLeaseExtensions.GetAllAndClear()
	le := make([]gfs.ChunkId, len(pe))
	for i, v := range pe {
		le[i] = v.(gfs.ChunkId)
	}
	args := &gfs.HeartbeatArg{
		Address:         cs.address,
	}
	var r gfs.HeartbeatReply
	err := util.Call(cs.master, "Master.RPCHeartbeat", args, &r)
	if err != nil {
		return err
	}

	cs.garbage = append(cs.garbage, r.Garbage...)
	return err
}

// garbage collection  Note: no lock are needed, since the background activities are single thread
func (cs *ChunkServer) garbageCollection() error {
	for _, v := range cs.garbage {
		_ = cs.deleteChunk(v)
	}

	cs.garbage = make([]gfs.ChunkId, 0)
	return nil
}

// RPCReportSelf reports all chunks the server holds
func (cs *ChunkServer) RPCReportSelf(_ gfs.ReportSelfArg, reply *gfs.ReportSelfReply) error {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	log.Debug(cs.address, " report self start")
	var ret []gfs.PersistentChunkInfo
	for handle, ck := range cs.chunk {
		//log.Info(cs.address, " report ", handle)
		ret = append(ret, gfs.PersistentChunkInfo{
			Id:       handle,
			Version:  ck.version,
			Length:   ck.length,
			Checksum: ck.checksum,
		})
	}
	reply.Chunks = ret
	log.Debug(cs.address, " report self end")

	return nil
}

// loadMeta loads metadata from disk
func (cs *ChunkServer) loadMeta() error {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	filename := path.Join(cs.rootDir, MetaFileName)
	file, err := os.OpenFile(filename, os.O_RDONLY, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metas []gfs.PersistentChunkInfo
	dec := gob.NewDecoder(file)
	err = dec.Decode(&metas)
	if err != nil {
		return err
	}

	log.Infof("Server %v : load metadata len: %v", cs.address, len(metas))
	// load into memory
	for _, ck := range metas {
		log.Infof("Server %v restore %v version: %v length: %v", cs.address, ck.Id, ck.Version, ck.Length)
		cs.chunk[ck.Id] = &chunkInfo{
			length:  ck.Length,
			version: ck.Version,
		}
	}

	return nil
}

// storeMeta stores metadata to disk
func (cs *ChunkServer) storeMeta() error {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	filename := path.Join(cs.rootDir, MetaFileName)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metas []gfs.PersistentChunkInfo
	for handle, ck := range cs.chunk {
		metas = append(metas, gfs.PersistentChunkInfo{
			Id: handle, Length: ck.length, Version: ck.version,
		})
	}

	log.Infof("Server %v : store metadata len: %v", cs.address, len(metas))
	enc := gob.NewEncoder(file)
	err = enc.Encode(metas)

	return err
}

// Shutdown shuts the chunkServer down
//func (cs *ChunkServer) Shutdown(args gfs.Nouse, reply *gfs.Nouse) error {
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warning(cs.address, " Shutdown")
		cs.dead = true
		close(cs.shutdown)
		_ = cs.l.Close()
	}
	err := cs.storeMeta()
	if err != nil {
		log.Warning("error in store metadata: ", err)
	}
}

// RPCCheckVersion is called by master to check version and detect stale chunk
func (cs *ChunkServer) RPCCheckVersion(args gfs.CheckVersionArg, reply *gfs.CheckVersionReply) error {
	cs.lock.RLock()
	ck, ok := cs.chunk[args.Handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned\n", args.Handle)
	}

	ck.Lock()
	defer ck.Unlock()

	if ck.version + gfs.ChunkVersion(1) == args.Version {
		reply.Version = ck.version
		reply.Stale = false
		ck.version++
	} else if ck.version + gfs.ChunkVersion(1) < args.Version{
		reply.Version = ck.version
		reply.Stale = true
		log.Warningf("%v : stale chunk %v", cs.address, args.Handle)
		ck.abandoned = true
	} else {
		reply.Version = ck.version
		reply.Stale = false
	}
	return nil
}

// RPCForwardData is called by client or another replica who sends data to the current memory buffer.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
	//log.Warning(cs.address, " data 1 ", args.DataID)
	if _, ok := cs.dl.Get(args.DataID); ok {
		return fmt.Errorf("Data %v already exists\n", args.DataID)
	}

	//log.Infof("Server %v : get data %v", cs.address, args.DataID)
	//log.Warning(cs.address, "data 2 ", args.DataID)
	cs.dl.Set(args.DataID, args.Data)
	//log.Warning(cs.address, "data 3 ", args.DataID)

	if len(args.ChainOrder) > 0 {
		next := args.ChainOrder[0]
		args.ChainOrder = args.ChainOrder[1:]
		err := util.Call(next, "ChunkServer.RPCForwardData", args, reply)
		return err
	}
	//log.Warning(cs.address, "data 4 ", args.DataID)

	return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, _ *gfs.CreateChunkReply) error {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	log.Infof("Server %v : create chunk %v", cs.address, args.Handle)

	cs.chunk[args.Handle] = &chunkInfo{
		length: 0,
	}
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", args.Handle))
	_, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return nil
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned\n", handle)
	}

	// read from disk
	var err error
	reply.Data = make([]byte, args.Length)
	ck.RLock()
	reply.Length, err = cs.readChunk(handle, args.Offset, reply.Data)
	ck.RUnlock()
	if err == io.EOF {
		log.Infof("readChunk(%v, %v, %v) EOF\n", handle, args.Offset, reply.Data)
		reply.ErrorCode = gfs.ReadEOF
		return nil
	}

	if err != nil {
		return err
	}
	return nil
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, _ *gfs.WriteChunkReply) error {
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	newLen := args.Offset + gfs.Offset(len(data))
	if newLen > gfs.MaxChunkSize {
		return fmt.Errorf("writeChunk new length is too large. Size %v > MaxSize %v", len(data), gfs.MaxChunkSize)
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned\n", handle)
	}

	if err = func() error {
		ck.Lock()
		defer ck.Unlock()

		// apply to local
		wait := make(chan error, 1)
		go func() {
			err = cs.writeChunk(handle, data, args.Offset)
			if err != nil{
				ck.abandoned = true
			}
			wait <- err
		}()

		// call secondaries
		callArgs := gfs.ApplyWriteChunkArg{DataID: args.DataID, Offset: args.Offset}
		err = util.CallAll(args.Secondaries, "ChunkServer.RPCApplyWriteChunk", callArgs)
		if err != nil {
			return err
		}

		err = <-wait
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	return nil
}

// RPCApplyWriteChunk is called by primary to apply WriteChunks
func (cs *ChunkServer) RPCApplyWriteChunk(args gfs.ApplyWriteChunkArg, _ *gfs.ApplyWriteChunkReply) error {
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned\n", handle)
	}

	log.Infof("Server %v : get chunk %v", cs.address, handle)
	ck.Lock()
	defer ck.Unlock()
	err = cs.writeChunk(handle, data, args.Offset)
	if err != nil {
		ck.abandoned = true
	}
	return err
}

// RPCSendCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, _ *gfs.SendCopyReply) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned\n", handle)
	}

	ck.RLock()
	defer ck.RUnlock()

	log.Infof("Server %v : Send copy of %v to %v", cs.address, handle, args.Address)
	data := make([]byte, ck.length)
	_, err := cs.readChunk(handle, 0, data)
	if err != nil {
		return err
	}

	var r gfs.ApplyCopyReply
	err = util.Call(args.Address, "ChunkServer.RPCApplyCopy", gfs.ApplyCopyArg{Handle: handle, Data: data, Version: ck.version}, &r)
	if err != nil {
		return err
	}

	return nil
}

// RPCApplyCopy is called by another replica
// rewrite the local version to given copy data
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, _ *gfs.ApplyCopyReply) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("Chunk %v does not exist or is abandoned\n", handle)
	}

	ck.Lock()
	defer ck.Unlock()

	log.Infof("Server %v : Apply copy of %v", cs.address, handle)
	ck.version = args.Version
	err := cs.writeChunk(handle, args.Data, 0)
	if err != nil {
		return err
	}
	log.Infof("Server %v : Apply done", cs.address)
	return nil
}

// writeChunk writes data at offset to a chunk at disk
func (cs *ChunkServer) writeChunk(handle gfs.ChunkId, data []byte, offset gfs.Offset) error {
	cs.lock.RLock()
	ck := cs.chunk[handle]
	cs.lock.RUnlock()

	// ck is already locked in top caller
	newLen := offset + gfs.Offset(len(data))
	if newLen > ck.length {
		ck.length = newLen
	}

	if newLen > gfs.MaxChunkSize {
		log.Fatal("new length > gfs.MaxChunkSize")
	}

	log.Infof("Server %v : write to chunk %v at %v len %v", cs.address, handle, offset, len(data))
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteAt(data, int64(offset))
	if err != nil {
		return err
	}

	return nil
}

// readChunk reads data at offset from a chunk at dist
func (cs *ChunkServer) readChunk(handle gfs.ChunkId, offset gfs.Offset, data []byte) (int, error) {
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))

	f, err := os.Open(filename)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	log.Infof("Server %v : read chunk %v at %v len %v", cs.address, handle, offset, len(data))
	return f.ReadAt(data, int64(offset))
}

// deleteChunk deletes a chunk during garbage collection
func (cs *ChunkServer) deleteChunk(handle gfs.ChunkId) error {
	cs.lock.Lock()
	delete(cs.chunk, handle)
	cs.lock.Unlock()

	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))
	err := os.Remove(filename)
	return err
}

// =================== DEBUG TOOLS ===================
func getContents(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close() // f.Close will run when we're finished.

	var result []byte
	buf := make([]byte, 100)
	for {
		n, err := f.Read(buf[0:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err // f will be closed if we return here.
		}
		result = append(result, buf[0:n]...) // append is discussed later.
	}
	return string(result), nil // f will be closed if we return here.
}

func (cs *ChunkServer) PrintSelf(no1 gfs.Nouse, no2 *gfs.Nouse) error {
	cs.lock.RLock()
	cs.lock.RUnlock()
	log.Info("============ ", cs.address, " ============")
	if cs.dead {
		log.Warning("DEAD")
	} else {
		for h, v := range cs.chunk {
			filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", h))
			log.Infof("chunk %v : version %v", h, v.version)
			str, _ := getContents(filename)
			log.Info(str)
		}
	}
	return nil
}
