package master

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"awesomeGFS/gfs"
	"awesomeGFS/gfs/util"
	log "github.com/sirupsen/logrus"
)

// chunkManager manges chunks
// including mapping from file to chunk and from chunk to chunkServer
type chunkManager struct {
	sync.RWMutex

	logFile *os.File						// file for logging
	serialCt int							// log serial number
	logLock sync.Mutex						// lock for logging

	chunk map[gfs.ChunkId]*chunkInfo		// mapping from chunk id to chunk info
	file  map[gfs.Path]*fileInfo			// mapping from path to file info

	ChunkNum gfs.ChunkId					// number of total chunks
	replicasNeedList []gfs.ChunkId 			// list of chunks need a new replicas
											// (happens when some servers are disconnected)
}

type chunkInfo struct {
	sync.RWMutex
	location []gfs.ServerAddress // set of replica locations
	primary  gfs.ServerAddress   // primary chunkServer
	expire   time.Time           // lease expire time
	version  gfs.ChunkVersion
	checksum gfs.Checksum		// useless ???
	path     gfs.Path			// useless ???
}

type fileInfo struct {
	sync.RWMutex
	chunks []gfs.ChunkId
}

type serialFileInfo struct {
	Path gfs.Path
	Info []gfs.PersistentChunkInfo
}

// Deserialize deserialize the metadata from disk
func (cm *chunkManager) Deserialize(files []serialFileInfo) error {
	log.Info("chunkManager Deserializing...")
	cm.Lock()
	defer cm.Unlock()

	now := time.Now()
	for _, v := range files {
		log.Info("Master restore files ", v.Path)
		f := new(fileInfo)
		for _, ck := range v.Info {
			f.chunks = append(f.chunks, ck.Id)
			log.Info("Master restore chunk ", ck.Id)
			cm.chunk[ck.Id] = &chunkInfo{
				expire:   now,
				version:  ck.Version,
				checksum: ck.Checksum,
				path: v.Path,
			}
		}
		cm.ChunkNum += gfs.ChunkId(len(v.Info))
		cm.file[v.Path] = f
	}

	return nil
}

// Serialize serialize the metadata for storing to disk
func (cm *chunkManager) Serialize() []serialFileInfo {
	log.Info("chunkManager Serializing...")

	// acquiring lock
	cm.logLock.Lock()
	defer cm.logLock.Unlock()
	cm.RLock()
	defer cm.RUnlock()

	var ret []serialFileInfo
	for k, v := range cm.file {
		var chunks []gfs.PersistentChunkInfo
		for _, handle := range v.chunks {
			chunks = append(chunks, gfs.PersistentChunkInfo{
				Id:       handle,
				Version:  cm.chunk[handle].version,
				Length:   0,
				Checksum: 0,
			})
		}

		ret = append(ret, serialFileInfo{Path: k, Info: chunks})
	}

	// TODO: clear log file and restart log
	if cm.logFile != nil {
		err := cm.logFile.Close()
		if err != nil {log.Warning(err)}
	}
	err := os.Remove(util.LogPathChunk)
	if err != nil {log.Warning(err)}
	logFile, err := os.OpenFile(util.LogPathChunk, os.O_CREATE | os.O_RDWR | os.O_TRUNC, 0666)
	if err != nil {panic(err)}
	cm.logFile = logFile
	cm.serialCt = 0

	return ret
}

// LoadLog load log info into namespace manager
func (cm *chunkManager) LoadLog() {
	log.Info("chunkManager loading log...")
	// parse log and redo committed actions
	cm.ParseLog()
	// continue log
	logFile, err := os.OpenFile(util.LogPathChunk, os.O_CREATE | os.O_RDWR | os.O_APPEND, 0666)
	if err != nil {log.Warning(err)}
	cm.logFile = logFile
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[gfs.ChunkId]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	log.Info("-----------new chunk manager-----------")
	return cm
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(chunkId gfs.ChunkId, addr gfs.ServerAddress, useLock bool) error {
	var ck *chunkInfo
	var ok bool

	if useLock {
		cm.RLock()
		ck, ok = cm.chunk[chunkId]
		cm.RUnlock()

		ck.Lock()
		defer ck.Unlock()
	} else {
		ck, ok = cm.chunk[chunkId]
	}

	if !ok {
		return fmt.Errorf("cannot find chunk %v", chunkId)
	}

	ck.location = append(ck.location, addr)
	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(chunkId gfs.ChunkId) ([]gfs.ServerAddress, error) {
	cm.RLock()
	ck, ok := cm.chunk[chunkId]
	cm.RUnlock()

	if !ok {
		return nil, fmt.Errorf("cannot find chunk %v", chunkId)
	}
	return ck.location, nil
}

// GetChunkId returns the chunk id for (path, index).
func (cm *chunkManager) GetChunkId(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkId, error) {
	cm.RLock()
	fInfo, ok := cm.file[path]
	cm.RUnlock()

	if !ok {
		return -1, fmt.Errorf("cannot get handle for %v[%v]", path, index)
	}

	if index < 0 || int(index) >= len(fInfo.chunks) {
		return -1, fmt.Errorf("invalid index for file %v No.%v chunk", path, index)
	}

	return fInfo.chunks[index], nil
}

// GetNeedList clears the need list: remove qualified chunks && do de-dup, then return the list
func (cm *chunkManager) GetNeedList() []gfs.ChunkId {
	cm.Lock()
	defer cm.Unlock()

	// remove qualified chunks in the list
	var newList []gfs.ChunkId
	for _, chunkId := range cm.replicasNeedList {
		if len(cm.chunk[chunkId].location) < gfs.MinimumNumReplicas {
			newList = append(newList, chunkId)
		}
	}

	// sort list to do de-dup
	sort.Slice(newList, func(i, j int) bool { return newList[i] < newList[j] })

	// De-dup chunk ids in the list
	cm.replicasNeedList = make([]gfs.ChunkId, 0)
	for i, v := range newList {
		if i == 0 || v != newList[i-1] {
			cm.replicasNeedList = append(cm.replicasNeedList, v)
		}
	}

	if len(cm.replicasNeedList) > 0 {
		return cm.replicasNeedList
	} else {
		return nil
	}
}

// GetLeaseHolder returns lease info of the chunk && server addresses.
// If no one has a lease, grants one to a replica it chooses.
// TODO add log
func (cm *chunkManager) GetLeaseHolder(chunkId gfs.ChunkId) (*gfs.Lease, []gfs.ServerAddress, error) {
	cm.RLock()
	ck, ok := cm.chunk[chunkId]
	cm.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("invalid chunkId %v", chunkId)
	}

	ck.Lock()
	defer ck.Unlock()

	lease := &gfs.Lease{}
	var staleServers []gfs.ServerAddress

	// if expires, grant a new lease
	if ck.expire.Before(time.Now()) {
		// update && check version
		ck.version++
		arg := gfs.CheckVersionArg{Handle: chunkId, Version: ck.version}

		var newList []string
		var lock sync.Mutex // lock for newList

		// wait util all chunkServer check version done
		var wg sync.WaitGroup
		wg.Add(len(ck.location))
		for _, v := range ck.location {
			go func(addr gfs.ServerAddress) {
				var r gfs.CheckVersionReply

				// distinguish call error and r.Stale
				err := util.Call(addr, "ChunkServer.RPCCheckVersion", arg, &r)
				if err == nil && r.Stale == false {
					// if chunk is not stale
					lock.Lock()
					newList = append(newList, string(addr))
					lock.Unlock()
				} else {
					// add to garbage collection
					log.Warningf("detect stale chunk %v in %v (err: %v)", chunkId, addr, err)
					staleServers = append(staleServers, addr)
				}
				wg.Done()
			}(v)
		}
		wg.Wait()

		//sort.Strings(newList)
		ck.location = make([]gfs.ServerAddress, len(newList))
		for i := range newList {
			ck.location[i] = gfs.ServerAddress(newList[i])
		}
		log.Warning(chunkId, " lease location ", ck.location)

		if len(ck.location) < gfs.MinimumNumReplicas {
			cm.Lock()
			cm.replicasNeedList = append(cm.replicasNeedList, chunkId)
			cm.Unlock()

			if len(ck.location) == 0 {
				// !! ATTENTION !!
				ck.version--
				return nil, nil, fmt.Errorf("no replica of %v", chunkId)
			}
		}

		// TODO choose primary, !!error chunkId no replicas!!
		ck.primary = ck.location[0]
		ck.expire = time.Now().Add(gfs.LeaseExpire)

		// do logging
		serial := cm.Log(util.ChunkLogAction{Type: util.UpdateChunk, ChunkId: chunkId, Version: ck.version, ChunkNum: cm.ChunkNum})
		defer cm.Commit(serial)
	}

	// fill lease attributes
	lease.Primary = ck.primary
	lease.Expire = ck.expire
	for _, v := range ck.location {
		if v != ck.primary {
			lease.Secondaries = append(lease.Secondaries, v)
		}
	}

	return lease, staleServers, nil
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkId, primary gfs.ServerAddress) error {
	return nil
	//log.Fatal("unsupported ExtendLease")
	//cm.RLock()
	//ck, ok := cm.chunk[handle]
	//cm.RUnlock()
	//
	//ck.Lock()
	//defer ck.Unlock()
	//
	//if !ok {
	//	return fmt.Errorf("invalid chunk handle %v", handle)
	//}
	//
	//now := time.Now()
	//if ck.primary != primary && ck.expire.After(now) {
	//	return fmt.Errorf("%v does not hold the lease for chunk %v", primary, handle)
	//}
	//ck.primary = primary
	//ck.expire = now.Add(gfs.LeaseExpire)
	//return nil
}

// CreateChunk creates a new chunk for path. servers for the chunk are denoted by addrList
// returns new chunk id, and the servers that create the chunk successfully
// TODO add log
func (cm *chunkManager) CreateChunk(path gfs.Path, addrList []gfs.ServerAddress) (gfs.ChunkId, []gfs.ServerAddress, error) {
	cm.Lock()
	defer cm.Unlock()

	newChunkId := cm.ChunkNum
	cm.ChunkNum++

	// update file info: add new chunkId into chunk list
	fInfo, ok := cm.file[path]
	if !ok {
		fInfo = new(fileInfo)
		cm.file[path] = fInfo
	}
	fInfo.chunks = append(fInfo.chunks, newChunkId)

	// update chunk info: add new chunk info mapping
	ck := &chunkInfo{path: path}
	cm.chunk[newChunkId] = ck

	// do logging
	serial := cm.Log(util.ChunkLogAction{Type: util.CreateChunk, Path1: path, ChunkId: newChunkId, Version: ck.version, ChunkNum: cm.ChunkNum})
	defer cm.Commit(serial)

	// create new chunks on a series of chunkServers
	var errs string
	var success []gfs.ServerAddress
	for _, v := range addrList {
		var r gfs.CreateChunkReply

		err := util.Call(v, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: newChunkId}, &r)
		if err == nil { // register
			ck.location = append(ck.location, v)
			success = append(success, v)
		} else {
			errs += err.Error() + ";"
		}
	}

	// return new chunk id and replica list
	if errs == "" {
		return newChunkId, success, nil
	} else {
		// replicas are not enough, add to need list
		cm.replicasNeedList = append(cm.replicasNeedList, newChunkId)
		return newChunkId, success, fmt.Errorf(errs)
	}
}

// RemoveServerChunks removes disconnected chunks
// if replicas number of a chunk is less than gfs.MinimumNumReplicas, add it to need list
func (cm *chunkManager) RemoveServerChunks(chunkIds []gfs.ChunkId, server gfs.ServerAddress) error {

	errList := ""
	for _, chunkId := range chunkIds {
		cm.RLock()
		ck, ok := cm.chunk[chunkId]
		cm.RUnlock()

		if !ok {
			continue
		}

		// update chunk info
		ck.Lock()
		var newList []gfs.ServerAddress
		for i := range ck.location {
			if ck.location[i] != server {
				newList = append(newList, ck.location[i])
			}
		}
		ck.location = newList
		ck.expire = time.Now()
		num := len(ck.location)
		ck.Unlock()

		// if number of replica < gfs.MinimumNumReplicas, add the chunk to replicaNeedList
		if num < gfs.MinimumNumReplicas {
			cm.replicasNeedList = append(cm.replicasNeedList, chunkId)
			if num == 0 {
				log.Errorf("lose all replica of %chunkId", chunkId)
				errList += fmt.Sprintf("Lose all replicas of chunk %chunkId;", chunkId)
			}
		}
	}

	if errList == "" {
		return nil
	} else {
		return fmt.Errorf(errList)
	}
}

// Rename do rename mapping from path to chunks
// TODO add log
func (cm *chunkManager) Rename(source, target gfs.Path) error {
	cm.Lock()
	defer cm.Unlock()

	// find original file info
	fInfo, ok := cm.file[source]
	if !ok {return nil}
	//if !ok {return fmt.Errorf("rename source file does not exist")}

	// map target path to the file info
	cm.file[target] = fInfo
	// eliminate the mapping from source to file info
	delete(cm.file, source)

	// do logging
	serial := cm.Log(util.ChunkLogAction{Type: util.RenameChunk, Path1: source, Path2: target})
	defer cm.Commit(serial)

	return nil
}



// ---------util functions for namespace manager-----------

// Log log the action into log file
func (cm *chunkManager) Log(action util.ChunkLogAction) int {
	cm.logLock.Lock()
	defer cm.logLock.Unlock()

	// fill action
	cm.serialCt++
	action.T = time.Now()
	action.Serial = cm.serialCt

	// write into log file
	marshal, err := json.Marshal(action)
	_, err = cm.logFile.Write(append(marshal, '\n'))
	if err != nil {panic(err)}
	err = cm.logFile.Sync()
	if err != nil {panic(err)}

	return action.Serial
}

// Commit log commit of the action into log file
func (cm *chunkManager) Commit(serial int) {
	cm.logLock.Lock()
	defer cm.logLock.Unlock()

	// fill action
	action := util.NamespaceLogAction{Type: util.COMMIT, Serial: serial, T: time.Now()}

	// write into log file
	marshal, err := json.Marshal(action)
	_, err = cm.logFile.Write(append(marshal, '\n'))
	if err != nil {panic(err)}
	err = cm.logFile.Sync()
	if err != nil {panic(err)}

	return
}

// ParseLog read the log file and parse the action
func (cm *chunkManager) ParseLog() {
	var rd *bufio.Reader
	var line string
	var action util.ChunkLogAction
	var actionMap = make(map[int]util.ChunkLogAction)

	// open log file in read only mode
	logFile, err := os.OpenFile(util.LogPathChunk, os.O_RDONLY, 0666)
	if err != nil {
		log.Warning(err)
		return
	}
	defer logFile.Close()

	// parse log line after line
	rd = bufio.NewReader(logFile)
	for ;; {
		line, err = rd.ReadString('\n')
		if err != nil || io.EOF == err {break}

		// use json unmarshal
		err := json.Unmarshal([]byte(line), &action)
		if err != nil {
			log.Warning(err)
			break
		}
		// log serial number begin from 1
		if action.Serial < 1 {
			log.Warningf("invalid serial number %d", action.Serial)
			break
		}
		log.Info("log Unmarshal obj: ", action.Type, action.Serial, action.Path1)
		if action.Serial > cm.serialCt {cm.serialCt = action.Serial}
		if action.Type != util.COMMIT {actionMap[action.Serial] = action} else {
			err := cm.DoAction(actionMap[action.Serial])
			if err != nil {log.Warning(err)}
		}
	}
}

// DoAction redo the action
func (cm *chunkManager) DoAction(action util.ChunkLogAction) error {
	// handle 4 kinds of actions
	switch action.Type {
	case util.CreateChunk:
		cm.ChunkNum = action.ChunkNum
		// update file info: add new chunkId into chunk list
		fInfo, ok := cm.file[action.Path1]
		if !ok {
			fInfo = new(fileInfo)
			cm.file[action.Path1] = fInfo
		}
		fInfo.chunks = append(fInfo.chunks, action.ChunkId)
		// update chunk info: add new chunk info mapping
		ck := &chunkInfo{path: action.Path1, version: action.Version}
		cm.chunk[action.ChunkId] = ck
		break
	case util.UpdateChunk:
		cm.ChunkNum = action.ChunkNum
		ck, ok := cm.chunk[action.ChunkId]
		if !ok {return fmt.Errorf("no such chuck %d", action.ChunkId)}
		ck.version = action.Version
		break
	case util.RenameChunk:
		// find original file info
		fInfo, ok := cm.file[action.Path1]
		if !ok {return fmt.Errorf("rename source file does not exist")}
		// map target path to the file info
		cm.file[action.Path2] = fInfo
		// eliminate the mapping from source to file info
		delete(cm.file, action.Path1)
		break
	}

	return nil
}
