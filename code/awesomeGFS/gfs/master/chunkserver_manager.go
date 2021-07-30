package master

import (
	"fmt"
	//"math/rand"
	"sync"
	"time"

	"awesomeGFS/gfs"
	"awesomeGFS/gfs/util"
	log "github.com/sirupsen/logrus"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers map[gfs.ServerAddress]*chunkServerInfo
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers: make(map[gfs.ServerAddress]*chunkServerInfo),
	}
	log.Info("-----------new chunk server manager-----------")
	return csm
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[gfs.ChunkId]bool // set of chunks that the chunkServer has
	garbage       []gfs.ChunkId
}

func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress, reply *gfs.HeartbeatReply) bool {
	csm.Lock()
	defer csm.Unlock()

	sv, ok := csm.servers[addr]
	if !ok {
		log.Info("New chunk server" + addr)
		csm.servers[addr] = &chunkServerInfo{time.Now(), make(map[gfs.ChunkId]bool), nil}
		return true
	} else {
		// send garbage
		reply.Garbage = csm.servers[addr].garbage
		csm.servers[addr].garbage = make([]gfs.ChunkId, 0)
		sv.lastHeartbeat = time.Now()
		return false
	}
}

// register a chunk to servers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkId) {
	csm.Lock()
	defer csm.Unlock()

	for _, v := range addrs {
		//csm.servers[v].chunks[handle] = true
		sv, ok := csm.servers[v]
		if ok {
			sv.chunks[handle] = true
		} else {
			log.Warning("add chunk in removed server ", sv)
		}
	}
}

// AddGarbage
func (csm *chunkServerManager) AddGarbage(addr gfs.ServerAddress, handle gfs.ChunkId) {
	csm.Lock()
	defer csm.Unlock()

	sv, ok := csm.servers[addr]
	if ok {
		sv.garbage = append(sv.garbage, handle)
		delete(sv.chunks, handle)
	}
}

// ChooseReReplication chooses servers to perform re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkId) (from, to gfs.ServerAddress, err error) {
	csm.RLock()
	defer csm.RUnlock()

	from = ""
	to = ""
	err = nil
	for a, v := range csm.servers {
		if v.chunks[handle] {
			from = a
		} else {
			to = a
		}
		if from != "" && to != "" {
			return
		}
	}
	err = fmt.Errorf("No enough server for replica %v\n", handle)
	return
}

func (csm *chunkServerManager) chooseTransferFromTo() (info []gfs.ChunkTransferInfo, err error) {
	var from, to gfs.ServerAddress
	var handle gfs.ChunkId

	csm.RLock()
	defer csm.RUnlock()

	var num = 0
	var serverNum = len(csm.servers)
	var serverAddr = make([]gfs.ServerAddress, serverNum)
	var serverChunkNum = make([]int, serverNum)
	var index = 0
	for addr, v := range csm.servers {
		serverAddr[index] = addr
		for _, flag := range v.chunks {
			if flag {
				serverChunkNum[index] += 1
				num += 1
			}
		}
		index += 1
	}
	averageChunkNum := num / serverNum + 1
	for i := 0; i < serverNum; i++ {
		// this server is the source
		if serverChunkNum[i] > averageChunkNum {
			transferNum := serverChunkNum[i] - averageChunkNum
			handles := make([]gfs.ChunkId, transferNum)
			cur := 0

			sv := csm.servers[serverAddr[i]]
			if sv != nil {
				for chunkId, flag := range sv.chunks {
					if flag {
						handles[cur] = chunkId
					}
					cur += 1
				}
			} else {return nil, err}

			for cur = 0; cur < transferNum; cur++ {
				// make clear "which chunk, from and to where"
				handle = handles[cur]
				from = serverAddr[i]
				var destIndex int
				for j := i; j < serverNum; j++ {
					if serverChunkNum[j] < averageChunkNum {
						to = serverAddr[j]
						destIndex = j
						break
					}
				}
				// put these info to reply
				info = append(info, gfs.ChunkTransferInfo{From: from, To: to, Handle: handle})
				log.Printf("-- TRANSFER chunk %v from %v to %v --", handle, from, to)
				log.Printf("before transfer %v from %v to %v, source chunkNum is: %d, dest chunkNum is: %d",
					handle, from, to, serverChunkNum[i], serverChunkNum[destIndex])
				// implement transfer
				var cr gfs.CreateChunkReply
				err = util.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &cr)
				if err != nil {
					return
				}
				var sr gfs.SendCopyReply
				err = util.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: handle, Address: to}, &sr)
				if err != nil {
					return
				}

				// update dest serverChunkNum
				serverChunkNum[destIndex]++
			}
			// update source serverChunkNum
			serverChunkNum[i] -= transferNum
		}
	}

	// report the situation
	for i := 0; i < serverNum; i++ {
		log.Printf("server ( %s ) has chunkNum: %d\n", serverAddr[i], serverChunkNum[i])
	}
	return info, nil
}

// ChooseServers returns servers to store new chunk
// called when a new chunk is create
func (csm *chunkServerManager) ChooseServers(num int) ([]gfs.ServerAddress, error) {

	if num > len(csm.servers) {
		return nil, fmt.Errorf("no enough servers for %v replicas", num)
	}

	csm.RLock()
	var all, ret []gfs.ServerAddress
	for a := range csm.servers {
		all = append(all, a)
	}
	csm.RUnlock()
	if len(all) == 0 {
		return ret, nil
	}

	choose, err := util.Sample(len(all), num)
	if err != nil {
		return nil, err
	}
	for _, v := range choose {
		ret = append(ret, all[v])
	}

	return ret, nil
}

// DetectDeadServers detect disconnected servers according to last heartbeat time
func (csm *chunkServerManager) DetectDeadServers() []gfs.ServerAddress {
	csm.RLock()
	defer csm.RUnlock()

	var ret []gfs.ServerAddress
	now := time.Now()
	for k, v := range csm.servers {
		if v.lastHeartbeat.Add(gfs.ServerTimeout).Before(now) {
			ret = append(ret, k)
		}
	}

	return ret
}

// RemoveServers removes metadata of disconnected server
// it returns the chunks that server holds
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) (handles []gfs.ChunkId, err error) {
	csm.Lock()
	defer csm.Unlock()

	err = nil
	sv, ok := csm.servers[addr]
	if !ok {
		err = fmt.Errorf("Cannot find chunk server %v\n", addr)
		return
	}
	for h, v := range sv.chunks {
		if v {
			handles = append(handles, h)
		}
	}
	delete(csm.servers, addr)

	return
}
