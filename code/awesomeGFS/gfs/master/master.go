package master

import (
	"encoding/gob"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"awesomeGFS/gfs"
	"awesomeGFS/gfs/util"
)

// Master Server struct
type Master struct {
	address    gfs.ServerAddress 	// master server address
	serverRoot string
	l          net.Listener

	shutdown   chan struct{}		// crash server, end RPC serve
	dead       bool 				// set to ture if server is shutdown

	hasPrimary   bool
	hasBackup    bool
	PrimaryAddr gfs.ServerAddress
	BackupAddr  gfs.ServerAddress

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

// New is used to construct a master to be used in test
func New(address gfs.ServerAddress, serverRoot string) *Master {
	master := &Master{
		address:    address,
		serverRoot: serverRoot,
		shutdown:   make(chan struct{}),
		hasPrimary: true,
		hasBackup:  false,
	}
	return master
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address gfs.ServerAddress, serverRoot string) *Master {
	// init master with basic config
	master := &Master{
		address:    address,
		serverRoot: serverRoot,
		shutdown:   make(chan struct{}),
		hasPrimary: true,
		hasBackup:  false,
	}

	// init master rpc listener
	rps := rpc.NewServer()
	_ = rps.Register(master)
	l, e := net.Listen("tcp", string(master.address))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	master.l = l

	// init master managers
	master.nm = newNamespaceManager()
	master.cm = newChunkManager()
	master.csm = newChunkServerManager()

	// init master mata data
	err := master.loadMeta()
	if err != nil {log.Warning(err)}

	// RPC Handler
	go func() {
		connZookeeper := util.GetConnect([]string {util.ZkList})
		defer connZookeeper.Close()
		// start listen master and backup
		ListenPrimary(connZookeeper, master)

		for {
			select {
			case <-master.shutdown:
				return
			default:
			}
			conn, err := master.l.Accept()
			if err == nil {
				go func() {
					rps.ServeConn(conn)
					_ = conn.Close()
				}()
			} else {
				if !master.dead {
					log.Fatal("master accept error:", err)
				}
			}
		}
	}()

	// Background Task: all the background activities
	// including: server disconnection handle, garbage collection, stale replica detection, etc
	go func() {
		checkTicker := time.Tick(gfs.ServerCheckInterval)
		storeTicker := time.Tick(gfs.MasterStoreInterval)
		loadBalanceTicker := time.Tick(gfs.MasterLoadBalanceInterval)
		for {
			var err error
			select {
			case <-master.shutdown:
				return
			case <-checkTicker:
				err = master.serverCheck()
			case <-storeTicker:
				err = master.storeMeta()
			case <-loadBalanceTicker:
				err = master.checkLoadBalance()
			}
			if err != nil {
				log.Error("Background error ", err)
			}
		}

	}()

	log.Infof("Master is running now at addr = %v...", address)

	return master
}

func ListenPrimary(conn *zk.Conn, master *Master) {
	masterFile, err := util.CreateMasterFile(conn, master.address)
	fmt.Printf("I( %s ) create masterFile '%s' \n", master.address, masterFile)
	if err != nil {
		fmt.Printf("%+v\n", err)
		return
	}

	snapshots, errors := util.Mirror(conn, "/master")
	go func() {
		for {
			select {
			case children := <-snapshots:
				fmt.Printf("I( %s ) see %+v\n", master.address, children)

				sort.Slice(children, func(i, j int) bool {
					iNum, _ := strconv.Atoi(children[i][len(children[i])-10:])
					jNum, _ := strconv.Atoi(children[j][len(children[j])-10:])
					return iNum < jNum
				})
				var index int
				for i, child := range children {
					if strings.Compare(child, masterFile) == 0 {
						index = i
						break
					}
				}

				if index == 0 {
					master.hasPrimary = false
					fmt.Printf("I( %s ) am primary \n", master.address)
				} else {
					pAddr, _, _err := conn.Get("/master/" + children[index - 1])
					if _err != nil {
						log.Error(_err)
						continue
					}
					master.PrimaryAddr = gfs.ServerAddress(pAddr)
					master.hasPrimary = true
					fmt.Printf("I( %s ) am backup of ( %s )\n", master.address, pAddr)
				}

				if index == len(children) - 1 {
					master.hasBackup = false
					fmt.Printf("I( %s ) don't have backup \n", master.address)
				} else {
					pAddr, _, _err := conn.Get("/master/" + children[index + 1])
					if _err != nil {
						log.Error(_err)
						continue
					}
					master.BackupAddr = gfs.ServerAddress(pAddr)
					master.hasBackup = true
					fmt.Printf("I( %s ) have backup ( %s )\n", master.address, pAddr)
				}

			case _err := <-errors:
				fmt.Printf("%+v\n", _err)
			}
		}
	}()
}

type PersistentBlock struct {
	NamespaceTree []serialTreeNode
	ChunkInfo     []serialFileInfo
}

// loadMeta loads metadata from disk
func (m *Master) loadMeta() error {
	var meta PersistentBlock
	var dec *gob.Decoder

	file, err := os.OpenFile(util.LogPathMasterMeta, os.O_RDONLY, util.LogPerm)
	if err != nil {goto LoadLog}
	defer file.Close()

	dec = gob.NewDecoder(file)
	err = dec.Decode(&meta)
	if err != nil {goto LoadLog}

	_ = m.nm.Deserialize(meta.NamespaceTree)
	_ = m.cm.Deserialize(meta.ChunkInfo)

LoadLog:
	m.nm.LoadLog()
	m.cm.LoadLog()

	return err
}

// storeMeta stores metadata to disk, that is when log meet checkpoint
func (m *Master) storeMeta() error {
	//filename := path.Join(m.serverRoot, MetaFileName)
	file, err := os.OpenFile(util.LogPathMasterMeta, os.O_WRONLY|os.O_CREATE, util.LogPerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var meta PersistentBlock

	meta.NamespaceTree = m.nm.Serialize()
	meta.ChunkInfo = m.cm.Serialize()

	log.Infof("Master : store metadata")
	enc := gob.NewEncoder(file)
	err = enc.Encode(meta)
	return err
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	if !m.dead {
		log.Warning(m.address, " Shutdown")
		m.dead = true
		//m.shutdown <- true
		close(m.shutdown)
		_ = m.l.Close()
	}

	//log.Info("store meta")
	//err := m.storeMeta()
	//if err != nil {
	//	log.Warning("error in store metadata: ", err)
	//}
}

// serverCheck checks all chunkServer according to last heartbeat time
// then removes all the information of the disconnected servers
func (m *Master) serverCheck() error {
	// detect dead servers
	addrs := m.csm.DetectDeadServers()
	for _, addr := range addrs {
		log.Printf("detect dead chunkServer ( %s ) \n", addr)
	}
	for _, v := range addrs {
		log.Warningf("remove server %v", v)
		handles, err := m.csm.RemoveServer(v)
		if err != nil {
			return err
		}
		err = m.cm.RemoveServerChunks(handles, v)
		if err != nil {
			return err
		}
	}

	// add replicas for need request
	handles := m.cm.GetNeedList()
	if handles != nil {
		log.Info("Master Need ", handles)
		m.cm.RLock()
		for i := 0; i < len(handles); i++ {
			ck := m.cm.chunk[handles[i]]

			if ck.expire.Before(time.Now()) {
				ck.Lock() // don't grant lease during copy
				err := m.reReplication(handles[i])
				log.Info(err)
				ck.Unlock()
			}
		}
		m.cm.RUnlock()
	}
	return nil
}

// reReplication performs re-replication, ck should be locked in top caller
// new lease will not be granted during copy
func (m *Master) reReplication(handle gfs.ChunkId) error {
	// chunk are locked, so master will not grant lease during copy time
	from, to, err := m.csm.ChooseReReplication(handle)
	if err != nil {
		return err
	}
	log.Warningf("allocate new chunk %v from %v to %v", handle, from, to)

	var cr gfs.CreateChunkReply
	err = util.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &cr)
	if err != nil {
		return err
	}

	var sr gfs.SendCopyReply
	err = util.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: handle, Address: to}, &sr)
	if err != nil {
		return err
	}

	_ = m.cm.RegisterReplica(handle, to, false)
	m.csm.AddChunk([]gfs.ServerAddress{to}, handle)
	return nil
}

// reReplication performs re-replication, ck should be locked in top caller
// new lease will not be granted during copy
func (m *Master) checkLoadBalance() error {
	// add replicas for need request
	handles := m.cm.GetNeedList()
	if handles != nil {
		log.Info("In checkLoadBalance, Master Need ", handles)
		m.cm.RLock()
		for i := 0; i < len(handles); i++ {
			ck := m.cm.chunk[handles[i]]

			if ck.expire.Before(time.Now()) {
				ck.Lock() // don't grant lease during copy
				err := m.reReplication(handles[i])
				log.Info(err)
				ck.Unlock()
			}
		}
		m.cm.RUnlock()
	}

	info, err := m.csm.chooseTransferFromTo()
	if err != nil {
		return err
	}
	// maintain the metadata in chunk manager and chunkServer manager, and tall 'from node' to delete chunk
	for _, transferInfo := range info {
		handle := transferInfo.Handle
		from := transferInfo.From
		to := transferInfo.To

		_ = m.cm.RegisterReplica(handle, to, false)
		err = m.cm.RemoveServerChunks([]gfs.ChunkId{handle}, from)
		m.csm.AddChunk([]gfs.ServerAddress{to}, handle)
		m.csm.AddGarbage(from, handle)
	}
	return nil
}

// RPCHeartbeat is called by chunkServer to let the master know that a chunk server is alive
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	var err error
	if m.hasBackup {
		err = util.Call(m.BackupAddr, "Master.RPCHeartbeat", args, &reply)
		if err == nil {
			go func() {
				err = m.getHeartbeat(args, reply)
			}()
		}
		return err
	} else {
		err = m.getHeartbeat(args, reply)
		return err
	}
}

func (m *Master) getHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	isFirst := m.csm.Heartbeat(args.Address, reply)

	// if is first heartbeat, let chunk server report itself
	if isFirst {
		fmt.Printf("I ( %s ) find new chunk server: %s\n", m.address, args.Address)
		var r gfs.ReportSelfReply
		err := util.Call(args.Address, "ChunkServer.RPCReportSelf", gfs.ReportSelfArg{}, &r)
		if err != nil {
			return err
		}

		for _, v := range r.Chunks {
			m.cm.RLock()
			ck, ok := m.cm.chunk[v.Id]
			if !ok {
				continue
			}
			version := ck.version
			fmt.Println("my version is ", version, ", chunkServer's chunk version is ", v.Version)
			m.cm.RUnlock()

			if v.Version == version {
				log.Infof("Master receive chunk %v from %v", v.Id, args.Address)
				_ = m.cm.RegisterReplica(v.Id, args.Address, true)
				m.csm.AddChunk([]gfs.ServerAddress{args.Address}, v.Id)
			} else {
				log.Infof("Master discard %v", v.Id)
			}
		}

		err = m.checkLoadBalance()
		if err != nil {
			return err
		}
	}
	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
// Master will communicate with all replicas holder to check version, if stale replica is detected, add it to garbage collection
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	var err error
	if m.hasBackup {
		err = util.Call(m.BackupAddr, "Master.RPCGetPrimaryAndSecondaries", args, &reply)
		if err == nil {
			go func() {
				_, staleServers, _ := m.cm.GetLeaseHolder(args.Handle)
				for _, v := range staleServers {
					m.csm.AddGarbage(v, args.Handle)
				}
			}()
		}
		return err
	} else {
		lease, staleServers, _err := m.cm.GetLeaseHolder(args.Handle)
		if _err != nil {
			return _err
		}
		for _, v := range staleServers {
			m.csm.AddGarbage(v, args.Handle)
		}

		reply.Primary = lease.Primary
		reply.Expire = lease.Expire
		reply.Secondaries = lease.Secondaries
		return nil
	}
}

// RPCGetReplicas is called by client to find all chunkServer that holds the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	servers, err := m.cm.GetReplicas(args.Handle)
	if err != nil {
		return err
	}
	for _, v := range servers {
		reply.Locations = append(reply.Locations, v)
	}
	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, reply *gfs.CreateFileReply) error {
	var err error
	if m.hasBackup {
		err = util.Call(m.BackupAddr, "Master.RPCCreateFile", args, &reply)
		if err == nil {
			go func() {
				err = m.nm.Create(args.Path)
			}()
		}
		return err
	} else {
		err = m.nm.Create(args.Path)
		return err
	}
}

// TODO 没有做垃圾清理的工作，即删除文件的内容始终在文件系统中，只是在命名空间中无法找到，从而无法拿到访问所需的Handle
// RPCDeleteFile is called by client to delete a file
func (m *Master) RPCDeleteFile(args gfs.DeleteFileArg, reply *gfs.DeleteFileReply) error {
	var err error
	if m.hasBackup {
		err = util.Call(m.BackupAddr, "Master.RPCDeleteFile", args, &reply)
		if err == nil {
			go func() {
				err = m.nm.Delete(args.Path)
			}()
		}
		return err
	} else {
		err = m.nm.Delete(args.Path)
		return err
	}
}

// RPCRenameFile is called by client to rename a file
func (m *Master) RPCRenameFile(args gfs.RenameFileArg, reply *gfs.RenameFileReply) error {
	var err error
	if m.hasBackup {
		err = util.Call(m.BackupAddr, "Master.RPCRenameFile", args, &reply)
		if err == nil {
			go func() {
				err = m.nm.Rename(args.Source, args.Target)
				err = m.cm.Rename(args.Source, args.Target)
			}()
		}
		return err
	} else {
		err = m.nm.Rename(args.Source, args.Target)
		if err != nil {return err}
		err = m.cm.Rename(args.Source, args.Target)
		return err
	}
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirReply) error {
	var err error
	if m.hasBackup {
		err = util.Call(m.BackupAddr, "Master.RPCMkdir", args, &reply)
		if err == nil {
			go func() {
				err = m.nm.Mkdir(args.Path)
			}()
		}
		return err
	} else {
		err = m.nm.Mkdir(args.Path)
		return err
	}
}

// RPCList is called by client to list all files in specific directory
func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	var err error
	reply.Files, err = m.nm.List(args.Path)
	return err
}

// RPCSetFileInfo is called by client to set file information
func (m *Master) RPCSetFileInfo(args gfs.SetFileInfoArg, reply *gfs.SetFileInfoReply) error {
	var err error
	if m.hasBackup {
		err = util.Call(m.BackupAddr, "Master.RPCSetFileInfo", args, &reply)
		if err == nil {
			go func() {
				err = m.nm.UpdateMeta(args.Path, args.Length, args.Chunks)
			}()
		}
		return err
	} else {
		err = m.nm.UpdateMeta(args.Path, args.Length, args.Chunks)
		return err
	}
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	ps, cwd, err := m.nm.lockParents(nil, args.Path, false)
	defer m.nm.unlockParents(nil, args.Path)
	if err != nil {
		return err
	}

	file, ok := cwd.children[ps[len(ps)-1]]
	if !ok {
		return fmt.Errorf("File %v does not exist\n", args.Path)
	}
	file.Lock()
	defer file.Unlock()

	reply.IsDir = file.isDir
	reply.Length = file.length
	reply.Chunks = file.chunks
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	ps, cwd, err := m.nm.lockParents(nil, args.Path, false)
	defer m.nm.unlockParents(nil, args.Path)
	if err != nil {
		return err
	}

	// append new chunks
	file, ok := cwd.children[ps[len(ps)-1]]
	if !ok {
		return fmt.Errorf("File %v does not exist\n", args.Path)
	}
	file.Lock()
	defer file.Unlock()

	if int(args.Index) == int(file.chunks) {
		fmt.Println("i come into IF ")
		file.chunks++

		addrs, _err := m.csm.ChooseServers(gfs.DefaultNumReplicas)
		if _err != nil {
			return _err
		}

		if m.hasBackup {
			var r gfs.BackupMasterCreateChunkReply
			err = util.Call(m.BackupAddr, "Master.RPCBackupMasterCreateChunk", gfs.BackupMasterCreateChunkArg{Path: args.Path, Addresses: addrs}, &r)
			if err == nil {
				reply.Handle = r.Handle
				go func() {
					_, addrs, err = m.cm.CreateChunk(args.Path, addrs)
					m.csm.AddChunk(addrs, reply.Handle)
				}()
			}
			return err
		} else {
			reply.Handle, addrs, err = m.cm.CreateChunk(args.Path, addrs)
			if err != nil {
				log.Warning("[ignored] An ignored error in RPCGetChunkHandle when create ", err, " in create chunk ", reply.Handle)
			}
			m.csm.AddChunk(addrs, reply.Handle)
		}
	} else {
		reply.Handle, err = m.cm.GetChunkId(args.Path, args.Index)
	}

	return err
}

func (m *Master) RPCBackupMasterCreateChunk(args gfs.BackupMasterCreateChunkArg, reply *gfs.BackupMasterCreateChunkReply) error {
	var addrs []gfs.ServerAddress
	var err error
	if m.hasBackup {
		var r gfs.BackupMasterCreateChunkReply
		err = util.Call(m.BackupAddr, "Master.RPCBackupMasterCreateChunk", gfs.BackupMasterCreateChunkArg{Path: args.Path, Addresses: args.Addresses}, &r)
		if err == nil {
			reply.Handle = r.Handle
			go func() {
				_, addrs, err = m.cm.CreateChunk(args.Path, args.Addresses)
				m.csm.AddChunk(addrs, reply.Handle)
			}()
		}
		return err
	} else {
		fmt.Println("i'm backup, i will create chunk on... ", args.Addresses)
		reply.Handle, addrs, err = m.cm.CreateChunk(args.Path, args.Addresses)
		if err != nil {
			log.Warning("[ignored] An ignored error in RPCGetChunkHandle when create ", err, " in create chunk ", reply.Handle)
		}
		m.csm.AddChunk(addrs, reply.Handle)
		return nil
	}
}



// -------------------------- for test -------------------------------

func (m *Master) ReportChunkServer() {
	var num = 0
	var serverNum = len(m.csm.servers)
	var serverAddr = make([]gfs.ServerAddress, serverNum)
	var serverChunkNum = make([]int, serverNum)
	var index = 0
	for addr, v := range m.csm.servers {
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

	// report the situation
	log.Printf("___Report___averageChunkNum: %d \n", averageChunkNum)
	for i := 0; i < serverNum; i++ {
		log.Printf("___Report___server ( %s ) has chunkNum: %d \n", serverAddr[i], serverChunkNum[i])
	}
}

func (m *Master) Mkdir(args gfs.MkdirArg, recursive bool) error {
	err := m.nm.newMkdir(args.Path, recursive)
	return err
}

func (m *Master) StoreMeta() error {
	return m.storeMeta()
}
