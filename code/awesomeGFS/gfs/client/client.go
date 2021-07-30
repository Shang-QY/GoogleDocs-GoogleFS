package client

import (
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"awesomeGFS/gfs"
	"awesomeGFS/gfs/chunkserver"
	"awesomeGFS/gfs/util"
	log "github.com/sirupsen/logrus"
)

// Client struct is the GFS client-side driver
type Client struct {
	// we don't emulate client collapse, so we don't need 'Shutdown' member
	address  gfs.ServerAddress
	master   gfs.ServerAddress
	shutdown chan struct{}
	leaseBuf *leaseBuffer
}

var client *Client

func HTTPCreate(w http.ResponseWriter, r *http.Request) {
	path, _ := ioutil.ReadAll(r.Body) //把 body 内容读入字符串 s
	log.Infof("[CREATE] path: %s", path)

	err := client.Create(gfs.Path(path))
	if err != nil {
		log.Error("cannot create : ", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}

func HTTPMkdir(w http.ResponseWriter, r *http.Request) {
	path, _ := ioutil.ReadAll(r.Body) //把 body 内容读入字符串 s
	log.Infof("[MKDIR] path: %s", path)

	err := client.Mkdir(gfs.Path(path))
	if err != nil {
		log.Error("cannot mkdir : ", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}

func HTTPDelete(w http.ResponseWriter, r *http.Request) {
	path, _ := ioutil.ReadAll(r.Body) //把 body 内容读入字符串 s
	log.Infof("[DELETE] path: %s", path)

	err := client.Delete(gfs.Path(path))
	if err != nil {
		log.Error("cannot delete : ", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}

func HTTPList(w http.ResponseWriter, r *http.Request) {
	path, _ := ioutil.ReadAll(r.Body) //把 body 内容读入字符串 s
	log.Infof("[LIST] path: %s", path)

	pathInfos, err := client.List(gfs.Path(path))
	if err != nil && err != io.EOF {
		log.Error("cannot list : ", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	data, _ := json.Marshal(pathInfos)
	_, _ = w.Write(data)
}

func HTTPGetFileInfo(w http.ResponseWriter, r *http.Request) {
	path, _ := ioutil.ReadAll(r.Body) //把 body 内容读入字符串 s
	log.Infof("[GET FILE INFO] path: %s", path)

	fileInfo, err := client.GetFileInfo(gfs.Path(path))
	if err != nil && err != io.EOF {
		log.Error("cannot getFileInfo : ", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	data, _ := json.Marshal(fileInfo)
	_, _ = w.Write(data)
}

func HTTPRename(w http.ResponseWriter, r *http.Request) {
	path, _ := ioutil.ReadAll(r.Body) //把 body 内容读入字符串 s
	log.Infof("[RENAME] path: %s", path)
	paths := strings.Split(string(path), ":")
	if len(paths) != 2 {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("rename path format is wrong!"))
		return
	}

	err := client.Rename(gfs.Path(paths[0]), gfs.Path(paths[1]))
	if err != nil {
		log.Error("cannot rename : ", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}

func HTTPRead(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	path := r.Form["path"][0]
	reg2 := r.Form["offset"][0]
	offset, _ := strconv.Atoi(reg2)
	reg3 := r.Form["size"][0]
	size, _ := strconv.Atoi(reg3)
	log.Infof("[READ] path: %s , offset: %v, size: %v", path, offset, size)

	buf := make([]byte, size)
	readBytes, err := client.Read(gfs.Path(path), gfs.Offset(offset), buf)
	if err != nil && err != io.EOF {
		log.Error("cannot read : ", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf[:readBytes])
}

func HTTPWrite(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	path := r.Form["path"][0]
	reg2 := r.Form["offset"][0]
	offset, _ := strconv.Atoi(reg2)
	data, _ := ioutil.ReadAll(r.Body)
	size := len(data)
	log.Infof("[WRITE] path: %s , offset: %v, size: %v", path, offset, size)

	err := client.Write(gfs.Path(path), gfs.Offset(offset), data)
	if err != nil {
		log.Error("cannot write : ", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}

func HTTPWriteAndCut(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	path := r.Form["path"][0]
	reg2 := r.Form["offset"][0]
	offset, _ := strconv.Atoi(reg2)
	data, _ := ioutil.ReadAll(r.Body)
	size := len(data)
	log.Infof("[WRITE AND CUT] path: %s , offset: %v, size: %v", path, offset, size)

	err := client.WriteAndCut(gfs.Path(path), gfs.Offset(offset), data)
	if err != nil {
		log.Error("cannot write : ", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}

func HTTPAppend(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	path := r.Form["path"][0]
	data, _ := ioutil.ReadAll(r.Body)
	size := len(data)
	log.Infof("[APPEND] path: %s , size: %v", path, size)

	_, err := client.Append(gfs.Path(path), data)
	if err != nil {
		log.Error("cannot append : ", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress) *Client {
	client = &Client{
		address:  addr,
		master:   masterAddr,
		shutdown: make(chan struct{}),
		leaseBuf: newLeaseBuffer(masterAddr, gfs.LeaseBufferTick),
	}

	go func() {
		connZookeeper := util.GetConnect([]string {util.ZkList})
		defer connZookeeper.Close()

		RegAndGetPrimaryMaster(connZookeeper, client)

		http.HandleFunc("/create", HTTPCreate)
		http.HandleFunc("/mkdir", HTTPMkdir)
		http.HandleFunc("/delete", HTTPDelete)
		http.HandleFunc("/list", HTTPList)
		http.HandleFunc("/getFileInfo", HTTPGetFileInfo)
		http.HandleFunc("/rename", HTTPRename)
		http.HandleFunc("/read", HTTPRead)
		http.HandleFunc("/write", HTTPWrite)
		http.HandleFunc("/writeAndCut", HTTPWriteAndCut)
		http.HandleFunc("/append", HTTPAppend)
		err := http.ListenAndServe(string(client.address), nil)
		if err != nil {
			log.Fatal("ListenAndServe: ", err)
		}

		log.Infof("Client is running now. addr = %v", addr)
	}()

	return client
}

func RegAndGetPrimaryMaster(conn *zk.Conn, client *Client) {
	clientFile, err := util.CreateClientFile(conn, client.address)
	fmt.Printf("I( %s ) create clientFile '%s' \n", client.address, clientFile)
	if err != nil {
		fmt.Printf("%+v\n", err)
		return
	}

	go func() {
		for {
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
				client.master = gfs.ServerAddress(pAddr)
				fmt.Printf("I( %s ) find primary master '%s' \n", client.address, pAddr)

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
					// abnormal situation need to handle, otherwise thread can't exit the loop
					if event.Type.String() == "EventNotWatching" {
						fmt.Println("[EventNotWatching]")
						break
					}
				}
			}
		}
	}()
}







// NewClient returns a new gfs client.
func NewClient(master gfs.ServerAddress) *Client {
	return &Client{
		master:   master,
		shutdown:   make(chan struct{}),
		leaseBuf: newLeaseBuffer(master, gfs.LeaseBufferTick),
	}
}

// Create is a client API, creates a file
func (c *Client) Create(path gfs.Path) error {
	var reply gfs.CreateFileReply
	err := util.Call(c.master, "Master.RPCCreateFile", gfs.CreateFileArg{Path: path}, &reply)
	if err != nil {
		return err
	}
	return nil
}

// Delete is a client API, deletes a file
func (c *Client) Delete(path gfs.Path) error {
	var reply gfs.DeleteFileReply
	err := util.Call(c.master, "Master.RPCDeleteFile", gfs.DeleteFileArg{Path: path}, &reply)
	if err != nil {
		return err
	}
	return nil
}

// Rename is a client API, deletes a file
func (c *Client) Rename(source gfs.Path, target gfs.Path) error {
	var reply gfs.RenameFileReply
	err := util.Call(c.master, "Master.RPCRenameFile", gfs.RenameFileArg{Source: source, Target: target}, &reply)

	if err != nil {
		return err
	}

	return nil
}

// Mkdir is a client API, makes a directory
func (c *Client) Mkdir(path gfs.Path) error {
	var reply gfs.MkdirReply
	err := util.Call(c.master, "Master.RPCMkdir", gfs.MkdirArg{Path: path}, &reply)
	if err != nil {
		return err
	}
	return nil
}

// List is a client API, lists all files in specific directory
func (c *Client) List(path gfs.Path) ([]gfs.PathInfo, error) {
	var reply gfs.ListReply
	err := util.Call(c.master, "Master.RPCList", gfs.ListArg{Path: path}, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Files, nil
}

func (c *Client) GetFileInfo(path gfs.Path) (gfs.GetFileInfoReply, error) {
	var f gfs.GetFileInfoReply
	err := util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return f, err
	}
	return f, nil
}

// Read is a client API, read file at specific offset
// it reads up to len(data) bytes form the File. it return the number of bytes and an error.
// the error is set to io.EOF if stream meets the end of file
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (n int, err error) {
	conn := util.GetConnect([]string {util.ZkList})
	defer conn.Close()

	err = util.GetReadLock(conn, path)
	if err != nil {
		return -1, err
	}

	var f gfs.GetFileInfoReply
	err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return -1, err
	}

	log.Printf("[Read] file size %v", f.Length)
	if int64(offset) > f.Length {
		return -1, fmt.Errorf("read offset exceeds file size")
	}

	var toRead int64
	if int64(offset) + int64(len(data)) > f.Length {
		toRead = f.Length - int64(offset)
	} else {
		toRead = int64(len(data))
	}

	pos := 0
	for pos < int(toRead) {
		index := gfs.ChunkIndex(offset / gfs.MaxChunkSize)
		chunkOffset := offset % gfs.MaxChunkSize

		if int64(index) >= f.Chunks {
			err = gfs.Error{Code: gfs.ReadEOF, Err: "EOF over chunks"}
			break
		}

		var handle gfs.ChunkId
		handle, err = c.GetChunkHandle(path, index)
		if err != nil {
			return
		}

		log.Info("get chunkId ", handle, " for path ", path)

		var n int
		//wait := time.NewTimer(gfs.ClientTryTimeout)
		//loop:
		for {
			//select {
			//case <-wait.C:
			//    err = gfs.Error{gfs.Timeout, "Read Timeout"}
			//    break loop
			//default:
			//}
			n, err = c.ReadChunk(handle, chunkOffset, data[pos:toRead])
			if err == nil || err.(gfs.Error).Code == gfs.ReadEOF {
				break
			}
			log.Warning("Read ", handle, " connection error, try again: ", err)
		}

		offset += gfs.Offset(n)
		pos += n
		if err != nil {
			break
		}
	}

	if err != nil && err.(gfs.Error).Code == gfs.ReadEOF {
		//log.Errorf("err != nil && err.(gfs.Error).Code == gfs.ReadEOF\n")
		return pos, io.EOF
	} else {
		return pos, err
	}
}

// Write is a client API. write data to file at specific offset
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	conn := util.GetConnect([]string {util.ZkList})
	defer conn.Close()

	err := util.GetWriteLock(conn, path)
	if err != nil {
		return err
	}

	var f gfs.GetFileInfoReply
	err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return err
	}

	log.Printf("[Write] file original size %v", f.Length)
	if int64(offset) > f.Length {
		return fmt.Errorf("write offset exceeds file size")
	}

	begin := 0
	for {
		index := gfs.ChunkIndex(offset / gfs.MaxChunkSize)
		chunkOffset := offset % gfs.MaxChunkSize

		handle, err := c.GetChunkHandle(path, index)
		if err != nil {
			return err
		}

		writeMax := int(gfs.MaxChunkSize - chunkOffset)
		var writeLen int
		if begin+writeMax > len(data) {
			writeLen = len(data) - begin
		} else {
			writeLen = writeMax
		}

		//wait := time.NewTimer(gfs.ClientTryTimeout)
		//loop:
		for {
			//select {
			//case <-wait.C:
			//    err = fmt.Errorf("Write Timeout")
			//    break loop
			//default:
			//}
			err = c.WriteChunk(handle, chunkOffset, data[begin:begin+writeLen])
			if err == nil {
				break
			}
			log.Warning("Write ", handle, "  connection error, try again ", err)
		}
		//if err != nil {
		//	return err
		//}

		offset += gfs.Offset(writeLen)
		begin += writeLen

		if begin == len(data) {
			break
		}
	}

	newSize := int64(offset)
	log.Printf("[Write] fize final size %v", newSize)
	if  newSize > f.Length {
		log.Infof("SetFileInfo: %s, length: %v, chunks: %v", path, newSize, newSize / gfs.MaxChunkSize + 1)
		err = util.Call(c.master, "Master.RPCSetFileInfo", gfs.SetFileInfoArg{Path: path, Length: newSize, Chunks: newSize / gfs.MaxChunkSize + 1}, &f)
		if err != nil {
			return err
		}
	}

	return nil
}

// WriteAndCut is a client API. write data to file at specific offset
func (c *Client) WriteAndCut(path gfs.Path, offset gfs.Offset, data []byte) error {
	conn := util.GetConnect([]string {util.ZkList})
	defer conn.Close()

	err := util.GetWriteLock(conn, path)
	if err != nil {
		return err
	}

	var f gfs.GetFileInfoReply
	err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return err
	}

	if int64(offset) > f.Length {
		return fmt.Errorf("write offset exceeds file size")
	}

	begin := 0
	for {
		index := gfs.ChunkIndex(offset / gfs.MaxChunkSize)
		chunkOffset := offset % gfs.MaxChunkSize

		handle, err := c.GetChunkHandle(path, index)
		if err != nil {
			return err
		}

		writeMax := int(gfs.MaxChunkSize - chunkOffset)
		var writeLen int
		if begin+writeMax > len(data) {
			writeLen = len(data) - begin
		} else {
			writeLen = writeMax
		}

		//wait := time.NewTimer(gfs.ClientTryTimeout)
		//loop:
		for {
			//select {
			//case <-wait.C:
			//    err = fmt.Errorf("Write Timeout")
			//    break loop
			//default:
			//}
			err = c.WriteChunk(handle, chunkOffset, data[begin:begin+writeLen])
			if err == nil {
				break
			}
			log.Warning("Write ", handle, "  connection error, try again ", err)
		}
		//if err != nil {
		//	return err
		//}

		offset += gfs.Offset(writeLen)
		begin += writeLen

		if begin == len(data) {
			break
		}
	}

	newSize := int64(offset)
	log.Infof("SetFileInfo: %s, length: %v, chunks: %v", path, newSize, newSize / gfs.MaxChunkSize + 1)
	err = util.Call(c.master, "Master.RPCSetFileInfo", gfs.SetFileInfoArg{Path: path, Length: newSize, Chunks: newSize / gfs.MaxChunkSize + 1}, &f)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	var f gfs.GetFileInfoReply
	err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return
	}

	err = c.Write(path, gfs.Offset(f.Length), data)
	return gfs.Offset(f.Length + int64(len(data))), err
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkId, error) {
	var reply gfs.GetChunkHandleReply
	err := util.Call(c.master, "Master.RPCGetChunkHandle", gfs.GetChunkHandleArg{Path: path, Index: index}, &reply)
	if err != nil {
		return 0, err
	}
	return reply.Handle, nil
}

// ReadChunk read data from the chunk at specific offset.
// <code>len(data)+offset</data> should be within chunk size.
func (c *Client) ReadChunk(handle gfs.ChunkId, offset gfs.Offset, data []byte) (int, error) {
	var readLen int

	if gfs.MaxChunkSize-offset > gfs.Offset(len(data)) {
		readLen = len(data)
	} else {
		readLen = int(gfs.MaxChunkSize - offset)
	}

	var l gfs.GetReplicasReply
	err := util.Call(c.master, "Master.RPCGetReplicas", gfs.GetReplicasArg{Handle: handle}, &l)
	if err != nil {
		return 0, gfs.Error{Code: gfs.UnknownError, Err: err.Error()}
	}
	loc := l.Locations[rand.Intn(len(l.Locations))]
	if len(l.Locations) == 0 {
		return 0, gfs.Error{Code: gfs.UnknownError, Err: "no replica"}
	}

	var r gfs.ReadChunkReply
	r.Data = data
	err = util.Call(loc, "ChunkServer.RPCReadChunk", gfs.ReadChunkArg{Handle: handle, Offset: offset, Length: readLen}, &r)
	if err != nil {
		//log.Errorf("util.Call(loc, \"ChunkServer.RPCReadChunk\" \n")
		return 0, gfs.Error{Code: gfs.UnknownError, Err: err.Error()}
	}
	if r.ErrorCode == gfs.ReadEOF {
		return r.Length, gfs.Error{Code: gfs.ReadEOF, Err: "read EOF"}
	}
	return r.Length, nil
}

// WriteChunk writes data to the chunk at specific offset.
// <code>len(data)+offset</data> should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkId, offset gfs.Offset, data []byte) error {
	if len(data)+int(offset) > gfs.MaxChunkSize {
		return fmt.Errorf("len(data)+offset = %v > max chunk size %v", len(data)+int(offset), gfs.MaxChunkSize)
	}

	l, err := c.leaseBuf.Get(handle)
	if err != nil {
		return err
	}

	dataID := chunkserver.NewDataID(handle)
	chain := append(l.Secondaries, l.Primary)

	var d gfs.ForwardDataReply
	err = util.Call(chain[0], "ChunkServer.RPCForwardData", gfs.ForwardDataArg{DataID: dataID, Data: data, ChainOrder: chain[1:]}, &d)
	if err != nil {
		return err
	}

	wcargs := gfs.WriteChunkArg{DataID: dataID, Offset: offset, Secondaries: l.Secondaries}
	err = util.Call(l.Primary, "ChunkServer.RPCWriteChunk", wcargs, &gfs.WriteChunkReply{})
	return err
}



//ReadForTest is used to test the read write lock
func (c *Client) ReadForTest(path gfs.Path, duration time.Duration) error {
	conn := util.GetConnect([]string {util.ZkList})
	defer conn.Close()

	err := util.GetReadLock(conn, path)
	if err != nil {
		return err
	}
	time.Sleep(duration)
	return err
}

//WriteForTest is used to test the read write lock
func (c *Client) WriteForTest(path gfs.Path, duration time.Duration) error {
	conn := util.GetConnect([]string{util.ZkList})
	defer conn.Close()

	err := util.GetWriteLock(conn, path)
	if err != nil {
		return err
	}
	time.Sleep(duration)
	return err
}
