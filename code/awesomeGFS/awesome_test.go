package main

import (
	"awesomeGFS/gfs"
	"awesomeGFS/gfs/chunkserver"
	"awesomeGFS/gfs/client"
	"awesomeGFS/gfs/master"
	"awesomeGFS/gfs/util"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"reflect"
	"strings"

	//"math/rand"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

var (
	m     	*master.Master
	cs    	[]*chunkserver.ChunkServer
	c     	*client.Client
	client1 *client.Client
	csAdd 	[]gfs.ServerAddress
	root  	string // root of tmp file path
)

const (
	mAdd  = ":7777"
	csNum = 5
	N     = 100
)

func errorAll(ch chan error, n int, t *testing.T) {
	for i := 0; i < n; i++ {
		if err := <-ch; err != nil {
			t.Error(err)
		}
	}
}

/*
 *  TEST SUITE 1 - Basic File Operation
 */
func TestUtil(t *testing.T) {
	println("-------split file path--------")
	a := "/usr/local/cd/af"
	b := "/usr/local/ab/bfa"

	sPath, _ := util.SplitFilePath(gfs.Path(a))
	tPath, _ := util.SplitFilePath(gfs.Path(b))
	println(sPath, tPath)
	commonPath := util.FindCommonPath(string(sPath), string(tPath))
	sDir := len(sPath) > commonPath + 1
	tDir := len(tPath) > commonPath + 1
	println(commonPath)
	println(sPath[:commonPath])
	println(tPath[commonPath:])
	println(sDir, tDir)

	println("-------gfs contain function--------")
	books := []gfs.PathInfo{gfs.PathInfo{Name: "a"}, gfs.PathInfo{Name: "b"}, gfs.PathInfo{Name: "baar"}}
	search := "bar"

	r := util.Contains(books,search)
	if r == -1 {
		fmt.Println("no")
	} else{
		fmt.Println("yes")
	}
}

func TestCreateFile(t *testing.T) {
	var err error
	err = m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	log.Errorf("err: %s", err)
	if err == nil {
		t.Error("the same file has been created twice")
	}

	err = m.RPCCreateFile(gfs.CreateFileArg{Path: "/foo/test1.txt"}, &gfs.CreateFileReply{})
	log.Errorf("err: %s", err)
	if err == nil {
		t.Error("the same file has been created twice")
	}

	err = m.RPCCreateFile(gfs.CreateFileArg{Path: "/foo"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m.RPCCreateFile(gfs.CreateFileArg{Path: "/foo/test1.txt"}, &gfs.CreateFileReply{})
	log.Errorf("err: %s", err)
	if err == nil {
		t.Error("the same file has been created twice")
	}
}

func TestMkdirRecursive(t *testing.T) {
	var err error
	var list gfs.ListReply
	sPath := "/usr/local/zyw"

	// check source dir
	err = m.RPCList(gfs.ListArg{Path: gfs.Path(sPath)}, &list)
	if err != nil {
		println(err)
	} else {
		t.Errorf("before mkdir, found dir %s", sPath)
	}

	// recursively mkdir
	err = m.Mkdir(gfs.MkdirArg{Path: gfs.Path(sPath)}, true)
	if err != nil {
		t.Error(err)
	}

	// check source dir
	err = m.RPCList(gfs.ListArg{Path: gfs.Path(sPath)}, &list)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("dir = %s, files: ", sPath)
	for _, v := range list.Files {
		fmt.Printf("%s ", v.Name)
	}
	fmt.Printf(", total = %d files\n", len(list.Files))
}

func renameTest(sPath, tPath string, t *testing.T) {
	var err error
	var list gfs.ListReply
	var exist bool
	sFile, tFile := "test1.txt", "text2.txt"

	// create source dir && source file
	err = m.Mkdir(gfs.MkdirArg{Path: gfs.Path(sPath)}, true)
	if err != nil {t.Error(err)}
	err = m.RPCCreateFile(gfs.CreateFileArg{Path: gfs.Path(sPath + "/" + sFile)}, &gfs.CreateFileReply{})
	if err != nil {t.Error(err)}
	// create target dir
	err = m.Mkdir(gfs.MkdirArg{Path: gfs.Path(tPath)}, true)
	if err != nil {t.Error(err)}

	// check source dir
	err = m.RPCList(gfs.ListArg{Path: gfs.Path(sPath)}, &list)
	if err != nil {t.Error(err)}
	exist = false
	for _, v := range list.Files {
		if v.Name == sFile {exist = true}
		fmt.Printf("%s ", v.Name)
	}
	fmt.Printf("total = %d files\n", len(list.Files))
	if !exist {
		t.Errorf("create %s/%s, but found no %s under %s!", sPath, sFile, sFile, sPath)
	}
	// check target dir
	err = m.RPCList(gfs.ListArg{Path: gfs.Path(tPath)}, &list)
	if err != nil {t.Error(err)}
	exist = false
	for _, v := range list.Files {
		if v.Name == tFile {exist = true}
		fmt.Printf("%s ", v.Name)
	}
	fmt.Printf("total = %d files\n", len(list.Files))
	if exist {
		t.Errorf("before rename, found %s under %s!", tFile, tPath)
	}

	// rename source to target
	err = m.RPCRenameFile(gfs.RenameFileArg{Source: gfs.Path(sPath + "/" + sFile), Target: gfs.Path(tPath + "/" + tFile)}, &gfs.RenameFileReply{})
	if err != nil {t.Error(err)}

	// check source dir again
	err = m.RPCList(gfs.ListArg{Path: gfs.Path(sPath)}, &list)
	if err != nil {t.Error(err)}
	exist = false
	for _, v := range list.Files {
		if v.Name == sFile {exist = true}
		fmt.Printf("%s ", v.Name)
	}
	fmt.Printf("total = %d files\n", len(list.Files))
	if exist {
		t.Errorf("after rename, found %s under %s!", sFile, sPath)
	}

	// check target dir again
	err = m.RPCList(gfs.ListArg{Path: gfs.Path(tPath)}, &list)
	if err != nil {t.Error(err)}
	exist = false
	for _, v := range list.Files {
		if v.Name == tFile {exist = true}
		fmt.Printf("%s ", v.Name)
	}
	fmt.Printf("total = %d files\n", len(list.Files))
	if !exist {
		t.Errorf("after rename, found no %s under %s!", tFile, tPath)
	}
}

func TestRenameFile(t *testing.T) {
	println("---------rename case 1---------")
	renameTest("/source1", "/target1", t)
	println("---------rename case 2---------")
	renameTest("/usr/source2", "/usr/target2", t)
	println("---------rename case 3---------")
	renameTest("/source3", "/source3/target3", t)
	println("---------rename case 4---------")
	renameTest("/target4/source4", "/target4", t)
	println("---------rename case 5---------")
	renameTest("/source5", "/source5", t)
	//println("---------rename case 6---------")
	//renameTest("/", "/", t)

}

func TestMkdirDeleteList(t *testing.T) {
	var errors error
	ch := make(chan error, 9)
	ch <- m.RPCMkdir(gfs.MkdirArg{Path: "/dir1"}, &gfs.MkdirReply{})
	ch <- m.RPCMkdir(gfs.MkdirArg{Path: "/dir2"}, &gfs.MkdirReply{})
	<- ch
	<- ch

	ch <- m.RPCCreateFile(gfs.CreateFileArg{Path: "/file1.txt"}, &gfs.CreateFileReply{})
	ch <- m.RPCCreateFile(gfs.CreateFileArg{Path: "/file2.txt"}, &gfs.CreateFileReply{})
	ch <- m.RPCCreateFile(gfs.CreateFileArg{Path: "/dir1/file3.txt"}, &gfs.CreateFileReply{})
	ch <- m.RPCCreateFile(gfs.CreateFileArg{Path: "/dir1/file4.txt"}, &gfs.CreateFileReply{})
	ch <- m.RPCCreateFile(gfs.CreateFileArg{Path: "/dir2/file5.txt"}, &gfs.CreateFileReply{})

	err := m.RPCCreateFile(gfs.CreateFileArg{Path: "/dir2/file5.txt"}, &gfs.CreateFileReply{})
	if err == nil {
		t.Error("the same file has been created twice")
	}

	err = m.RPCMkdir(gfs.MkdirArg{Path: "/dir1"}, &gfs.MkdirReply{})
	if err == nil {
		t.Error("the same directory has been created twice")
	}

	var l gfs.ListReply
	var r gfs.DeleteFileReply

	//对非根目录下的 List 和 Delete 测试
	todelete := make(map[string]bool)
	todelete["file3.txt"] = true
	todelete["file4.txt"] = true
	ch <- m.RPCList(gfs.ListArg{Path: "/dir1"}, &l)
	for _, v := range l.Files {
		delete(todelete, v.Name)	//检查List能够覆盖所有子文件

		errors = m.RPCDeleteFile(gfs.DeleteFileArg{Path: gfs.Path("/dir1/" + v.Name)}, &r)	//检查Delete正确路径，返回无错误
		if errors != nil {
			t.Error("the delete return value wrong")
		}
	}
	if len(todelete) != 0 {
		t.Error("error in list root path, get", l.Files)
	}
	errors = m.RPCList(gfs.ListArg{Path: "/dir1"}, &l)		//再次List，确认删除操作成功
	for _, v := range l.Files {
		log.Printf("get %s\n", v.Name)
		if v.Name == "dir1" || v.Name == "dir2" ||v.Name == "file1.txt" ||v.Name == "file2.txt" {
			t.Error("the delete has no effect")
		}
	}

	//对根目录下的 List 和 Delete 测试
	todelete["dir1"] = true
	todelete["dir2"] = true
	todelete["file1.txt"] = true
	todelete["file2.txt"] = true
	ch <- m.RPCList(gfs.ListArg{Path: "/"}, &l)
	for _, v := range l.Files {
		delete(todelete, v.Name)	//检查List能够覆盖所有子文件

		errors = m.RPCDeleteFile(gfs.DeleteFileArg{Path: gfs.Path("/" + v.Name)}, &r)	//检查Delete正确路径，返回无错误
		if errors != nil {
			t.Error("the delete return value wrong")
		}
	}
	if len(todelete) != 0 {
		t.Error("error in list root path, get", l.Files)
	}
	errors = m.RPCList(gfs.ListArg{Path: "/"}, &l)		//再次List，确认删除操作成功
	for _, v := range l.Files {
		log.Printf("get %s\n", v.Name)
		if v.Name == "dir1" || v.Name == "dir2" ||v.Name == "file1.txt" ||v.Name == "file2.txt" {
			t.Error("the delete has no effect")
		}
	}

	errorAll(ch, 7, t)
}

func TestRPCGetChunkHandle(t *testing.T) {
	err := m.RPCCreateFile(gfs.CreateFileArg{Path: "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}

	var r1, r2 gfs.GetChunkHandleReply
	_path := gfs.Path("/test1.txt")
	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: _path}, &r1)
	if err != nil {
		t.Error(err)
	}
	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: _path}, &r2)
	if err != nil {
		t.Error(err)
	}
	if r1.Handle != r2.Handle {
		t.Errorf("got different handle: %d and %d", r1.Handle, r2.Handle)
	}

	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: _path, Index: 2}, &r2)
	if err == nil {
		t.Error("discontinuous chunk should not be created")
	}
}

func TestWriteChunk(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestWriteChunk.txt")
	ch := make(chan error, N+2)
	ch <- m.RPCCreateFile(gfs.CreateFileArg{Path: p}, &gfs.CreateFileReply{})
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p}, &r1)
	for i := 0; i < N; i++ {
		go func(x int) {
			ch <- c.WriteChunk(r1.Handle, gfs.Offset(x*2), []byte(fmt.Sprintf("%2d", x)))
		}(i)
	}
	errorAll(ch, N+2, t)
}

func TestReadChunk(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestWriteChunk.txt")
	ch := make(chan error, N+1)
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p}, &r1)
	for i := 0; i < N; i++ {
		go func(x int) {
			buf := make([]byte, 2)
			n, err := c.ReadChunk(r1.Handle, gfs.Offset(x*2), buf)
			ch <- err
			expected := []byte(fmt.Sprintf("%2d", x))
			if n != 2 {
				t.Error("should read exactly 2 bytes but", n, "instead")
			} else if !reflect.DeepEqual(expected, buf) {
				t.Error("expected (", expected, ") != buf (", buf, ")")
			}
		}(i)
	}
	errorAll(ch, N+1, t)
}

// check if the content of replicas are the same, returns the number of replicas
func checkReplicas(handle gfs.ChunkId, length int, t *testing.T) int {
	var data [][]byte

	// get replicas location from master
	var l gfs.GetReplicasReply
	err := m.RPCGetReplicas(gfs.GetReplicasArg{Handle: handle}, &l)
	if err != nil {
		t.Error(err)
	}

	// read
	args := gfs.ReadChunkArg{Handle: handle, Length: length}
	for _, addr := range l.Locations {
		var r gfs.ReadChunkReply
		err := util.Call(addr, "ChunkServer.RPCReadChunk", args, &r)
		if err == nil {
			data = append(data, r.Data)
			//fmt.Println("find in ", addr)
		}
	}

	// check equality
	for i := 1; i < len(data); i++ {
		if !reflect.DeepEqual(data[0], data[i]) {
			t.Error("replicas are different. ", data[0], "vs", data[i])
		}
	}

	return len(data)
}

func TestReplicaEquality(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	var data [][]byte
	p := gfs.Path("/TestWriteChunk.txt")
	_ = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p}, &r1)

	n := checkReplicas(r1.Handle, N*2, t)
	if n != gfs.DefaultNumReplicas {
		t.Error("expect", gfs.DefaultNumReplicas, "replicas, got only", len(data))
	}
}

/*
 *  TEST SUITE 2 - Client API
 */

// if the append would cause the chunk to exceed the maximum size
// this chunk should be pad and the data should be appended to the next chunk
func TestPadOver(t *testing.T) {
	p := gfs.Path("/appendover.txt")

	ch := make(chan error, 6)
	ch <- c.Create(p)

	bound := gfs.MaxAppendSize - 1
	buf := make([]byte, bound)
	for i := 0; i < bound; i++ {
		buf[i] = byte(i%26 + 'a')
	}

	for i := 0; i < 4; i++ {
		_, err := c.Append(p, buf)
		ch <- err
	}

	buf = buf[:5]
	// an append cause pad, and client should retry to next chunk
	offset, err := c.Append(p, buf)
	ch <- err
	if offset != gfs.Offset(bound * 4 + len(buf)) { // i.e. 0 at next chunk
		t.Error("data should be appended to the beginning of next chunk")
	}

	errorAll(ch, 6, t)
}

// big data that invokes several chunks
func TestWriteReadBigData(t *testing.T) {
	p := gfs.Path("/bigData.txt")

	ch := make(chan error, 3)
	ch <- c.Create(p)

	size := gfs.MaxChunkSize * 3
	t.Logf("size: %v", size)
	expected := make([]byte, size)
	for i := 0; i < size; i++ {
		expected[i] = byte(i%26 + 'a')
	}

	// write large data
	ch <- c.Write(p, 0, expected)

	// read
	buf := make([]byte, size)
	n, err := c.Read(p, 0, buf)
	t.Logf("read n: %v", n)
	ch <- err

	if n != size {
		t.Error("read counter is wrong")
	}
	if !reflect.DeepEqual(expected, buf) {
		t.Error("read wrong data")
	}

	// test read at EOF
	n, err = c.Read(p, gfs.MaxChunkSize/2+gfs.Offset(size), buf)
	if err == nil {
		t.Error("an error should be returned if read at EOF")
	}

	// test append offset
	//var offset gfs.Offset
	//buf = buf[:gfs.MaxAppendSize-1]
	//offset, err = c.Append(p, buf)
	//if offset != gfs.MaxChunkSize/2+gfs.Offset(size) {
	//	t.Error("append in wrong offset")
	//}
	//ch <- err

	errorAll(ch, 3, t)
}

// TestWriteReadLock is used to test the correctness of concurrency control implemented by Zookeeper.
// the result show below:
// awesome_test.go:408: test start: 		2021-07-12 10:59:17.386257 +0800 CST m=+0.304993204
// awesome_test.go:412: first write end: 	2021-07-12 10:59:19.425276 +0800 CST m=+2.344035573
// awesome_test.go:417: second write end: 	2021-07-12 10:59:24.441531 +0800 CST m=+7.360346829
// awesome_test.go:427: second read end: 	2021-07-12 10:59:26.450982 +0800 CST m=+9.369820562
// awesome_test.go:422: first read end: 	2021-07-12 10:59:29.457872 +0800 CST m=+12.376744804
// awesome_test.go:432: third write end: 	2021-07-12 10:59:31.4747 +0800 CST m=+14.393595999
// this read write lock is fair. It doesn't prefer reader or writer. And achieve that: Read and write operations are
// mutually exclusive and read operations can execute concurrently.
func TestWriteReadLock(t *testing.T) {
	p := gfs.Path("/readWriteLock.txt")
	err := c.Create(p)
	if err != nil {
		t.Error(err)
	}

	ch := make(chan error, 5)

	now := time.Now()
	t.Logf("test start: %v", now)

	go func() {
		ch <- c.WriteForTest(p, time.Second * 2)
		t.Logf("first write end: %v", time.Now())
	}()
	time.Sleep(time.Second)
	go func() {
		ch <- c.WriteForTest(p, time.Second * 5)
		t.Logf("second write end: %v", time.Now())
	}()
	time.Sleep(time.Second)
	go func() {
		ch <- c.ReadForTest(p, time.Second * 5)
		t.Logf("first read end: %v", time.Now())
	}()
	time.Sleep(time.Second)
	go func() {
		ch <- c.ReadForTest(p, time.Second * 2)
		t.Logf("second read end: %v", time.Now())
	}()
	time.Sleep(time.Second)
	go func() {
		ch <- c.WriteForTest(p, time.Second * 2)
		t.Logf("third write end: %v", time.Now())
	}()

	errorAll(ch, 5, t)
}

// TestMasterPrimaryBackup is used to test the correctness of "master slave backup in chain form",
// which need everyone in chain know their master and their backup at any time. We implement this
// work in Zookeeper. The test below is the situation that ---
// there are 4 masters in chain, and their dead order is: master 2 dead, master 1 dead, master 4 dead, and then master 3 dead.
// Three possibilities of chain collapse are constructed:
// (1) the head dead
// (2) the middle node dead
// (3) the tail dead
// watch the log in test result, we can see correct reflect of live nodes in every possibilities.
func TestListenMasterAndBackup(t *testing.T) {
	ch := make(chan error, 5)

	master1 := master.New(":1300", "")
	master2 := master.New(":1301", "")
	master3 := master.New(":1302", "")
	master4 := master.New(":1303", "")

	// dead order: master 2 dead, master 1 dead, master 4 dead, and then master 3 dead.
	go func() {
		connZookeeper := util.GetConnect([]string {util.ZkList})
		defer connZookeeper.Close()
		master.ListenPrimary(connZookeeper, master1)
		time.Sleep(time.Second * 7)
		ch <- nil
	}()
	time.Sleep(time.Second)
	go func() {
		connZookeeper := util.GetConnect([]string {util.ZkList})
		defer connZookeeper.Close()
		master.ListenPrimary(connZookeeper, master2)
		time.Sleep(time.Second * 4)
		ch <- nil
	}()
	time.Sleep(time.Second)
	go func() {
		connZookeeper := util.GetConnect([]string {util.ZkList})
		defer connZookeeper.Close()
		master.ListenPrimary(connZookeeper, master3)
		time.Sleep(time.Second * 8)
		ch <- nil
	}()
	time.Sleep(time.Second)
	go func() {
		connZookeeper := util.GetConnect([]string {util.ZkList})
		defer connZookeeper.Close()
		master.ListenPrimary(connZookeeper, master4)
		time.Sleep(time.Second * 5)
		ch <- nil
	}()

	errorAll(ch, 4, t)
}

// TestMasterBackupWork is used to test the correctness of "chain master slave": two real master nodes form "chain
// master slave backup", and when the master collapse, the slave works immediately, which means gfs clients can find
// slave master node and slave master node hold lasted metadata.
func TestMasterBackupWork(t *testing.T) {
	ch := make(chan error, 5)
	// create a client, which connect to Zookeeper and get the primary master all time.
	client1 = client.NewAndServe(":1314", ":7777")
	t.Logf("client 1 new")

	time.Sleep(time.Second)
	go func() {
		connZookeeper := util.GetConnect([]string {util.ZkList})
		defer connZookeeper.Close()
		// create a backup master, which dead 10 second later then primary
		mt1 := master.NewAndServe(":1300", "")
		time.Sleep(time.Second * 10)
		mt1.Shutdown()
		ch <- nil
	}()

	// main thread sleep 2 second to wait 2 master nodes construct successfully
	time.Sleep(time.Second * 2)

	// client request master to CREATE, change primary master's metadata, and AFTER one second,
	// the primary master dead. And then, the client will get the new master address(origin
	// backup), use LIST to see if new master is correct and available
	var files []gfs.PathInfo
	err := client1.Create("/dance")
	// note: this test need to run with clear log files, otherwise will get "file dance already exists in dir"
	if err != nil {
		t.Error(err)
	}
	files, err = client1.List("/")
	var find bool
	for _, v := range files {
		fmt.Println(v.Name)
		if strings.Compare(v.Name, "dance") == 0 {
			find = true
		}
	}
	if find == false {
		t.Errorf("don't find ( %s ), primary master works wrong", "dance")
	}

	time.Sleep(time.Second)
	m.Shutdown()
	time.Sleep(time.Second)

	files, err = client1.List("/")
	if err != nil {
		t.Error(err)
	}
	find = false
	for _, v := range files {
		fmt.Println(v.Name)
		if strings.Compare(v.Name, "dance") == 0 {
			find = true
		}
	}
	if find == false {
		t.Errorf("don't find dance")
	}

	errorAll(ch, 1, t)
}

// TestChunkServerWatchAndOtherMetaInBackupMaster is used to test the correctness of chunkServer's watching at primary
// master address and backup master can maintain all the metadata of 'chunk manager' and 'chunk server manager' correctly.
// So when the primary master is dead, the backup master can play role like "tall clients where the chunk store" or
// "who is primary" or "which chunk need reReplica" and so on.
func TestChunkServerWatchAndOtherMetaInBackupMaster(t *testing.T) {
	// to adapt the test suite context
	m.Shutdown()
	time.Sleep(time.Second * 3)
	m = master.NewAndServe(mAdd, path.Join(root, "m"))
	time.Sleep(time.Second * 3)

	ch := make(chan error, 5)
	// create a client, which connect to Zookeeper and get the primary master all time.
	client1 = client.NewAndServe(":1315", ":7777")
	t.Logf("client 1 new")

	time.Sleep(time.Second)
	go func() {
		connZookeeper := util.GetConnect([]string {util.ZkList})
		defer connZookeeper.Close()
		// create a backup master, which dead 10 second later then primary
		mt2 := master.NewAndServe(":1300", "")
		time.Sleep(time.Second * 10)
		mt2.Shutdown()
		ch <- nil
	}()

	// main thread sleep 2 second to wait 2 master nodes construct successfully
	time.Sleep(time.Second * 2)

	// client request master to CREATE and APPEND, change primary master's metadata, and AFTER one second,
	// the primary master dead. And then, the client will get the new master address(origin
	// backup), use LIST to see if new master is correct and available, use READ to see if chunk infos is
	// correct.
	var files []gfs.PathInfo
	err := client1.Create("/fly")
	// note: this test need to run with clear log files, otherwise will get "file dance already exists in dir"
	if err != nil {
		t.Error(err)
	}
	files, err = client1.List("/")
	var find bool
	for _, v := range files {
		fmt.Println(v.Name)
		if strings.Compare(v.Name, "fly") == 0 {
			find = true
		}
	}
	if find == false {
		t.Errorf("don't find ( %s ), primary master works wrong", "fly")
	}
	// APPEND some word "great" to /fly
	_, err = client1.Append("/fly", []byte("great"))
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Second)
	m.Shutdown()
	time.Sleep(time.Second)

	files, err = client1.List("/")
	if err != nil {
		t.Error(err)
	}
	find = false
	for _, v := range files {
		fmt.Println(v.Name)
		if strings.Compare(v.Name, "fly") == 0 {
			find = true
		}
	}
	if find == false {
		t.Errorf("don't find fly")
	}
	// use READ and check if it can still read the correct content
	buf := make([]byte, 5)
	_, err = client1.Read("/fly", 0, buf)
	if err != nil {
		t.Error(err)
	}
	if strings.Compare(string(buf), "great") != 0 {
		t.Errorf("read thing don't like write")
	}

	errorAll(ch, 1, t)
}

// TestAddChunkServerDynamicAndLoadBalance is used to check add ChunkServer dynamically and load balance.
func TestAddChunkServerDynamicAndLoadBalance(t *testing.T) {
	// to adapt the test suite context
	m.Shutdown()
	time.Sleep(time.Second * 3)
	m = master.NewAndServe(mAdd, path.Join(root, "m"))
	time.Sleep(time.Second * 3)

	// close two chunkServer
	for i := 0; i < 2; i++ {
		cs[i].Shutdown()
	}
	time.Sleep(time.Second * 3)

	// create a client, which connect to Zookeeper and get the primary master all time.
	client1 = client.NewAndServe(":1314", ":7777")

	// create and write four files
	for i := 0; i < 4; i++ {
		err := client1.Create(gfs.Path("/run" + strconv.Itoa(i)))
		if err != nil {
			t.Error(err)
		}
		// APPEND some word "great" to /run
		_, err = client1.Append(gfs.Path("/run" + strconv.Itoa(i)), []byte("great"))
		if err != nil {
			t.Error(err)
		}
	}
	m.ReportChunkServer()

	t.Logf("Now add a new data node dynamicly and check the load balance\n")
	chunkserver.NewAndServe(":10010", mAdd, path.Join(root, "cs10"))

	time.Sleep(time.Second * 3)
	m.ReportChunkServer()
	// read origin four files to check the "data source has changed" and "metadata in master is right"
	for i := 0; i < 4; i++ {
		buf := make([]byte, 5)
		_, err := client1.Read(gfs.Path("/run" + strconv.Itoa(i)), 0, buf)
		if err != nil {
			t.Error(err)
		}
		if strings.Compare(string(buf), "great") != 0 {
			t.Errorf("in file ( %s ) read thing don't like write\n", "/run" + strconv.Itoa(i))
		}
	}
}

/*
 *  TEST SUITE 3 - Fault Tolerance
 */

// Shutdown all servers in turns. You should perform re-replication well
func TestReReplication(t *testing.T) {
	p := gfs.Path("/re-replication.txt")

	ch := make(chan error, 2)
	ch <- c.Create(p)

	_, _ = c.Append(p, []byte("Dangerous"))

	fmt.Println("###### Mr. Disaster is coming...")
	time.Sleep(gfs.LeaseExpire)

	cs[1].Shutdown()
	cs[2].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[1] = chunkserver.NewAndServe(csAdd[1], mAdd, path.Join(root, "cs1"))
	cs[2] = chunkserver.NewAndServe(csAdd[2], mAdd, path.Join(root, "cs2"))

	cs[3].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[4].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[3] = chunkserver.NewAndServe(csAdd[3], mAdd, path.Join(root, "cs3"))
	cs[4] = chunkserver.NewAndServe(csAdd[4], mAdd, path.Join(root, "cs4"))
	time.Sleep(gfs.ServerTimeout)

	cs[0].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[0] = chunkserver.NewAndServe(csAdd[0], mAdd, path.Join(root, "cs0"))
	time.Sleep(gfs.ServerTimeout)

	// check equality and number of replicas
	var r1 gfs.GetChunkHandleReply
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p}, &r1)
	n := checkReplicas(r1.Handle, N*2, t)

	if n < gfs.MinimumNumReplicas {
		t.Errorf("Cannot perform replicas promptly, only get %v replicas", n)
	} else {
		fmt.Printf("###### Well done, you save %v replicas during disaster\n", n)
	}

	errorAll(ch, 2, t)
}

// Shut master down after a few actions, and restart it to see if the log works
func TestMasterLog(t *testing.T) {
	var p gfs.Path = "/usr/zyw"
	var mTestAddr gfs.ServerAddress = ":7778"
	var list gfs.ListReply
	//var chunkIds gfs.GetChunkHandleReply
	var exist1 = false
	var exist2 = false
	var err error

	// before testing, clear log folder
	err = os.Remove("./gfs/log/chunk.log")
	if err != nil {log.Error(err)}
	err = os.Remove("./gfs/log/namespace.log")
	if err != nil {log.Error(err)}

	// start a new master
	mTest := master.NewAndServe(mTestAddr, path.Join(root, "mTest"))

	// make some changes on master data
	err = mTest.Mkdir(gfs.MkdirArg{Path: p}, true)
	if err != nil {t.Error("make dir fail")}
	err = mTest.RPCCreateFile(gfs.CreateFileArg{Path: p + "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {t.Error("create file fail")}
	err = mTest.RPCCreateFile(gfs.CreateFileArg{Path: p + "/test2.txt"}, &gfs.CreateFileReply{})
	if err != nil {t.Error("create file fail")}

	// before shutting down, check data on master
	err = mTest.RPCList(gfs.ListArg{Path: p}, &list)
	if err != nil {t.Error(err)}
	if len(list.Files) != 2 {t.Error("create 2 files, but found ", len(list.Files), " files")}
	for _, v := range list.Files {
		if v.Name == "test1.txt" {exist1 = true}
		if v.Name == "test2.txt" {exist2 = true}
		fmt.Printf("%s ", v.Name)
	}
	fmt.Printf("total = %d files\n", len(list.Files))
	if !exist1 || !exist2 {
		t.Errorf("create 2 files, but found exist1 = %v, exist2 = %v", exist1, exist2)
	}
	//err = mTest.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p + "/test1.txt"}, &chunkIds)
	//log.Info(chunkIds.Handle)
	//err = mTest.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p + "/test2.txt"}, &chunkIds)
	//log.Info(chunkIds.Handle)

	// shut master down && restart
	fmt.Println("###### Shutdown Master mTest")
	mTest.Shutdown()
	fmt.Println("###### Restarting Master mTest")
	mTest = master.NewAndServe(mTestAddr, path.Join(root, "mTest"))

	// check recovery
	err = mTest.RPCList(gfs.ListArg{Path: p}, &list)
	if err != nil {t.Error(err)}
	if len(list.Files) != 2 {t.Error("create 2 files, but found ", len(list.Files), " files")}
	for _, v := range list.Files {
		if v.Name == "test1.txt" {exist1 = true}
		if v.Name == "test2.txt" {exist2 = true}
		fmt.Printf("%s ", v.Name)
	}
	fmt.Printf("total = %d files\n", len(list.Files))
	if !exist1 || !exist2 {
		t.Errorf("create 2 files, but found exist1 = %v, exist2 = %v", exist1, exist2)
	}

	// test over, shut down the mTest again
	fmt.Println("###### Shutdown Master again")
	mTest.Shutdown()

	// after testing, clear log folder
	err = os.Remove("./gfs/log/chunk.log")
	if err != nil {log.Error(err)}
	err = os.Remove("./gfs/log/namespace.log")
	if err != nil {log.Error(err)}
}

// after a few actions, do checkpoint for master, then restart it to see if the checkpoint works
func TestMasterCheckPoint(t *testing.T) {
	var p gfs.Path = "/usr/zyw"
	var mTestAddr gfs.ServerAddress = ":7778"
	var list gfs.ListReply
	//var chunkIds gfs.GetChunkHandleReply
	var exist1 = false
	var exist2 = false
	var err error

	// before testing, clear log folder
	err = os.Remove("./gfs/log/gfs-master.meta")
	if err != nil {log.Error(err)}

	// start a new master
	mTest := master.NewAndServe(mTestAddr, path.Join(root, "mTest"))

	// make some changes on master data
	err = mTest.Mkdir(gfs.MkdirArg{Path: p}, true)
	if err != nil {t.Error("make dir fail")}
	err = mTest.RPCCreateFile(gfs.CreateFileArg{Path: p + "/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {t.Error("create file fail")}
	err = mTest.RPCCreateFile(gfs.CreateFileArg{Path: p + "/test2.txt"}, &gfs.CreateFileReply{})
	if err != nil {t.Error("create file fail")}

	// before shutting down, check data on master
	err = mTest.RPCList(gfs.ListArg{Path: p}, &list)
	if err != nil {t.Error(err)}
	if len(list.Files) != 2 {t.Error("create 2 files, but found ", len(list.Files), " files")}
	for _, v := range list.Files {
		if v.Name == "test1.txt" {exist1 = true}
		if v.Name == "test2.txt" {exist2 = true}
		fmt.Printf("%s ", v.Name)
	}
	fmt.Printf("total = %d files\n", len(list.Files))
	if !exist1 || !exist2 {
		t.Errorf("create 2 files, but found exist1 = %v, exist2 = %v", exist1, exist2)
	}
	//err = mTest.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p + "/test1.txt"}, &chunkIds)
	//log.Info(chunkIds.Handle)
	//err = mTest.RPCGetChunkHandle(gfs.GetChunkHandleArg{Path: p + "/test2.txt"}, &chunkIds)
	//log.Info(chunkIds.Handle)

	// force master do checkpoint && shut master down && restart
	err = mTest.StoreMeta()
	if err != nil {t.Error(err)}
	fmt.Println("###### Shutdown Master mTest")
	mTest.Shutdown()
	fmt.Println("###### Restarting Master mTest")
	mTest = master.NewAndServe(mTestAddr, path.Join(root, "mTest"))

	// check recovery
	err = mTest.RPCList(gfs.ListArg{Path: p}, &list)
	if err != nil {t.Error(err)}
	if len(list.Files) != 2 {t.Error("create 2 files, but found ", len(list.Files), " files")}
	for _, v := range list.Files {
		if v.Name == "test1.txt" {exist1 = true}
		if v.Name == "test2.txt" {exist2 = true}
		fmt.Printf("%s ", v.Name)
	}
	fmt.Printf("total = %d files\n", len(list.Files))
	if !exist1 || !exist2 {
		t.Errorf("create 2 files, but found exist1 = %v, exist2 = %v", exist1, exist2)
	}

	// test over, shut down the mTest again
	fmt.Println("###### Shutdown Master again")
	mTest.Shutdown()

	// after testing, clear log folder
	err = os.Remove("./gfs/log/gfs-master.meta")
	if err != nil {log.Error(err)}
}

// Shut chunkServer down after a few actions, and restart it to see if the log works
func TestChunkServerLog(t *testing.T) {
	var csTestAddr gfs.ServerAddress = ":10009"
	var reply gfs.CheckVersionReply
	var err error

	// before testing, clear log folder
	err = os.Remove("./gfs/log/gfs-server.meta")
	if err != nil {log.Error(err)}
	err = os.Remove("./gfs/log/chunk10.chk")
	if err != nil {log.Error(err)}
	err = os.Remove("./gfs/log/chunk11.chk")
	if err != nil {log.Error(err)}

	// start a new master
	csTest := chunkserver.NewAndServe(csTestAddr, mAdd, "./gfs/log")

	// make some changes on master data
	err = csTest.RPCCreateChunk(gfs.CreateChunkArg{Handle: 10}, &gfs.CreateChunkReply{})
	if err != nil {t.Error("create chunk 10 fail")}
	err = csTest.RPCCreateChunk(gfs.CreateChunkArg{Handle: 11}, &gfs.CreateChunkReply{})
	if err != nil {t.Error("create chunk 11 fail")}

	// before shutting down, check data on master
	err = csTest.RPCCheckVersion(gfs.CheckVersionArg{Handle: 10, Version: 0}, &reply)
	if err != nil {t.Error(err)}
	if reply.Version != 0 {t.Error("chunk 10 version != 0")}
	log.Info(reply.Stale, reply.Version)
	err = csTest.RPCCheckVersion(gfs.CheckVersionArg{Handle: 11, Version: 1}, &reply)
	if err != nil {t.Error(err)}
	if reply.Version != 0 {t.Error("chunk 11 version != 0")}
	log.Info(reply.Stale, reply.Version)

	// shut chunkServer down && restart
	fmt.Println("###### Shutdown ChunkServer csTest")
	csTest.Shutdown()
	fmt.Println("###### Restarting ChunkServer csTest")
	csTest = chunkserver.NewAndServe(csTestAddr, mAdd, "./gfs/log")

	// after shutting down, check recovery
	err = csTest.RPCCheckVersion(gfs.CheckVersionArg{Handle: 10, Version: 0}, &reply)
	if err != nil {t.Error(err)}
	if reply.Version != 0 {t.Error("chunk 10 version != 0")}
	log.Info(reply.Stale, reply.Version)
	err = csTest.RPCCheckVersion(gfs.CheckVersionArg{Handle: 11, Version: 1}, &reply)
	if err != nil {t.Error(err)}
	if reply.Version != 1 {t.Error("chunk 11 version != 1")}
	log.Info(reply.Stale, reply.Version)

	// test over, shut down the csTest again
	fmt.Println("###### Shutdown ChunkServer csTest again")
	csTest.Shutdown()

	// after testing, clear log folder
	err = os.Remove("./gfs/log/gfs-server.meta")
	if err != nil {log.Error(err)}
	err = os.Remove("./gfs/log/chunk10.chk")
	if err != nil {log.Error(err)}
	err = os.Remove("./gfs/log/chunk11.chk")
	if err != nil {log.Error(err)}
}

// after a few actions, do checkpoint for chunkServer, then restart it to see if the checkpoint works
func TestChunkServerCheckPoint(t *testing.T) {
	var csTestAddr gfs.ServerAddress = ":10009"
	var reply gfs.CheckVersionReply
	var err error

	// before testing, clear log folder
	err = os.Remove("./gfs/log/gfs-server.meta")
	if err != nil {log.Error(err)}
	err = os.Remove("./gfs/log/chunk10.chk")
	if err != nil {log.Error(err)}
	err = os.Remove("./gfs/log/chunk11.chk")
	if err != nil {log.Error(err)}

	// start a new master
	csTest := chunkserver.NewAndServe(csTestAddr, mAdd, "./gfs/log")

	// make some changes on master data
	err = csTest.RPCCreateChunk(gfs.CreateChunkArg{Handle: 10}, &gfs.CreateChunkReply{})
	if err != nil {t.Error("create chunk 10 fail")}
	err = csTest.RPCCreateChunk(gfs.CreateChunkArg{Handle: 11}, &gfs.CreateChunkReply{})
	if err != nil {t.Error("create chunk 11 fail")}

	// before shutting down, check data on master
	err = csTest.RPCCheckVersion(gfs.CheckVersionArg{Handle: 10, Version: 0}, &reply)
	if err != nil {t.Error(err)}
	if reply.Version != 0 {t.Error("chunk 10 version != 0")}
	log.Info(reply.Stale, reply.Version)
	err = csTest.RPCCheckVersion(gfs.CheckVersionArg{Handle: 11, Version: 1}, &reply)
	if err != nil {t.Error(err)}
	if reply.Version != 0 {t.Error("chunk 11 version != 0")}
	log.Info(reply.Stale, reply.Version)

	// shut chunkServer down && restart
	fmt.Println("###### Shutdown ChunkServer csTest")
	csTest.Shutdown()
	fmt.Println("###### Restarting ChunkServer csTest")
	csTest = chunkserver.NewAndServe(csTestAddr, mAdd, "./gfs/log")

	// after shutting down, check recovery
	err = csTest.RPCCheckVersion(gfs.CheckVersionArg{Handle: 10, Version: 0}, &reply)
	if err != nil {t.Error(err)}
	if reply.Version != 0 {t.Error("chunk 10 version != 0")}
	log.Info(reply.Stale, reply.Version)
	err = csTest.RPCCheckVersion(gfs.CheckVersionArg{Handle: 11, Version: 1}, &reply)
	if err != nil {t.Error(err)}
	if reply.Version != 1 {t.Error("chunk 11 version != 1")}
	log.Info(reply.Stale, reply.Version)

	// test over, shut down the csTest again
	fmt.Println("###### Shutdown ChunkServer csTest again")
	csTest.Shutdown()

	// after testing, clear log folder
	err = os.Remove("./gfs/log/gfs-server.meta")
	if err != nil {log.Error(err)}
	err = os.Remove("./gfs/log/chunk10.chk")
	if err != nil {log.Error(err)}
	err = os.Remove("./gfs/log/chunk11.chk")
	if err != nil {log.Error(err)}
}


/*
 *  TEST SUITE 4 - Challenge
 */

// TODO: simulate an extremely adverse condition

func TestMain(tm *testing.M) {
	// create temporary directory
	var err error
	root, err = ioutil.TempDir("", "gfs-")
	if err != nil {
		log.Fatal("cannot create temporary directory: ", err)
	}

	if os.Args[len(os.Args)-1] == "-s" {
		os.Args = os.Args[:len(os.Args)-1]
		log.SetLevel(log.FatalLevel)
	}

	// run master
	_ = os.Mkdir(path.Join(root, "m"), 0755)
	m = master.NewAndServe(mAdd, path.Join(root, "m"))

	// run chunkServers
	csAdd = make([]gfs.ServerAddress, csNum)
	cs = make([]*chunkserver.ChunkServer, csNum)
	for i := 0; i < csNum; i++ {
		ii := strconv.Itoa(i)
		_ = os.Mkdir(path.Join(root, "cs"+ii), 0755)
		csAdd[i] = gfs.ServerAddress(fmt.Sprintf(":%v", 10000+i))
		cs[i] = chunkserver.NewAndServe(csAdd[i], mAdd, path.Join(root, "cs"+ii))
	}

	// init client
	c = client.NewClient(mAdd)
	time.Sleep(300 * time.Millisecond)

	// run tests
	ret := tm.Run()

	// shutdown
	for _, v := range cs {
		v.Shutdown()
	}
	m.Shutdown()
	_ = os.RemoveAll(root)

	os.Exit(ret)
}
