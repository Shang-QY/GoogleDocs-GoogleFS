package util

import (
	"awesomeGFS/gfs"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

const ZkList = "139.224.113.116:2181"
const IndexLength = 10

func GetConnect(zkList []string) (conn *zk.Conn) {
	conn, _, err := zk.Connect(zkList, 10*time.Second)
	if err != nil {
		fmt.Println("bug")
		fmt.Println(err)
		fmt.Println("bug end")
	}
	return
}

func GetReadLock(conn *zk.Conn, path gfs.Path) error {
	var lockPath string
	var errCreate error
	for
	{
		lockPath, errCreate = conn.Create("/" + strings.ReplaceAll(string(path)[1:], "/", "\\"), nil, 0, zk.WorldACL(zk.PermAll))
		if errCreate != nil {
			fmt.Println(errCreate)
			if errCreate.Error() == "zk: node already exists" {
				lockPath, errCreate = conn.Create("/" + strings.ReplaceAll(string(path)[1:], "/", "\\") + "/READ", nil, zk.FlagEphemeral | zk.FlagSequence, zk.WorldACL(zk.PermAll))
				if errCreate != nil {
					if errCreate.Error() == "zk: node does not exist" {
						continue
					}
					fmt.Println(errCreate)
					return errCreate
				}
				break
			}
			return errCreate
		}
		lockPath, errCreate = conn.Create("/" + strings.ReplaceAll(string(path)[1:], "/", "\\") + "/READ", nil, zk.FlagEphemeral | zk.FlagSequence, zk.WorldACL(zk.PermAll))
		if errCreate != nil {
			return errCreate
		}
		break
	}
	fmt.Println("lock by zookeeper lock full path:", lockPath)
	// "", parent, lock
	parentsOrLock := strings.Split(lockPath, "/")
	fmt.Println("lock by zookeeper lock:", parentsOrLock[2])
	children, _, _err := conn.Children("/" + strings.ReplaceAll(string(path)[1:], "/", "\\"))
	if _err != nil {
		return errCreate
	}
	sort.Slice(children, func(i, j int) bool {
		iNum, _ := strconv.Atoi(children[i][len(children[i])-IndexLength:])
		jNum, _ := strconv.Atoi(children[j][len(children[j])-IndexLength:])
		return iNum > jNum
	})
	for _, v := range children {
		log.Println(v)
	}

	index := len(children)
	for i, child := range children {
		if strings.Compare(child, parentsOrLock[2]) == 0 {
			fmt.Printf("my ticket index : %v, my ticket: %s\n", i, child)
			index = i
		}
		if i > index && child[0] == 'W' {
			monitorPath := "/" + parentsOrLock[1] + "/" + child
			fmt.Printf("i: %v > my ticket index, monitor path: %s\n", i, monitorPath)
			_, _, events, err_ := conn.ExistsW(monitorPath)
			if err_ != nil {
				fmt.Println(err_)
				return errCreate
			}
			_, _, _err = conn.Get(monitorPath)
			if _err != nil {
				fmt.Println(_err)
				continue
			}
			fmt.Printf("start monitor at: %s\n", monitorPath)
			for {
				event := <- events
				fmt.Println("path:", event.Path)
				fmt.Println("type:", event.Type.String())
				if event.Type.String() == "EventNodeDeleted" {
					fmt.Println("[EventNodeDeleted]")
					break
				}
			}
		}
	}
	fmt.Println("final get READ lock")
	return nil
}

func GetWriteLock(conn *zk.Conn, path gfs.Path) error {
	var lockPath string
	var errCreate error
	for
	{
		lockPath, errCreate = conn.Create("/" + strings.ReplaceAll(string(path)[1:], "/", "\\"), nil, 0, zk.WorldACL(zk.PermAll))
		if errCreate != nil {
			fmt.Println(errCreate)
			if errCreate.Error() == "zk: node already exists" {
				lockPath, errCreate = conn.Create("/" + strings.ReplaceAll(string(path)[1:], "/", "\\") + "/WRITE", nil, zk.FlagEphemeral | zk.FlagSequence, zk.WorldACL(zk.PermAll))
				if errCreate != nil {
					if errCreate.Error() == "zk: node does not exist" {
						continue
					}
					fmt.Println(errCreate)
					return errCreate
				}
				break
			}
			return errCreate
		}
		lockPath, errCreate = conn.Create("/" + strings.ReplaceAll(string(path)[1:], "/", "\\") + "/WRITE", nil, zk.FlagEphemeral | zk.FlagSequence, zk.WorldACL(zk.PermAll))
		if errCreate != nil {
			return errCreate
		}
		break
	}
	fmt.Println("lock by zookeeper lock full path:", lockPath)
	// "", parent, lock
	parentsOrLock := strings.Split(lockPath, "/")
	fmt.Println("lock by zookeeper lock:", parentsOrLock[2])
	children, _, _err := conn.Children("/" + strings.ReplaceAll(string(path)[1:], "/", "\\"))
	if _err != nil {
		return _err
	}
	sort.Slice(children, func(i, j int) bool {
		iNum, _ := strconv.Atoi(children[i][len(children[i])-10:])
		jNum, _ := strconv.Atoi(children[j][len(children[j])-10:])
		return iNum > jNum
	})
	for _, v := range children {
		log.Println(v)
	}

	index := len(children)
	for i, child := range children {
		if strings.Compare(child, parentsOrLock[2]) == 0 {
			fmt.Printf("my ticket index : %v, my ticket: %s\n", i, child)
			index = i
		}
		if i > index {
			monitorPath := "/" + parentsOrLock[1] + "/" + child
			fmt.Printf("i: %v > my ticket index, monitor path: %s\n", i, monitorPath)
			_, _, events, err := conn.ExistsW(monitorPath)
			if err != nil {
				fmt.Println(err)
				return err
			}
			_, _, _err = conn.Get(monitorPath)
			if _err != nil {
				fmt.Println(_err)
				continue
			}
			fmt.Printf("start monitor at: %s\n", monitorPath)
			for {
				event := <- events
				fmt.Println("path:", event.Path)
				fmt.Println("type:", event.Type.String())
				if event.Type.String() == "EventNodeDeleted" {
					fmt.Println("[EventNodeDeleted]")
					break
				}
			}
		}
	}
	fmt.Println("final get WRITE lock")
	return nil
}

func Mirror(conn *zk.Conn, path string) (chan []string, chan error) {
	snapshots := make(chan []string)
	errors := make(chan error)

	go func() {
		for {
			snapshot, _, events, err := conn.ChildrenW(path)
			if err != nil {
				errors <- err
				return
			}
			snapshots <- snapshot
			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()

	return snapshots, errors
}

// this need <No file in gfs named "master">
func CreateMasterFile(conn *zk.Conn, addr gfs.ServerAddress) (masterFile string, err error) {
	lockPath, err := conn.Create("/master", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		fmt.Println(err)
		if err.Error() == "zk: node already exists" {
			lockPath, err = conn.Create("/master/master", []byte(addr), zk.FlagEphemeral | zk.FlagSequence, zk.WorldACL(zk.PermAll))
			if err != nil {
				fmt.Println(err)
				return
			}
		} else {
			return
		}
	} else {
		lockPath, err = conn.Create("/master/master", []byte(addr), zk.FlagEphemeral | zk.FlagSequence, zk.WorldACL(zk.PermAll))
		if err != nil {
			return
		}
	}

	fmt.Println("create master file in zookeeper full path:", lockPath)
	// "", parent, lock
	parentsOrLock := strings.Split(lockPath, "/")
	fmt.Println("create master file path:", parentsOrLock[2])
	return parentsOrLock[2], nil
}

// this need <No file in gfs named "client">
func CreateClientFile(conn *zk.Conn, addr gfs.ServerAddress) (masterFile string, err error) {
	lockPath, err := conn.Create("/client", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		fmt.Println(err)
		if err.Error() == "zk: node already exists" {
			lockPath, err = conn.Create(string("/client/"+addr), []byte(addr), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
			if err != nil {
				fmt.Println(err)
				return
			}
		} else {
			return
		}
	} else {
		lockPath, err = conn.Create(string("/client/"+addr), []byte(addr), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err != nil {
			return
		}
	}

	fmt.Println("create client file in zookeeper full path:", lockPath)
	// "", parent, lock
	parentsOrLock := strings.Split(lockPath, "/")
	fmt.Println("create client file path:", parentsOrLock[2])
	return parentsOrLock[2], nil
}

