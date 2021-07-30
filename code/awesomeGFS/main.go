package main

import (
	"awesomeGFS/gfs/client"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"path"
	"strconv"

	//"math/rand"
	//"time"

	"awesomeGFS/gfs"
	"awesomeGFS/gfs/chunkserver"
	"awesomeGFS/gfs/master"
	"os"
)

func runMaster() {
	if len(os.Args) < 4 {
		printUsage()
		return
	}
	addr := gfs.ServerAddress(os.Args[2])
	master.NewAndServe(addr, os.Args[3])

	ch := make(chan bool)
	<-ch
}

func runChunkServer() {
	if len(os.Args) < 5 {
		printUsage()
		return
	}
	addr := gfs.ServerAddress(os.Args[2])
	serverRoot := os.Args[3]
	masterAddr := gfs.ServerAddress(os.Args[4])
	chunkserver.NewAndServe(addr, masterAddr, serverRoot)

	ch := make(chan bool)
	<-ch
}

func runClient() {
	if len(os.Args) < 4 {
		printUsage()
		return
	}
	addr := gfs.ServerAddress(os.Args[2])
	mAddr := gfs.ServerAddress(os.Args[3])
	client.NewAndServe(addr, mAddr)

	ch := make(chan bool)
	<-ch
}

func runClientWithFixConfig() {
	// create temporary directory
	root, err := ioutil.TempDir("", "gfs-")
	if err != nil {
		log.Fatal("cannot create temporary directory: ", err)
	}

	// run master
	const mAddr = ":7777"
	_ = os.Mkdir(path.Join(root, "m"), 0755)
	master.NewAndServe(mAddr, path.Join(root, "m"))

	// run chunkServers
	const csNum = 5
	csAddr := make([]gfs.ServerAddress, csNum)
	cs := make([]*chunkserver.ChunkServer, csNum)
	for i := 0; i < csNum; i++ {
		ii := strconv.Itoa(i)
		_ = os.Mkdir(path.Join(root, "cs"+ii), 0755)
		csAddr[i] = gfs.ServerAddress(fmt.Sprintf(":%v", 10000+i))
		cs[i] = chunkserver.NewAndServe(csAddr[i], mAddr, path.Join(root, "cs"+ii))
	}

	// init client
	const clientAddr = ":1314"
	client.NewAndServe(clientAddr, mAddr)
	ch := make(chan bool)
	<-ch
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  gfs clientWithFixConfig")
	fmt.Println("  gfs master <addr> <root path>")
	fmt.Println("  gfs chunkServer <addr> <root path> <master addr>")
	fmt.Println("  gfs client <addr> <master addr>")
	fmt.Println()
}

func main() {
	log.SetLevel(log.DebugLevel)
	if len(os.Args) < 2 {
		printUsage()
		return
	}
	switch os.Args[1] {
	case "master":
		runMaster()
	case "chunkServer":
		runChunkServer()
	case "client":
		runClient()
	case "clientWithFixConfig":
		runClientWithFixConfig()
	default:
		printUsage()
	}
}


// -- old main for test go env
//package main
//
//import (
//	"awesomeGFS/greeting"
//	"fmt"
//)
//
//func main() {
//	fmt.Println("hello world!")
//
//	message := greeting.Hello("Gladys")
//	fmt.Println(message)
//}



