package master

import (
	"awesomeGFS/gfs/util"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	//"path"
	"strings"
	"sync"

	"awesomeGFS/gfs"
	log "github.com/sirupsen/logrus"
)

type namespaceManager struct {
	sync.Mutex
	logLock  sync.Mutex		// lock for logging
	root     *nsTree		// root node
	logFile  *os.File		// file for logging
	serialCt int			// for serialization
}

type nsTree struct {
	sync.RWMutex

	// dir or file
	isDir    bool
	// directory
	children map[string]*nsTree
	// file
	length int64
	chunks int64
}

type serialTreeNode struct {
	IsDir    bool				// dir or file
	Children map[string]int		// dir content
	Length	 int64				// file size
	Chunks   int64				// file chunks
}

// Serialize serialize the metadata for storing to disk
func (nm *namespaceManager) Serialize() []serialTreeNode {
	log.Info("namespaceManager Serializing...")

	// acquiring lock
	nm.logLock.Lock()
	defer nm.logLock.Unlock()
	nm.root.RLock()
	defer nm.root.RUnlock()

	// serialize metadata
	nm.serialCt = 0
	var ret []serialTreeNode
	nm.tree2array(&ret, nm.root)

	// clear log file and restart log
	if nm.logFile != nil {
		err := nm.logFile.Close()
		if err != nil {log.Warning(err)}
	}
	err := os.Remove(util.LogPathNamespace)
	if err != nil {log.Warning(err)}
	logFile, err := os.OpenFile(util.LogPathNamespace, os.O_CREATE | os.O_RDWR | os.O_TRUNC, 0666)
	if err != nil {panic(err)}
	nm.logFile = logFile
	nm.serialCt = 0

	return ret
}

// Deserialize deserialize the metadata from disk
func (nm *namespaceManager) Deserialize(array []serialTreeNode) error {
	log.Info("namespaceManager Deserializing...")
	nm.root.Lock()
	defer nm.root.Unlock()
	nm.root = nm.array2tree(array, len(array)-1)
	return nil
}

// LoadLog load log info into namespace manager
func (nm *namespaceManager) LoadLog() {
	log.Info("namespaceManager loading log...")
	// parse log and redo committed actions
	nm.ParseLog()
	// continue log
	logFile, err := os.OpenFile(util.LogPathNamespace, os.O_CREATE | os.O_RDWR | os.O_APPEND, 0666)
	if err != nil {log.Warning(err)}
	nm.logFile = logFile
}

func newNamespaceManager() *namespaceManager {
	nm := &namespaceManager{
		root: &nsTree{
			isDir: true,
			children: make(map[string]*nsTree),
		},
		serialCt: 0,
	}
	log.Info("-----------new namespace manager-----------")
	return nm
}

// lockParents place read lock on all parents of p. It returns the list of
// parents' name and the direct parent nsTree. If a parent does not exist,
// an error is also returned.
func (nm *namespaceManager) lockParents(root *nsTree, path gfs.Path, goDown bool) ([]string, *nsTree, error) {
	// if has a begin dir, use it; if not, use root
	var cwd *nsTree
	var ps [] string
	if root != nil {cwd = root} else {cwd = nm.root}
	if len(path) == 0 || path == "/" {return ps, cwd, nil}

	//log.Info("[lockParents] path = ", path)
	if path[0] == '/' {ps = strings.Split(string(path), "/")[1:]} else {
		ps = strings.Split(string(path), "/")
	}
	if len(ps) == 0 {return ps, cwd, nil}

	// getting locks
	if root == nil {
		//log.Printf("[lockParents] Rlock '/'")
		cwd.RLock()
	}
	for i, name := range ps {
		// check child path name
		child, ok := cwd.children[name]
		if !ok {
			return ps, cwd, fmt.Errorf("path %s not found", path)
		}
		if i == len(ps) - 1 {
			if goDown {cwd = child}
		} else {
			//log.Printf("[lockParents] Rlock %s", name)
			cwd = child
			cwd.RLock()
		}
		if cwd.isDir == false {
			return ps, cwd, fmt.Errorf("path %s contain %s, which is not a dir", path, name)
		}
	}
	return ps, cwd, nil
}

// unlockParents remove read lock on all parents. If a parent does not exist,
// it just stops and returns. This is the inverse of lockParents.
func (nm *namespaceManager) unlockParents(root *nsTree, path gfs.Path) {
	// if has a begin dir, use it; if not, use root
	var cwd *nsTree
	var ps []string
	if root != nil {cwd = root} else {cwd = nm.root}
	if len(path) == 0 || path == "/" {return}

	// split path
	//log.Info("[lockParents] path = ", path)
	if path[0] == '/' {ps = strings.Split(string(path), "/")[1:]} else {
		ps = strings.Split(string(path), "/")
	}
	if len(ps) == 0  {return}

	// releasing locks
	if root == nil {
		//log.Printf("[unlockParents] RUnlock '/'")
		cwd.RUnlock()
	}
	for _, name := range ps[:len(ps)-1] {
		c, ok := cwd.children[name]
		if !ok {
			log.Println("error in unlock")
			return
		}
		cwd = c
		log.Printf("[unlockParents] RUnlock %s", name)
		cwd.RUnlock()
	}

}

func (nm *namespaceManager) UpdateMeta(fullPath gfs.Path, length int64, chunks int64) error {
	log.Info("UpdateMeta ", fullPath)
	ps, cwd, err := nm.lockParents(nil, fullPath, false)
	defer nm.unlockParents(nil, fullPath)
	if err != nil {
		return err
	}

	file, ok := cwd.children[ps[len(ps)-1]]
	if !ok {
		return fmt.Errorf("File %v does not exist\n", fullPath)
	}
	file.Lock()
	defer file.Unlock()

	// do logging
	serial := nm.Log(util.NamespaceLogAction{Type: util.UPDATE, Path1: string(fullPath), Length: length, Chunks: chunks})
	nm.Commit(serial)

	// set meta
	file.length = length
	file.chunks = chunks

	return nil
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(fullPath gfs.Path) error {
	log.Info("create file ", fullPath)

	// split fullPath into path + filename
	path, filename := util.SplitFilePath(fullPath)
	// lock ancestor dirs read lock
	_, cwd, err := nm.lockParents(nil, path, true)
	defer nm.unlockParents(nil, path)
	if err != nil {return err}
	// lock parent dir write lock
	cwd.Lock()
	defer cwd.Unlock()
	// add new file node
	if _, ok := cwd.children[filename]; ok {
		return fmt.Errorf("file %s already exists in dir %s", filename, path)
	}
	cwd.children[filename] = new(nsTree)

	// TODO: log
	serial := nm.Log(util.NamespaceLogAction{Type: util.CREATE, Path1: string(fullPath)})
	nm.Commit(serial)
	return nil
}

// Delete deletes an file on path p.
func (nm *namespaceManager) Delete(fullPath gfs.Path) error {
	log.Info("delete ", fullPath)

	// split fullPath into path + filename
	path, filename := util.SplitFilePath(fullPath)
	// lock ancestor dirs read lock
	_, cwd, err := nm.lockParents(nil, path, true)
	defer nm.unlockParents(nil, path)
	if err != nil {return err}
	// lock parent dir write lock
	cwd.Lock()
	defer cwd.Unlock()
	// rename && delete
	node := cwd.children[filename]
	if node == nil {
		return fmt.Errorf("no such file/dir %s", fullPath)
	}
	delete(cwd.children, filename)
	//cwd.children[gfs.DeletedFilePrefix+filename] = node

	// TODO: log
	serial := nm.Log(util.NamespaceLogAction{Type: util.DELETE, Path1: string(fullPath)})
	nm.Commit(serial)
	return nil
}

// Rename rename an file on path p.
func (nm *namespaceManager) Rename(source, target gfs.Path) error {
	log.Info("[Rename] rename from ", source, " to ", target)

	// if source == target
	if strings.EqualFold(string(source), string(target)) {
		log.Info("rename from source to equal target")
		return nil
	}

	// get all locks for source and target path
	var sCwd *nsTree
	var tCwd *nsTree
	sPath, sFilename := util.SplitFilePath(source)
	tPath, tFilename := util.SplitFilePath(target)
	if len(sPath) <= 0 {sPath = "/"}
	if len(tPath) <= 0 {tPath = "/"}
	restPath := util.FindCommonPath(string(sPath), string(tPath))
	log.Info(restPath)
	log.Info(sPath)
	log.Info(tPath)
	sDir := len(sPath) > restPath + 1
	tDir := len(tPath) > restPath + 1
	commonPath := restPath
	if commonPath == 0 {commonPath++}
	log.Info("commonPath = ", sPath[:commonPath], " ", sDir, tDir)
	_, cwd, err := nm.lockParents(nil, sPath[:commonPath], true)
	defer nm.unlockParents(nil, sPath[:commonPath])
	if err != nil {return err}
	if !sDir && !tDir {
		// if source && target are under the same dir
		log.Info("[Rename] lock ", sPath[:commonPath])
		cwd.Lock()
		defer cwd.Unlock()
		sCwd = cwd
		tCwd = cwd
	} else {
		// if source && target are not under the same dir
		// if both source && target are not under cwd
		if sDir && tDir {
			log.Info("[Rename] Rlock ", sPath[:commonPath])
			cwd.RLock()
			defer cwd.RUnlock()
		} else {
			log.Info("[Rename] lock ", sPath[:commonPath])
			cwd.Lock()
			defer cwd.Unlock()
		}
		// if source is not under cwd
		if sDir {
			log.Info("sPath rest = ", sPath[restPath:])
			_, sCwd, err = nm.lockParents(cwd, sPath[restPath:], true)
			if err != nil {
				fmt.Println(err)
			}
			nm.unlockParents(cwd, sPath[restPath:])
			log.Info("[Rename] lock ", sPath[restPath:])
			sCwd.Lock()
			defer sCwd.Unlock()
		} else {sCwd = cwd}
		// if target is not under cwd
		if tDir {
			log.Info("tPath rest = ", tPath[restPath:])
			_, tCwd, err = nm.lockParents(cwd, tPath[restPath:], true)
			if err != nil {
				fmt.Println(err)
			}
			nm.unlockParents(cwd, tPath[restPath:])
			log.Info("[Rename] lock ", tPath[restPath:])
			tCwd.Lock()
			defer tCwd.Unlock()
		} else {tCwd = cwd}
	}

	if sCwd == nil || tCwd == nil {
		return fmt.Errorf("cannot find source dir %s or target dir %s", sPath, tPath)
	}

	// remove node under source dir
	node := sCwd.children[sFilename]
	if node == nil {
		return fmt.Errorf("no such file/dir %s", source)
	}
	delete(sCwd.children, sFilename)
	//sCwd.children[gfs.DeletedFilePrefix + sFilename] = node
	// add node to target dir
	if _, ok := tCwd.children[tFilename]; ok {
		return fmt.Errorf("%s already exists, will be covered by %s", target, source)
	}
	tCwd.children[tFilename] = node

	// TODO: log
	serial := nm.Log(util.NamespaceLogAction{Type: util.RENAME, Path1: string(source), Path2: string(target)})
	nm.Commit(serial)

	return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(fullPath gfs.Path) error {
	ParentPath, filename := util.SplitFilePath(fullPath)

	log.Info("mkdir ", ParentPath, "/", filename)

	_, cwd, err := nm.lockParents(nil, ParentPath, true)
	defer nm.unlockParents(nil, ParentPath)
	if err != nil {
		return err
	}

	cwd.Lock()
	defer cwd.Unlock()

	if _, ok := cwd.children[filename]; ok {
		return fmt.Errorf("Dir %s already exists\n", fullPath)
	}
	cwd.children[filename] = &nsTree{
		isDir: true,
		children: make(map[string]*nsTree),
	}

	// TODO: log
	serial := nm.Log(util.NamespaceLogAction{Type: util.MKDIR, Path1: string(fullPath)})
	nm.Commit(serial)

	return nil
}

// List returns information of all files and directories inside p.
func (nm *namespaceManager) List(path gfs.Path) ([]gfs.PathInfo, error) {
	log.Info("list ", path)

	var dir *nsTree
	if path == ("/") {
		dir = nm.root
	} else {
		_, cwd, err := nm.lockParents(nil, path, true)
		defer nm.unlockParents(nil, path)
		if err != nil {
			return nil, err
		}
		dir = cwd
	}
	dir.RLock()
	defer dir.RUnlock()

	if !dir.isDir {
		return nil, fmt.Errorf("path %s is a file, not directory", path)
	}

	ls := make([]gfs.PathInfo, 0, len(dir.children))
	for name, v := range dir.children {
		ls = append(ls, gfs.PathInfo{
			Name:   name,
			IsDir:  v.isDir,
			Length: v.length,
			Chunks: v.chunks,
		})
	}
	return ls, nil
}

// newMkdir has a recursive parameter, if true, can create dir recursively
func (nm *namespaceManager) newMkdir(fullPath gfs.Path, recursive bool) error {
	// split fullPath into dir names
	ps := strings.Split(string(fullPath), "/")[1:]
	path := "/"

	// recursively search for each dir
	for i, v := range ps {
		println("path = ", path, ", v = ", v)
		list, err := nm.List(gfs.Path(path))
		if err != nil {return err}
		index := util.Contains(list, v)

		// update path
		if i != 0 {path += "/"}
		path += ps[i]

		// if no such dir exist, create one
		if index == -1 && recursive{
			err := nm.Mkdir(gfs.Path(path))
			if err != nil {return err}
		} else if index == -1 {
			return fmt.Errorf("path %s not found", path)
		}
	}
	return nil
}


// ---------util functions for namespace manager-----------

// tree2array transforms the namespace tree into an array for serialization
func (nm *namespaceManager) tree2array(array *[]serialTreeNode, node *nsTree) int {
	n := serialTreeNode{IsDir: node.isDir, Chunks: node.chunks, Length: node.length}
	if node.isDir {
		n.Children = make(map[string]int)
		for k, v := range node.children {
			n.Children[k] = nm.tree2array(array, v)
		}
	}

	*array = append(*array, n)
	ret := nm.serialCt
	nm.serialCt++
	return ret
}

// array2tree transforms the an serialized array to namespace tree
func (nm *namespaceManager) array2tree(array []serialTreeNode, id int) *nsTree {
	n := &nsTree{
		isDir:  array[id].IsDir,
		chunks: array[id].Chunks,
		length: array[id].Length,
	}

	if array[id].IsDir {
		n.children = make(map[string]*nsTree)
		for k, v := range array[id].Children {
			n.children[k] = nm.array2tree(array, v)
		}
	}

	return n
}


// Log log the action into log file
func (nm *namespaceManager) Log(action util.NamespaceLogAction) int {
	nm.Lock()
	defer nm.Unlock()

	// fill action
	nm.serialCt++
	action.T = time.Now()
	action.Serial = nm.serialCt

	// write into log file
	marshal, err := json.Marshal(action)
	_, err = nm.logFile.Write(append(marshal, '\n'))
	if err != nil {panic(err)}
	err = nm.logFile.Sync()
	if err != nil {panic(err)}

	return action.Serial
}

// Commit log commit of the action into log file
func (nm *namespaceManager) Commit(serial int) {
	nm.Lock()
	defer nm.Unlock()

	// fill action
	action := util.NamespaceLogAction{Type: util.COMMIT, Serial: serial, T: time.Now()}

	// write into log file
	marshal, err := json.Marshal(action)
	_, err = nm.logFile.Write(append(marshal, '\n'))
	if err != nil {panic(err)}
	err = nm.logFile.Sync()
	if err != nil {panic(err)}

	return
}

// ParseLog read the log file and parse the action
func (nm *namespaceManager) ParseLog() {
	var rd *bufio.Reader
	var line string
	var action util.NamespaceLogAction
	var actionMap = make(map[int]util.NamespaceLogAction)

	// open log file in read only mode
	logFile, err := os.OpenFile(util.LogPathNamespace, os.O_RDONLY, 0666)
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
		if action.Serial > nm.serialCt {nm.serialCt = action.Serial}
		if action.Type != util.COMMIT {actionMap[action.Serial] = action} else {
			err := nm.DoAction(actionMap[action.Serial])
			if err != nil {log.Warning(err)}
		}
	}
}

// DoAction redo the action
func (nm *namespaceManager) DoAction(action util.NamespaceLogAction) error {
	path1, filename1 := util.SplitFilePath(gfs.Path(action.Path1))
	cwd1, err := nm.getCwd(path1)
	if err != nil {return err}

	// handle 4 kinds of actions
	switch action.Type {
	case util.CREATE:
		cwd1.children[filename1] = new(nsTree)
		break
	case util.MKDIR:
		cwd1.children[filename1] = &nsTree{
			isDir: true,
			children: make(map[string]*nsTree),
		}
		break
	case util.DELETE:
		node := cwd1.children[filename1]
		delete(cwd1.children, filename1)
		cwd1.children[gfs.DeletedFilePrefix+filename1] = node
		break
	case util.RENAME:
		// get target node
		path2, filename2 := util.SplitFilePath(gfs.Path(action.Path2))
		cwd2, err := nm.getCwd(path2)
		if err != nil {return err}
		// do rename
		node := cwd1.children[filename1]
		delete(cwd1.children, filename1)
		cwd1.children[gfs.DeletedFilePrefix+filename1] = node
		cwd2.children[filename2] = node
		break
	case util.UPDATE:
		file, ok := cwd1.children[filename1]
		if !ok {
			return fmt.Errorf("File %v does not exist\n", action.Path1)
		}
		file.length = action.Length
		file.chunks = action.Chunks
		break
	}

	return nil
}

func (nm *namespaceManager) getCwd(path gfs.Path) (*nsTree, error) {
	var cwd *nsTree
	var ps [] string

	// set root node
	cwd = nm.root
	if len(path) == 0 || path == "/" {return cwd, nil}

	// split path
	if path[0] == '/' {ps = strings.Split(string(path), "/")[1:]} else {
		ps = strings.Split(string(path), "/")
	}
	if len(ps) == 0 {return cwd, nil}

	// recursively get cwd
	for _, name := range ps {
		// check child path name
		child, ok := cwd.children[name]
		if !ok {return cwd, fmt.Errorf("path %s not found", path)}
		// set next cwd
		cwd = child
		if cwd.isDir == false {
			return cwd, fmt.Errorf("path %s contain %s, which is not a dir", path, name)
		}
	}

	return cwd, nil
}

