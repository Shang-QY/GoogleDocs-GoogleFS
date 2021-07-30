package util

import (
	"fmt"
	"math/rand"
	"net/rpc"

	"awesomeGFS/gfs"
)

// Call is RPC call helper
func Call(srv gfs.ServerAddress, rpcname string, args interface{}, reply interface{}) error {
	c, errx := rpc.Dial("tcp", string(srv))
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	return err
}

// CallAll applies the rpc call to all destinations.
func CallAll(dst []gfs.ServerAddress, rpcname string, args interface{}) error {
	ch := make(chan error)
	for _, d := range dst {
		go func(addr gfs.ServerAddress) {
			ch <- Call(addr, rpcname, args, nil)
		}(d)
	}
	errList := ""
	for range dst {
		if err := <-ch; err != nil {
			errList += err.Error() + ";"
		}
	}

	if errList == "" {
		return nil
	} else {
		return fmt.Errorf(errList)
	}
}

// Sample randomly chooses k elements from {0, 1, ..., n-1}.
// n should not be less than k.
func Sample(n, k int) ([]int, error) {
	if n < k {
		return nil, fmt.Errorf("population is not enough for sampling (n = %d, k = %d)", n, k)
	}
	return rand.Perm(n)[:k], nil
}


// SplitFilePath partition the last filename from p
// e.g. /foo/bar/haha.txt -> /foo/bar , haha.txt
func SplitFilePath(p gfs.Path) (gfs.Path, string) {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i], string(p[i+1:])
		}
	}
	return "", ""
}

// FindCommonPath returns the max common path index
// e.g. a = "/usr/local/ab", b = "/usr/local/abc", ret = 4
func FindCommonPath(a, b string) int {
	lastBreak, i := -1, 0

	la := len(a)
	lb := len(b)
	for i = 0; i < la && i < lb; i++ {
		if a[i] != b[i] { break}
		fmt.Printf("%c ", a[i])
		//if a[i] == '/' {lastBreak = i}
		//if i == la - 1 || i == lb - 1 {lastBreak = i + 1}
	}

	if (i == la && i < lb && b[i] == '/') || (i == lb && i < la && a[i] == '/') || (i == la && i == lb) {
		return i
	}

	for lastBreak = i - 1; lastBreak > 0; lastBreak-- {
		if a[lastBreak] == '/' {break}
	}
	return lastBreak
}

// Contains returns whether the slice contain the subject
func Contains(slice []gfs.PathInfo, s string) int {
	for index, value := range slice {
		if value.Name == s {
			return index
		}
	}
	return -1
}


