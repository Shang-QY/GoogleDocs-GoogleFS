package client

import (
	"awesomeGFS/gfs"
	"awesomeGFS/gfs/util"
	"sync"
	"time"
)

type leaseBuffer struct {
	sync.RWMutex
	master gfs.ServerAddress
	buffer map[gfs.ChunkId]*gfs.Lease
	tick   time.Duration
}

// newLeaseBuffer returns a leaseBuffer.
// The downloadBuffer will cleanup expired items every tick.
func newLeaseBuffer(ms gfs.ServerAddress, tick time.Duration) *leaseBuffer {
	buf := &leaseBuffer{
		buffer: make(map[gfs.ChunkId]*gfs.Lease),
		tick:   tick,
		master: ms,
	}

	// cleanup
	go func() {
		ticker := time.Tick(tick)
		for {
			<-ticker
			now := time.Now()
			buf.Lock()
			for id, item := range buf.buffer {
				if item.Expire.Before(now) {
					delete(buf.buffer, id)
				}
			}
			buf.Unlock()
		}
	}()

	return buf
}

func (buf *leaseBuffer) Get(handle gfs.ChunkId) (*gfs.Lease, error) {
	buf.Lock()
	defer buf.Unlock()
	lease, ok := buf.buffer[handle]

	if !ok { // ask master to send one
		var l gfs.GetPrimaryAndSecondariesReply
		err := util.Call(buf.master, "Master.RPCGetPrimaryAndSecondaries", gfs.GetPrimaryAndSecondariesArg{Handle: handle}, &l)
		if err != nil {
			return nil, err
		}

		lease = &gfs.Lease{Primary: l.Primary, Expire: l.Expire, Secondaries: l.Secondaries}
		buf.buffer[handle] = lease
		return lease, nil
	}
	return lease, nil
}
