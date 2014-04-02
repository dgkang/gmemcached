package cluster

import (
	"crypto/md5"
	"fmt"
	"github.com/dgkang/gmemcached/gmemcached"
	"hash/adler32"
	"io"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	VirtualServers = 32
)

type ConnectionHash struct {
	conn *gmemcached.GMConnection
	hash uint32
}

type ConsistentHashSL struct {
	vconns []*ConnectionHash
	conns  []*gmemcached.GMConnection
	m      sync.RWMutex
	ct     time.Duration
	wt     time.Duration
	rt     time.Duration
}

func BKDRHash(s []byte) uint32 {
	var seed uint32 = 131
	var hash uint32 = 0

	for _, v := range s {
		hash = hash*seed + uint32(v)
	}
	return hash & 0x7FFFFFFF
}

func MD5Hash(s []byte) uint32 {
	hash := md5.New()
	io.WriteString(hash, string(s))
	str := fmt.Sprintf("%x", hash.Sum(nil))
	return adler32.Checksum([]byte(str))
}

func (C *ConsistentHashSL) Len() int {
	return len(C.vconns)
}

func (C *ConsistentHashSL) Swap(i, j int) {
	C.vconns[i], C.vconns[j] = C.vconns[j], C.vconns[i]
}

func (C *ConsistentHashSL) Less(i, j int) bool {
	return C.vconns[i].hash < C.vconns[j].hash
}

func (C *ConsistentHashSL) Add(server string, port int) error {
	C.m.Lock()
	defer func() {
		C.m.Unlock()
	}()

	if G, E := gmemcached.ConnectTimeout(server, port, C.ct, C.wt, C.rt); E != nil {
		return E
	} else {
		for i := 0; i < VirtualServers; i++ {
			s := fmt.Sprintf("%s:%d:%d", server, port, i)
			h := MD5Hash([]byte(s))
			C.vconns = append(C.vconns, &ConnectionHash{G, h})
		}
		C.conns = append(C.conns, G)
	}
	sort.Sort(C)
	return nil
}

func (C *ConsistentHashSL) Set(server ...string) error {
	for _, v := range server {
		if h, p, e := net.SplitHostPort(v); e != nil {
			return e
		} else {
			if p, e := strconv.ParseInt(p, 10, 32); e == nil {
				if e := C.Add(h, int(p)); e != nil {
					return e
				}
			} else {
				return e
			}
		}
	}
	/*
		for _, v := range C.vconns {
			fmt.Printf("%d,", v.hash)
		}
		fmt.Printf("\n")
	*/
	return nil
}

func (C *ConsistentHashSL) Get(key []byte) *gmemcached.GMConnection {
	C.m.RLock()
	defer func() {
		C.m.RUnlock()
	}()
	if len(C.vconns) == 0 {
		return nil
	}
	h := MD5Hash(key)

	if h >= C.vconns[len(C.vconns)-1].hash {
		return C.vconns[0].conn
	}
	for _, v := range C.vconns {
		if v.hash >= h {
			return v.conn
		}
	}
	return nil
}

func (C *ConsistentHashSL) List() []*gmemcached.GMConnection {
	C.m.RLock()
	defer func() {
		C.m.RUnlock()
	}()
	return C.conns
}
