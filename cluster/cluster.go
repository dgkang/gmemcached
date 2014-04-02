package cluster

import (
	"github.com/dgkang/gmemcached/gmemcached"
	"hash/adler32"
	"net"
	"strconv"
	"sync"
	"time"
)

type Cluster interface {
	Add(server string, por int) error
	Set(server ...string) error
	Get(key []byte) *gmemcached.GMConnection
	List() []*gmemcached.GMConnection
}

type SelectorType uint32

const (
	ModSelector  SelectorType = 1
	HashSelector SelectorType = 2
)

type ServerList struct {
	conns []*gmemcached.GMConnection
	m     sync.RWMutex
	ct    time.Duration
	wt    time.Duration
	rt    time.Duration
}

func New(st SelectorType, ct time.Duration, wt time.Duration, rt time.Duration) Cluster {
	if st == ModSelector {
		return &ServerList{conns: make([]*gmemcached.GMConnection, 0), ct: ct, wt: wt, rt: rt}
	} else if st == HashSelector {
		return &ConsistentHashSL{vconns: make([]*ConnectionHash, 0), conns: make([]*gmemcached.GMConnection, 0), ct: ct, wt: wt, rt: rt}
	}
	return nil
}

func (S *ServerList) Add(server string, port int) error {
	S.m.Lock()
	defer func() {
		S.m.Unlock()
	}()
	if G, E := gmemcached.ConnectTimeout(server, port, S.ct, S.wt, S.rt); E != nil {
		return E
	} else {
		S.conns = append(S.conns, G)
	}
	return nil
}

func (S *ServerList) Set(server ...string) error {
	for _, v := range server {
		if h, p, e := net.SplitHostPort(v); e != nil {
			return e
		} else {
			if p, e := strconv.ParseInt(p, 10, 32); e == nil {
				if e := S.Add(h, int(p)); e != nil {
					return e
				}
			} else {
				return e
			}
		}
	}
	return nil
}

func (S *ServerList) Get(key []byte) *gmemcached.GMConnection {
	S.m.RLock()
	defer func() {
		S.m.RUnlock()
	}()
	if len(S.conns) == 0 {
		return nil
	}
	h := adler32.Checksum(key)
	return S.conns[int(h)%len(S.conns)]
}

func (S *ServerList) List() []*gmemcached.GMConnection {
	S.m.RLock()
	defer func() {
		S.m.RUnlock()
	}()
	return S.conns
}
