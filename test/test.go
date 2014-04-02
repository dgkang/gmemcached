package main

import (
	"fmt"
	"github.com/dgkang/gmemcached/cluster"
	"github.com/dgkang/gmemcached/gmemcached"
	"time"
)

func main() {
	G, E := gmemcached.Connect("localhost", 2345)
	if E != nil {
		fmt.Printf("%s\n", E.Error())
		return
	}

	if R, E := G.Command("stats", nil, "settings"); E == nil {
		for k, v := range R.Values() {
			fmt.Printf("%s:%+v\n", k, v)
		}
	} else {
		fmt.Printf("E:%s\n", E.Error())
	}

	C := cluster.New(cluster.HashSelector, 5000*time.Microsecond, 500*time.Microsecond, 500*time.Microsecond)
	if E := C.Set("localhost:2345", "localhost:2346", "localhost:2347", "localhost:2348"); E != nil {
		fmt.Printf("%s\n", E.Error())
		return
	}

	G = C.Get([]byte("age"))
	if G == nil {
		fmt.Printf("no find server\n")
		return
	}

	if R, E := G.Command("set", 80, "age", 0, 0, gmemcached.SizeOfBody(80)); E != nil {
		fmt.Printf("E:%s\n", E.Error())
	} else {
		fmt.Printf("RS:%d\n", R.ReplyType)
	}

	GG := C.Get([]byte("age"))
	if R, E := GG.Command("get", nil, "age"); E == nil {
		if i, e := gmemcached.Int64(R.Item("age")["data"]); e == nil {
			fmt.Printf("R:%d\n", i)
		}
		fmt.Printf("RS:%d\n", R.ReplyType)
	} else {
		fmt.Printf("E:%s\n", E.Error())
	}

	if R, E := GG.Command("incr", nil, "age", 20); E == nil {
		if i, e := gmemcached.Int64(R.Values()["data"]); e == nil {
			fmt.Printf("R:%d\n", i)
		}
		fmt.Printf("RS:%d\n", R.ReplyType)
	} else {
		fmt.Printf("E:%s\n", E.Error())
	}

	if R, E := GG.Command("version", nil); E == nil {
		fmt.Printf("R:%s\n", gmemcached.String(R.Values()["data"]))
	} else {
		fmt.Printf("E:%s\n", E.Error())
	}
}
