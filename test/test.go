package main

import (
	"fmt"
	"github.com/dgkang/gmemcached/cluster"
	"github.com/dgkang/gmemcached/gmemcached"
	"time"
)

func main() {
	C := cluster.New(cluster.Adler32Selector, 5000*time.Microsecond, 500*time.Microsecond, 500*time.Microsecond)
	if E := C.Set("localhost:2345", "localhost:2346", "localhost:2347", "localhost:2348"); E != nil {
		fmt.Printf("%s\n", E.Error())
	}
	/*
		G, E := gmemcached.Connect("localhost", 2345)
		if E != nil {
			fmt.Printf("%s\n", E.Error())
			return
		}

		if R,E := G.CreateCommand("stats"); E == nil{
			if E := G.SendCommand(R,nil); E == nil {
				for k,v := range R.Values() {
					fmt.Printf("%s:%+v\n",k,v)
				}
			}
		}
	*/
	G := C.Get([]byte("id"))
	if G == nil {
		fmt.Printf("no find server\n")
		return
	}
	if R, E := G.CreateCommand("set", "id", 0, 0, gmemcached.SizeOfBody(112)); E == nil {
		if E := G.SendCommand(R, 112); E == nil {
			fmt.Printf("R:%d\n", R.ReplyType)
		}
	}

	GG := C.Get([]byte("id"))
	if R, E := GG.CreateCommand("get", "id"); E == nil {
		if E := GG.SendCommand(R, nil); E == nil {
			fmt.Printf("G:%v\n", R.Item("id")["data"])
		}
	}
}
