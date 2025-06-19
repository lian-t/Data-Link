package x

import (
	"fmt"
	"runtime/debug"
)

func GoSafe(f func()) {
	go RunSafe(f)
}
func RunSafe(f func()) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("ERROR:\n%v\nSTACK:\n%s", err, debug.Stack())
		}
	}()
	f()
}

func GoSafeVal(f func(v interface{}), v interface{}) {
	go RunSafeVal(f, v)
}
func RunSafeVal(f func(v interface{}), v interface{}) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("ERROR:\n%v\nSTACK:\n%s", err, debug.Stack())
		}
	}()
	f(v)
}
