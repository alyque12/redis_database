package main

import (
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

var SETS = map[string]string{}
var SetsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "string", str: "Error: Wrong number of args for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SetsMu.Lock()
	SETS[key] = value
	SetsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "Error: Wrong number of args for 'get' command"}
	}

	key := args[0].bulk

	SetsMu.RLock()
	value, ok := SETS[key]
	SetsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

var HSET = map[string]map[string]string{}
var HsetMu = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "string", str: "Error: Wrong number of args for 'hset' command"}
	}

	key1 := args[0].bulk
	key2 := args[1].bulk
	value := args[2].bulk

	HsetMu.Lock()
	if _, ok := HSET[key1]; !ok {
		HSET[key1] = map[string]string{}
	}
	HSET[key1][key2] = value
	HsetMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "string", str: "Error: Wrong number of args for 'hget' command"}
	}

	key1 := args[0].bulk
	key2 := args[1].bulk

	HsetMu.RLock()
	value, ok := HSET[key1][key2]
	HsetMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "string", str: "Error: Wrong number of args for 'hgetall' command"}
	}

	key := args[0].bulk
	HsetMu.RLock()
	value, ok := HSET[key]
	HsetMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	var ret_array []Value

	for key := range value {
		ret_array = append(ret_array, Value{typ: "bulk", bulk: value[key]})
	}

	return Value{typ: "array", array: ret_array}
}
