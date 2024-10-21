package main

import (
	"fmt"
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

var (
	SETs    = map[string]string{}
	SETsmu  = sync.RWMutex{}
	HSETs   = map[string]map[string]string{}
	HSETsmu = sync.RWMutex{}
)

func hset(args []Value) Value {
	result := Value{}

	if len(args) != 3 {
		result.typ = "error"
		result.str = "Error: wrong number of argumemnts. Usage: hset hash key value"
		return (result)
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsmu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsmu.Unlock()

	result.typ = "string"
	result.str = "OK"
	return result
}

func hget(args []Value) Value {
	result := Value{}

	if len(args) != 2 {
		result.typ = "error"
		result.str = "Error: wrong number of arguments. Usage: hget hash key"
		return result
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsmu.RLock()
	value, ok := HSETs[hash][key]
	HSETsmu.RUnlock()

	if !ok {
		result.typ = "null"
		return result
	}

	result.typ = "bulk"
	result.bulk = value
	return result
}

func hgetall(args []Value) Value {
	result := Value{}

	if len(args) != 1 {
		result.typ = "error"
		result.str = "Error: wrong number of argumemnts. Usage: hgetall hash"
	}

	hash := args[0].bulk

	HSETsmu.RLock()
	value, ok := HSETs[hash]
	HSETsmu.RUnlock()

	if !ok {
		result.typ = "null"
	}

	values := []Value{}
	for k, v := range value {
		fbulk := fmt.Sprintf("%s: %s", k, v)
		values = append(values, Value{typ: "bulk", bulk: fbulk})
	}

	result.typ = "array"
	result.array = values
	return result
}

func set(args []Value) Value {
	result := Value{}

	if len(args) != 2 {
		result.typ = "error"
		result.str = "Error: wrong number of argumemnts. Usage: set key value"
		return (result)
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsmu.Lock()
	SETs[key] = value
	SETsmu.Unlock()

	result.typ = "string"
	result.str = "OK"
	return result
}

func get(args []Value) Value {
	result := Value{}

	if len(args) != 1 {
		result.typ = "error"
		result.str = "Error: wrong number of arguments. Usage: get key"
		return result
	}

	key := args[0].bulk

	SETsmu.RLock()
	value, ok := SETs[key]
	SETsmu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	result.typ = "bulk"
	result.bulk = value
	return result
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}
