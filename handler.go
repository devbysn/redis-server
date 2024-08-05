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

func ping(args []Value) Value {
	fmt.Println("Handler: Executing PING command")
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	fmt.Println("Handler: Executing SET command")
	if len(args) != 2 {
		fmt.Println("Handler: Error - wrong number of arguments for SET")
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	fmt.Printf("Handler: SET %s = %s\n", key, value)
	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	fmt.Println("Handler: Executing GET command")
	if len(args) != 1 {
		fmt.Println("Handler: Error - wrong number of arguments for GET")
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		fmt.Printf("Handler: GET %s - Key not found\n", key)
		return Value{typ: "null"}
	}

	fmt.Printf("Handler: GET %s = %s\n", key, value)
	return Value{typ: "bulk", bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	fmt.Println("Handler: Executing HSET command")
	if len(args) != 3 {
		fmt.Println("Handler: Error - wrong number of arguments for HSET")
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	fmt.Printf("Handler: HSET %s %s = %s\n", hash, key, value)
	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	fmt.Println("Handler: Executing HGET command")
	if len(args) != 2 {
		fmt.Println("Handler: Error - wrong number of arguments for HGET")
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		fmt.Printf("Handler: HGET %s %s - Key not found\n", hash, key)
		return Value{typ: "null"}
	}

	fmt.Printf("Handler: HGET %s %s = %s\n", hash, key, value)
	return Value{typ: "bulk", bulk: value}
}

func hgetall(args []Value) Value {
	fmt.Println("Handler: Executing HGETALL command")
	if len(args) != 1 {
		fmt.Println("Handler: Error - wrong number of arguments for HGETALL")
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		fmt.Printf("Handler: HGETALL %s - Hash not found\n", hash)
		return Value{typ: "null"}
	}

	values := []Value{}
	for k, v := range value {
		values = append(values, Value{typ: "bulk", bulk: k})
		values = append(values, Value{typ: "bulk", bulk: v})
	}

	fmt.Printf("Handler: HGETALL %s - Returning %d key-value pairs\n", hash, len(values)/2)
	return Value{typ: "array", array: values}
}
