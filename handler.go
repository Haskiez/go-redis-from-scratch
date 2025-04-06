package main

import (
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Handlers = map[string]func([]Value) Value{
	"HELLO": hello,
	// "COMMAND": command,
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"DEL":     deleteKeys,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
	"EXISTS":  exists,
	"KEYS":    keys,
}

type KV struct {
	value string
	ttl   int64
}

type Expiration struct {
	cmd   string
	value int64
}

var respVersion = 2

var SETs = map[string]KV{}
var SETsMu = sync.RWMutex{}

func hello(args []Value) Value {
	typ := "array"
	if len(args) == 1 && args[0].bulk == "3" {
		typ = "map"
		respVersion = 3
	}

	return Value{typ: typ, array: []Value{
		{typ: "bulk", bulk: "server"},
		{typ: "bulk", bulk: "redis-from-scratch"},
		{typ: "bulk", bulk: "version"},
		{typ: "bulk", bulk: "2025-04-06.1"},
		{typ: "bulk", bulk: "proto"},
		{typ: "integer", num: respVersion},
		{typ: "bulk", bulk: "id"},
		{typ: "integer", num: rand.Int()},
	}}
}

func ping(args []Value) Value {
	return Value{typ: "string", str: "PONG"}
}

func command(args []Value) Value {
	return Value{typ: "array", array: []Value{
		{typ: "string", str: "COMMAND"},
		{typ: "string", str: "https://redis.io/docs/latest/commands/command/"},
		{typ: "string", str: "HELLO"},
		{typ: "string", str: "https://redis.io/docs/latest/commands/hello/"},
		{typ: "string", str: "PING"},
		{typ: "string", str: "https://redis.io/docs/latest/commands/ping/"},
		{typ: "string", str: "PING"},
		{typ: "string", str: "https://redis.io/docs/latest/commands/ping/"},
		{typ: "string", str: "GET"},
		{typ: "string", str: "https://redis.io/docs/latest/commands/get/"},
		{typ: "string", str: "SET"},
		{typ: "string", str: "https://redis.io/docs/latest/commands/set/"},
		{typ: "string", str: "DEL"},
		{typ: "string", str: "https://redis.io/docs/latest/commands/del/"},
		{typ: "string", str: "HSET"},
		{typ: "string", str: "https://redis.io/docs/latest/commands/hset/"},
		{typ: "string", str: "HGET"},
		{typ: "string", str: "https://redis.io/docs/latest/commands/hget/"},
		{typ: "string", str: "HGETALL"},
		{typ: "string", str: "https://redis.io/docs/latest/commands/hgetall/"},
	}}
}

/**
* https://redis.io/docs/latest/commands/set/
 */
func set(args []Value) Value {

	const (
		SETKEY_NOT_EXISTS = "NX"
		SETKEY_EXISTS     = "XX"

		TTL_S   = "EX"
		TTL_MS  = "PX"
		TTL_UTS = "EXAT"
		TTL_UTM = "PXAT"
		KEEPTTL = "KEEPTTL"
	)

	var ttlopt = map[string]string{
		TTL_S:   TTL_S,
		TTL_MS:  TTL_MS,
		TTL_UTS: TTL_UTS,
		TTL_UTM: TTL_UTM,
		KEEPTTL: KEEPTTL,
	}

	var (
		setkeyrule = ""
		exp        = Expiration{cmd: "", value: -1}
		rtv        = false
	)

	if len(args) < 2 {
		return ErrorValue("ERR wrong number of arguments for 'set' command")
	}

	key := args[0].bulk
	value := args[1].bulk

	for i := 2; i < len(args); i++ {
		if args[i].bulk == SETKEY_EXISTS || args[i].bulk == SETKEY_NOT_EXISTS {
			setkeyrule = args[i].bulk
			continue
		}
		if strings.ToUpper(args[i].bulk) == "GET" {
			rtv = true
			continue
		}
		if v, ok := ttlopt[strings.ToUpper(args[i].bulk)]; ok {
			exp.cmd = v
			if exp.cmd == KEEPTTL {
				exp.value = 0
				continue
			}
			if len(args) < i+2 {
				return ErrorValue("ERR value required for '" + v + "' option.")
			}
			ttl, err := strconv.ParseInt(args[i+1].bulk, 10, 64)
			if err != nil {
				return ErrorValue("ERR integer required for '" + v + "' option.")
			}
			if exp.cmd == TTL_UTS || exp.cmd == TTL_UTM {
				exp.value = ttl
			} else {
				mult := time.Second
				if exp.cmd == TTL_MS {
					mult = time.Millisecond
				}
				exp.value = time.Now().Add(time.Duration(ttl) * mult).Unix()
			}
			i++
			continue
		}
		return ErrorValue("ERR invalid option '" + strings.ToUpper(args[i].bulk) + "'.")
	}

	SETsMu.Lock()
	currv, ok := SETs[key]

	if (setkeyrule == SETKEY_EXISTS && !ok) || (setkeyrule == SETKEY_NOT_EXISTS && ok) {
		SETsMu.Unlock()
		return NullValue()
	}

	if exp.cmd == KEEPTTL {
		exp.value = currv.ttl
	}

	SETs[key] = KV{value: value, ttl: exp.value}
	SETsMu.Unlock()

	if rtv {
		if !ok {
			return NullValue()
		}
		return Value{typ: "bulk", bulk: currv.value}
	}

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return ErrorValue("ERR wrong number of arguments for 'get' command")
	}

	key := args[0].bulk

	SETsMu.Lock()
	value, ok := SETs[key]
	if ok && value.ttl != -1 && value.ttl < time.Now().Unix() {
		delete(SETs, key)
		ok = false
	}
	SETsMu.Unlock()

	if !ok {
		return NullValue()
	}

	return Value{typ: "bulk", bulk: value.value}
}

func deleteKeys(args []Value) Value {
	if len(args) == 0 {
		return ErrorValue("ERR wrong number of arguments for 'DEL' command")
	}
	numDel := 0
	SETsMu.Lock()
	for _, arg := range args {
		_, ok := SETs[arg.bulk]
		if ok {
			numDel++
			delete(SETs, arg.bulk)
		}
	}
	SETsMu.Unlock()
	return Value{typ: "integer", num: numDel}
}

var HSETs = map[string]map[string]KV{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) < 3 {
		return ErrorValue("ERR wrong number of arguments for 'hset' command")
	}

	hash := args[0].bulk
	cache := map[string]KV{}

	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return ErrorValue("ERR wrong number of arguments for 'hset' command")
		}
		k := args[i].bulk
		v := args[i+1].bulk
		cache[k] = KV{value: v, ttl: -1}
	}

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]KV{}
	}
	for k, v := range cache {
		HSETs[hash][k] = v
	}
	HSETsMu.Unlock()

	return Value{typ: "integer", num: len(cache)}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return ErrorValue("ERR wrong number of arguments for 'hget' command")
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return NullValue()
	}

	return Value{typ: "bulk", bulk: value.value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return ErrorValue("ERR wrong number of arguments for 'hgetall' command")
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	values, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return NullValue()
	}

	rt := Value{typ: "array"}

	if respVersion == 3 {
		rt.typ = "map"
	}

	for vk, vv := range values {
		rt.array = append(rt.array, Value{typ: "bulk", bulk: vk}, Value{typ: "bulk", bulk: vv.value})
	}

	return rt
}

// https://redis.io/docs/latest/commands/exists/
func exists(args []Value) Value {
	e := 0
	SETsMu.RLock()
	for _, arg := range args {
		_, ok := SETs[arg.bulk]
		if ok {
			e++
		}
	}
	return Value{typ: "integer", num: e}
}

func keys(args []Value) Value {
	if len(args) != 1 {
		return ErrorValue("ERR wrong number of arguments for 'KEYS' command.")
	}

	re, err := regexp.Compile(args[0].bulk)

	if err != nil {
		return ErrorValue("Pattern must be a valid regex expression.")
	}

	rv := Value{typ: "array", array: []Value{}}

	SETsMu.RLock()
	for k := range SETs {
		if re.Match([]byte(k)) {
			rv.array = append(rv.array, Value{typ: "bulk", bulk: k})
		}
	}
	return rv
}
