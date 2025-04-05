package main

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

type KV struct {
	value string
	ttl   int64
}

type Expiration struct {
	cmd   string
	value int64
}

var SETs = map[string]KV{}
var SETsMu = sync.RWMutex{}

func ping(args []Value) Value {
	return Value{typ: "string", str: "PONG"}
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
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
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
				return Value{typ: "error", str: "ERR value required for '" + v + "' option."}
			}
			ttl, err := strconv.ParseInt(args[i+1].bulk, 10, 64)
			if err != nil {
				return Value{typ: "error", str: "ERR integer required for '" + v + "' option."}
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
		return Value{typ: "error", str: "ERR invalid option '" + strings.ToUpper(args[i].bulk) + "'."}
	}

	SETsMu.Lock()
	currv, ok := SETs[key]

	if (setkeyrule == SETKEY_EXISTS && !ok) || (setkeyrule == SETKEY_NOT_EXISTS && ok) {
		SETsMu.Unlock()
		return Value{typ: "null"}
	}

	if exp.cmd == KEEPTTL {
		exp.value = currv.ttl
	}

	SETs[key] = KV{value: value, ttl: exp.value}
	SETsMu.Unlock()

	if rtv {
		if !ok {
			return Value{typ: "null"}
		}
		return Value{typ: "bulk", bulk: currv.value}
	}

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
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
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value.value}
}

var HSETs = map[string]map[string]KV{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]KV{}
	}
	HSETs[hash][key] = KV{value: value, ttl: -1}
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value.value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETsMu.RLock()
	values, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	rt := Value{typ: "array"}

	for vk, vv := range values {
		rt.array = append(rt.array, Value{typ: "bulk", bulk: vk}, Value{typ: "bulk", bulk: vv.value})
	}

	return rt
}
