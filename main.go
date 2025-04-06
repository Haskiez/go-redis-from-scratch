package main

import (
	"log/slog"
	"net"
	"strings"
)

func main() {
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		slog.Error(err.Error())
		return
	}

	slog.Info("listening on port :6379")

	aof, err := NewAof("database.aof")
	if err != nil {
		slog.Error(err.Error())
		return
	}
	defer aof.Close()

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]
		handler, ok := Handlers[command]
		if !ok {
			slog.Error("invalid command", "error", err)
			return
		}
		handler(args)
	})

	conn, err := l.Accept()
	if err != nil {
		slog.Error(err.Error())
		return
	}
	defer conn.Close()

	resp := NewResp(conn)
	writer := NewWriter(conn)

	for {
		value, err := resp.Read()
		if err != nil {
			slog.Error(err.Error())
			return
		}

		if value.typ != "array" {
			slog.Error("invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			slog.Error("invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			slog.Error("invalid command", "cmd", command)
			writer.Write(Value{typ: "error", str: "Command not implemented: '" + command + "'."})
			continue
		}

		if command == "SET" || command == "HSET" || command == "DEL" {
			aof.Write(value)
		}

		result := handler(args)

		writer.Write(result)
	}

}
