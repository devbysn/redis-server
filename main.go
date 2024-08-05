package main

import (
    "fmt"
    "net"
    "strings"
)

func main() {
    fmt.Println("Main: Starting Redis server on port :6379")

    // Create a new server
    l, err := net.Listen("tcp", ":6379")
    if err != nil {
        fmt.Println("Main: Error creating listener:", err)
        return
    }

    fmt.Println("Main: Waiting for connections...")

    // Listen for connections
    conn, err := l.Accept()
    if err != nil {
        fmt.Println("Main: Error accepting connection:", err)
        return
    }

    fmt.Println("Main: New connection accepted")

    defer conn.Close()

    for {
        fmt.Println("Main: Waiting for new command...")
        resp := NewResp(conn)
        value, err := resp.Read()
        if err != nil {
            fmt.Println("Main: Error reading command:", err)
            return
        }

        fmt.Printf("Main: Received command: %+v\n", value)

        if value.typ != "array" {
            fmt.Println("Main: Invalid request, expected array")
            return
        }

        if len(value.array) == 0 {
            fmt.Println("Main: Invalid request, expected array length > 0")
            continue
        }

        command := strings.ToUpper(value.array[0].bulk)
        args := value.array[1:]

        fmt.Printf("Main: Processing command: %s with args: %+v\n", command, args)

        writer := NewWriter(conn)

        handler, ok := Handlers[command]

        if !ok {
            fmt.Println("Main: Invalid command:", command)
            writer.Write(Value{typ: "string", str: ""})
            continue
        }

        result := handler(args)
        fmt.Printf("Main: Command result: %+v\n", result)

        err = writer.Write(result)
        if err != nil {
            fmt.Println("Main: Error writing response:", err)
        }
    }
}
