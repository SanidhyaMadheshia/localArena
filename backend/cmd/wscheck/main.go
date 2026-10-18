// Command wscheck dials a LocalArena WebSocket URL and prints the welcome's
// player id ("id=N") or the HTTP status of a refused upgrade. Debug tool.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/protocol"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: wscheck ws://host/ws?ticket=...")
		os.Exit(2)
	}
	d := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	c, resp, err := d.Dial(os.Args[1], nil)
	if err != nil {
		if resp != nil {
			fmt.Printf("status=%d\n", resp.StatusCode)
		} else {
			fmt.Println("error=" + err.Error())
		}
		os.Exit(1)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var w protocol.Welcome
	if err := c.ReadJSON(&w); err != nil {
		fmt.Println("error=" + err.Error())
		os.Exit(1)
	}
	fmt.Printf("id=%d mode=%s\n", w.ID, w.Mode)
}
