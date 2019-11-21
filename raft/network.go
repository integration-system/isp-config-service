package raft

import (
	"github.com/hashicorp/raft"
	"net"
	"time"
)

type StreamLayer struct {
	net.Listener
}

func (r *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}
