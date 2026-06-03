package config

import "runtime"

type Config struct {
	HTTPListenAddr      string
	GRPCListenAddr      string
	WebSocketPath       string
	IngressCap          int
	ReadBufferSize      int
	MaxMessageBytes     int
	ShardCount          int
	WorkerCount         int
	SubscriberBufferCap int
}

func Default() Config {
	workers := runtime.NumCPU() * 2
	if workers < 1 {
		workers = 1
	}
	return Config{
		HTTPListenAddr:      ":8080",
		GRPCListenAddr:      ":9090",
		WebSocketPath:       "/ingress/ws",
		IngressCap:          1024,
		ReadBufferSize:      4096,
		MaxMessageBytes:     65536,
		ShardCount:          256,
		WorkerCount:         workers,
		SubscriberBufferCap: 64,
	}
}