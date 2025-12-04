package main

import (
	"context"
	"log"
	"net"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/thetangentline/grpc/proto"
	grpc "google.golang.org/grpc"
	glog "google.golang.org/grpc/grpclog"
)

// -------------------------------------------------------------------->

const (
	PORT = ":8080"
)

var grpcLog glog.LoggerV2 = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

type Connection struct {
	stream grpc.ServerStreamingServer[proto.Message]
	id     string
	active bool
	error  chan error
}

type Server struct {
	proto.UnimplementedBroadcastServer
	Connections []*Connection
}

// -------------------------------------------------------------------->

func (s *Server) CreateConnection(connect *proto.Connect, stream grpc.ServerStreamingServer[proto.Message]) error {
	// Create connection for new client
	conn := &Connection{
		stream: stream,
		id:     uuid.New().String(),
		active: true,
		error:  make(chan error, 1),
	}
	s.Connections = append(s.Connections, conn)

	grpcLog.Info("Client connected: ", connect.User.Name)
	return <-conn.error
}

func (s *Server) BroadcastMessage(ctx context.Context, msg *proto.Message) (*proto.Close, error) {
	// Flag to wait for broadcasting to finish
	wait := sync.WaitGroup{}

	// Broadcast messages
	for _, conn := range s.Connections {
		wait.Add(1)

		// Create goroutine to send message to each client
		go func(msg *proto.Message, conn *Connection) {
			defer wait.Done()

			if conn.active {
				err := conn.stream.Send(msg)
				grpcLog.Info("Sending message to: ", conn.stream)

				if err != nil {
					grpcLog.Error("Error sending to client %s: %v", conn.id, err)
					conn.active = false
					conn.error <- err
				}
			}
		}(msg, conn)
	}

	// Wait for all threads to finish
	wait.Wait()

	return &proto.Close{}, nil
}

// -------------------------------------------------------------------->

func main() {
	var connection []*Connection

	server := &Server{
		Connections: connection,
	}

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatal("Error creating server")
	}

	grpcLog.Info("Staring server at port", PORT)

	proto.RegisterBroadcastServer(grpcServer, server)
	grpcServer.Serve(listener)

}
