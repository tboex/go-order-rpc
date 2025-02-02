package main

import (
	"context"
	"fmt"
	"log"
	"net"

	orderpb "order-rpc/api/proto"

	"google.golang.org/grpc"
)

type orderServer struct {
	orderpb.UnimplementedOrderServiceServer
}

func (s *orderServer) CreateOrder(ctx context.Context, req *orderpb.OrderRequest) (*orderpb.OrderResponse, error) {
	orderID := "ORD1234"
	fmt.Printf("Received order: Item=%s, Quantity=%d\n", req.Item, req.Quantity)

	return &orderpb.OrderResponse{
		OrderId: orderID,
		Status:  "Confirmed",
	}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	orderpb.RegisterOrderServiceServer(grpcServer, &orderServer{})

	fmt.Println("Order RPC service running on :50051")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
