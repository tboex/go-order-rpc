package main

import (
	"context"
	"fmt"
	"log"
	"time"

	orderpb "order-rpc/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := orderpb.NewOrderServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.CreateOrder(ctx, &orderpb.OrderRequest{
		Item:     "Laptop",
		Quantity: 1,
	})
	if err != nil {
		log.Fatalf("Error calling CreateOrder: %v", err)
	}

	fmt.Printf("Order created: ID=%s, Status=%s\n", res.OrderId, res.Status)
}
