package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	orderpb "go-order-rpc/api/proto"

	"google.golang.org/grpc"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

var (
	psql_host     = "localhost"
	psql_port     = 5432
	psql_user     = os.Getenv("POSTGRES_USER")
	psql_password = os.Getenv("POSTGRES_PASS")
	psql_dbname   = "orderdb"
)

type orderServer struct {
	orderpb.UnimplementedOrderServiceServer
	mu       sync.Mutex
	db       *sqlx.DB
	cache    *redis.Client
	orderCnt int
}

func (s *orderServer) CreateOrder(ctx context.Context, req *orderpb.OrderRequest) (*orderpb.OrderResponse, error) {
	// Safely increment the order count
	s.mu.Lock()
	s.orderCnt++
	orderID := fmt.Sprintf("ORD%05d", s.orderCnt) // e.g., ORD00001

	err := s.db.QueryRow("INSERT INTO orders (item, quantity) VALUES ($1, $2) RETURNING id", req.Item, req.Quantity).Scan(&orderID)
	if err != nil {
		return nil, err
	}

	orderKey := fmt.Sprintf("order:%s", orderID)
	orderVal := fmt.Sprintf("Item=%s, Quantity=%d", req.Item, req.Quantity)

	// Store in Redis with cache duration
	s.cache.Set(ctx, orderKey, orderVal, 10*time.Minute)

	s.mu.Unlock()

	fmt.Printf("Received order (%s): Item=%s, Quantity=%d\n", orderID, req.Item, req.Quantity)

	return &orderpb.OrderResponse{
		OrderId:  orderID,
		Status:   "Confirmed",
		Item:     req.Item,
		Quantity: req.Quantity,
	}, nil
}

func main() {
	// Connect to PSQL
	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
		psql_host, psql_port, psql_user, psql_password, psql_dbname)
	db, err := sqlx.Connect("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Connect to Redis
	cache := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer cache.Close()

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	orderpb.RegisterOrderServiceServer(grpcServer, &orderServer{db: db, cache: cache})

	fmt.Println("Order RPC service running on :50051")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
