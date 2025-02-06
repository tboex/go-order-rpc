package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	orderpb "go-order-rpc/api/proto"
	logging "go-order-rpc/util"

	"go.uber.org/zap"
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
	mu     sync.Mutex
	db     *sqlx.DB
	cache  *redis.Client
	logger *zap.SugaredLogger
}

func (s *orderServer) CreateOrder(ctx context.Context, req *orderpb.OrderRequest) (*orderpb.OrderResponse, error) {
	// Safely increment the order count
	s.mu.Lock()
	var orderID int

	err := s.db.QueryRow("INSERT INTO orders (item, quantity) VALUES ($1, $2) RETURNING id", req.Item, req.Quantity).Scan(&orderID)
	if err != nil {
		return nil, err
	}

	orderKey := fmt.Sprintf("order:%d", orderID)
	orderVal := fmt.Sprintf("Item=%s, Quantity=%d", req.Item, req.Quantity)

	// Store in Redis with cache duration
	s.cache.Set(ctx, orderKey, orderVal, 10*time.Minute)

	s.mu.Unlock()

	var orderIDTag = fmt.Sprintf("ORD%05d", orderID)
	s.logger.Infow("Received order",
		"id", orderIDTag,
		"item", req.Item,
		"quantity", req.Quantity,
	)

	return &orderpb.OrderResponse{
		OrderId:  orderIDTag,
		Status:   "Confirmed",
		Item:     req.Item,
		Quantity: req.Quantity,
	}, nil
}

// GetOrder retrieves an order from Redis first, then PostgreSQL as fallback
func (s *orderServer) GetOrder(ctx context.Context, req *orderpb.OrderRequestID) (*orderpb.OrderResponse, error) {
	orderID := strings.TrimLeft(req.OrderId[len("ORD"):], "0")
	orderKey := fmt.Sprintf("order:%s", orderID)

	// Check Redis cache
	cachedOrder, err := s.cache.Get(ctx, orderKey).Result()
	if err == nil {
		var item string
		var quantity int
		fmt.Sscanf(cachedOrder, "%s:%d", &item, &quantity)

		s.logger.Infow("Cache hit",
			"id", req.OrderId,
			"item", item,
			"quantity", quantity,
		)
		return &orderpb.OrderResponse{
			OrderId:  req.OrderId,
			Item:     item,
			Quantity: int32(quantity),
			Status:   "Confirmed (Cached)",
		}, nil
	}

	s.logger.Debug("Cache miss, querying database...")

	// Query PostgreSQL
	var item string
	var quantity int
	err = s.db.QueryRow("SELECT item, quantity FROM orders WHERE id=$1", orderID).Scan(&item, &quantity)
	if err != nil {
		return nil, fmt.Errorf("order not found")
	}

	// Store in Redis for future requests
	s.cache.Set(ctx, orderKey, fmt.Sprintf("%s:%d", item, quantity), 10*time.Minute)

	s.logger.Infow("Order retrieved from database",
		"id", req.OrderId,
		"item", item,
		"quantity", quantity,
	)

	return &orderpb.OrderResponse{
		OrderId:  req.OrderId,
		Item:     item,
		Quantity: int32(quantity),
		Status:   "Confirmed",
	}, nil
}

func (s *orderServer) TrackOrderStatus(req *orderpb.OrderRequestID, stream orderpb.OrderService_TrackOrderStatusServer) error {
	orderID := req.OrderId
	statuses := []string{"Processing", "Shipped", "Out for Delivery", "Delivered"}

	for _, status := range statuses {
		time.Sleep(2 * time.Second) //Aritifical delay
		err := stream.Send(&orderpb.OrderStatusUpdate{
			OrderId:           orderID,
			Status:            status,
			EstimatedDelivery: "2 days", // TEMP value
		})
		if err != nil {
			return err
		}
		s.logger.Infow(fmt.Sprintf("Order %s status updated", orderID),
			"id", orderID,
			"status", status,
		)
	}

	return nil
}

func (s *orderServer) CreateBulkOrders(stream orderpb.OrderService_CreateBulkOrdersServer) error {
	var orderIDs []string
	successCount := 0

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&orderpb.BulkOrderResponse{
				SuccessCount: int32(successCount),
				OrderIds:     orderIDs,
			})
		}

		if err != nil {
			return err
		}

		var orderID int
		err = s.db.QueryRow("INSERT INTO orders (item, quantity) VALUES ($1, $2) RETURNING id", req.Item, req.Quantity).Scan(&orderID)
		if err != nil {
			continue
		}

		orderKey := fmt.Sprintf("order:%d", orderID)
		orderVal := fmt.Sprintf("Item=%s, Quantity=%d", req.Item, req.Quantity)

		// Store in Redis with cache duration
		s.cache.Set(stream.Context(), orderKey, orderVal, 10*time.Minute)

		var orderIDTag = fmt.Sprintf("ORD%05d", orderID)
		s.logger.Infow("Received order",
			"id", orderIDTag,
			"item", req.Item,
			"quantity", req.Quantity,
		)

		orderIDs = append(orderIDs, orderIDTag)
		successCount++
	}
}

func main() {
	// Create Logger
	logger := logging.CreateLogger()
	defer logger.Sync() // flushes buffer, if any
	sugar := logger.Sugar()

	// Connect to PSQL
	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
		psql_host, psql_port, psql_user, psql_password, psql_dbname)
	db, err := sqlx.Connect("postgres", psqlInfo)
	if err != nil {
		sugar.Errorf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Connect to Redis
	cache := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer cache.Close()

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		sugar.Errorf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	orderpb.RegisterOrderServiceServer(grpcServer, &orderServer{db: db, cache: cache, logger: sugar})

	sugar.Infoln("Order RPC service running on :50051")
	if err := grpcServer.Serve(listener); err != nil {
		sugar.Errorf("Failed to serve: %v", err)
	}
}
