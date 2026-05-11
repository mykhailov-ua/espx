package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/mykhailov-ua/ad-event-processor/internal/auth"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/crypto"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/delivery/grpc"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/pb"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/repository"
	"github.com/mykhailov-ua/ad-event-processor/internal/auth/token"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	google_grpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := database.Connect(ctx, cfg.DBDSN, cfg.DBMaxConns, cfg.DBMinConns)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	repo := repository.New(pool)
	tokenMaker, err := token.NewPasetoMaker(cfg.TokenSymmetricKey)
	if err != nil {
		slog.Error("failed to create token maker", "error", err)
		os.Exit(1)
	}

	hasher := crypto.NewPasswordHasher(
		uint32(cfg.Argon2Memory),
		uint32(cfg.Argon2Iterations),
		uint8(cfg.Argon2Parallelism),
	)
	authService := auth.NewService(repo, tokenMaker, hasher)
	grpcHandler := grpc.NewHandler(authService, cfg)

	lis, err := net.Listen("tcp", ":"+cfg.AuthServerPort)
	if err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
	}

	server := google_grpc.NewServer()
	pb.RegisterAuthServiceServer(server, grpcHandler)
	reflection.Register(server)

	slog.Info("starting auth gRPC server", "port", cfg.AuthServerPort)

	go func() {
		if err := server.Serve(lis); err != nil {
			slog.Error("gRPC server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down auth gRPC server")
	server.GracefulStop()
}
