package main

import (
	"context"
	"log"
	"net"
	"net/http"

	document_liveeditv1 "github.com/Anabol1ks/LiveEdit/gen/proto/document"
	user_liveeditv1 "github.com/Anabol1ks/LiveEdit/gen/proto/user"
	"github.com/go-redis/redis/v8"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	editor_liveeditv1 "github.com/Anabol1ks/LiveEdit/gen/proto/editor"
	"github.com/Anabol1ks/LiveEdit/internal/auth"
	"github.com/Anabol1ks/LiveEdit/internal/config"
	"github.com/Anabol1ks/LiveEdit/internal/db"
	"github.com/Anabol1ks/LiveEdit/internal/logger"
	"github.com/Anabol1ks/LiveEdit/internal/middleware"
	"github.com/Anabol1ks/LiveEdit/internal/service/document"
	"github.com/Anabol1ks/LiveEdit/internal/service/editor"
	"github.com/Anabol1ks/LiveEdit/internal/service/user"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.Load()

	if err := logger.Init(cfg.AppEnv); err != nil {
		panic(err)
	}

	log := logger.L()
	log.Info("Logger initialized successfully")

	db.ConnectDB(cfg, log)
	db.Migrate(log)

	jwtManager := &auth.JWTManager{
		AccessSecret:  cfg.AccessSecret,
		RefreshSecret: cfg.RefreshSecret,
		AccessTTL:     cfg.AccessTTL,
		RefreshTTL:    cfg.RefreshTTL,
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // или твой адрес Redis
		Password: "",               // если есть пароль
		DB:       0,                // по умолчанию
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatal("Не удалось подключиться к Redis", zap.Error(err))
	}

	publicMethods := map[string]bool{
		"/user.UserService/Register":     true,
		"/user.UserService/Login":        true,
		"/user.UserService/RefreshToken": true,
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(middleware.AuthUnaryInterceptor(jwtManager, publicMethods)),
		grpc.StreamInterceptor(middleware.AuthStreamInterceptor(jwtManager, publicMethods)),
	)

	userService := &user.Service{
		DB:  db.DB,
		JWT: jwtManager,
		Log: log,
	}

	user_liveeditv1.RegisterUserServiceServer(grpcServer, userService)

	documentService := &document.Service{
		DB:  db.DB,
		JWT: jwtManager,
		Log: log,
	}
	document_liveeditv1.RegisterDocumentServiceServer(grpcServer, documentService)

	editorService := &editor.Service{
		DB:    db.DB,
		JWT:   jwtManager,
		Log:   log,
		Redis: rdb,
	}

	editor_liveeditv1.RegisterEditorServiceServer(grpcServer, editorService)

	lis, err := net.Listen("tcp", cfg.AppPort) // напр. ":50051"
	if err != nil {
		log.Fatal("failed to listen: ", zap.String("error", err.Error()))
	}

	log.Info("gRPC server started on", zap.String("address", cfg.AppPort))
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("server failed:", zap.String("error", err.Error()))
	}

	go runRESTGateway(cfg.AppPort)

}

func runRESTGateway(grpcEndpoint string) {
	ctx := context.Background()
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}

	// Регистрируем gRPC сервисы для gateway
	if err := user_liveeditv1.RegisterUserServiceHandlerFromEndpoint(ctx, mux, grpcEndpoint, opts); err != nil {
		log.Fatalf("failed to register user service: %v", err)
	}
	// if err := document_liveeditv1.RegisterDocumentServiceHandlerFromEndpoint(ctx, mux, grpcEndpoint, opts); err != nil {
	// 	log.Fatalf("failed to register document service: %v", err)
	// }
	// if err := editor_liveeditv1.RegisterEditorServiceHandlerFromEndpoint(ctx, mux, grpcEndpoint, opts); err != nil {
	// 	log.Fatalf("failed to register editor service: %v", err)
	// }

	log.Println("REST gateway started on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("failed to serve REST gateway: %v", err)
	}
}
