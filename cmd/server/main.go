package main

import (
	"context"
	"net"
	"net/http"
	"time"

	document_liveeditv1 "github.com/Anabol1ks/LiveEdit/gen/proto/document"
	user_liveeditv1 "github.com/Anabol1ks/LiveEdit/gen/proto/user"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/cors"

	_ "embed"

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
	"google.golang.org/grpc/credentials/insecure"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // CORS можно доработать
}

func wsEditorHandler(editorService *editor.Service, jwtManager *auth.JWTManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("token")
		if token == "" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
		Claims, err := jwtManager.Parse(token, false)
		if err != nil {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		userID := Claims.UserID
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		editorService.HandleWebSocket(conn, uint64(userID))
	}
}

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

	// Запуск автосохранения редактора
	stopCh := make(chan struct{})
	go editorService.StartAutoSave(5*time.Second, stopCh)
	defer close(stopCh)

	editor_liveeditv1.RegisterEditorServiceServer(grpcServer, editorService)

	// Сначала запускаем REST Gateway в отдельной горутине
	go runRESTGateway("localhost"+cfg.AppPort, log, editorService, jwtManager)

	lis, err := net.Listen("tcp", cfg.AppPort) // напр. ":50051"
	if err != nil {
		log.Fatal("failed to listen: ", zap.String("error", err.Error()))
	}

	log.Info("gRPC server started on", zap.String("address", cfg.AppPort))
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal("server failed:", zap.String("error", err.Error()))
	}
}

func runRESTGateway(grpcEndpoint string, log *zap.Logger, editorService *editor.Service, jwtManager *auth.JWTManager) {
	ctx := context.Background()
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	if err := user_liveeditv1.RegisterUserServiceHandlerFromEndpoint(ctx, mux, grpcEndpoint, opts); err != nil {
		log.Fatal("failed to register user service: ", zap.Error(err))
	}
	if err := document_liveeditv1.RegisterDocumentServiceHandlerFromEndpoint(ctx, mux, grpcEndpoint, opts); err != nil {
		log.Fatal("failed to register document service: ", zap.Error(err))
	}
	if err := editor_liveeditv1.RegisterEditorServiceHandlerFromEndpoint(ctx, mux, grpcEndpoint, opts); err != nil {
		log.Fatal("failed to register editor service: ", zap.Error(err))
	}

	// Создаём основной ServeMux для http
	mainMux := http.NewServeMux()
	// Swagger UI и статика
	mainMux.Handle("/swagger/", http.StripPrefix("/swagger/", http.FileServer(http.Dir("./swagger"))))
	// REST API
	mainMux.Handle("/", mux)
	// WebSocket endpoint
	mainMux.HandleFunc("/ws/editor", wsEditorHandler(editorService, jwtManager))

	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:5173"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: true,
	}).Handler(mainMux)

	log.Info("REST gateway and Swagger UI started on :8080")
	if err := http.ListenAndServe(":8080", corsHandler); err != nil {
		log.Fatal("failed to serve REST gateway: ", zap.Error(err))
	}
}
