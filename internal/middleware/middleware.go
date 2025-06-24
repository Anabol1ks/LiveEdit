package middleware

import (
	"context"
	"strings"

	"github.com/Anabol1ks/LiveEdit/internal/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Unary interceptor
func AuthUnaryInterceptor(jwtManager *auth.JWTManager, publicMethods map[string]bool) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {

		if publicMethods[info.FullMethod] {
			return handler(ctx, req)
		}

		userCtx, err := authenticateContext(ctx, jwtManager)
		if err != nil {
			return nil, err
		}

		return handler(userCtx, req)
	}
}

// Stream interceptor
func AuthStreamInterceptor(jwtManager *auth.JWTManager, publicMethods map[string]bool) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {

		if publicMethods[info.FullMethod] {
			return handler(srv, ss)
		}

		userCtx, err := authenticateContext(ss.Context(), jwtManager)
		if err != nil {
			return err
		}

		wrapped := &wrappedServerStream{ServerStream: ss, wrappedCtx: userCtx}
		return handler(srv, wrapped)
	}
}

// Вспомогательная функция для извлечения user_id из токена
func authenticateContext(ctx context.Context, jwtManager *auth.JWTManager) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, status.Error(codes.Unauthenticated, "missing metadata")
	}

	authHeader := md["authorization"]
	if len(authHeader) == 0 {
		return ctx, status.Error(codes.Unauthenticated, "missing token")
	}

	token := strings.TrimPrefix(authHeader[0], "Bearer ")
	claims, err := jwtManager.Parse(token, false)
	if err != nil {
		return ctx, status.Error(codes.Unauthenticated, "invalid token")
	}

	return context.WithValue(ctx, "user_id", claims.UserID), nil
}

// Обёртка для stream, чтобы подменить context
type wrappedServerStream struct {
	grpc.ServerStream
	wrappedCtx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.wrappedCtx
}
