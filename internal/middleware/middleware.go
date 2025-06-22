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

func AuthInterceptor(jwtManager *auth.JWTManager, publicMethods map[string]bool) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {

		if publicMethods[info.FullMethod] {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		authHeader := md["authorization"]
		if len(authHeader) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing token")
		}

		token := strings.TrimPrefix(authHeader[0], "Bearer ")
		claims, err := jwtManager.Parse(token, false)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid token !!!!!")
		}

		ctx = context.WithValue(ctx, "user_id", claims.UserID)

		return handler(ctx, req)
	}
}
