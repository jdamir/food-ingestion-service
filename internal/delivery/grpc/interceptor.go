package grpc

import (
	"context"
	auth "food-ingestion-service/internal/delivery/grpc/auth_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ContextKey string

const (
	UserIDKey  ContextKey = "user_id"
	GroupIDKey ContextKey = "group_id"
)

func AuthInterceptor(authClient auth.AuthServiceClient) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Пропускаем рефлексию Postman без авторизации
		if info.FullMethod == "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo" {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
		}

		tokens := md.Get("access_token")
		if len(tokens) == 0 || tokens[0] == "" {
			return nil, status.Errorf(codes.Unauthenticated, "access_token is required")
		}

		// Вызываем метод ValidateToken с правильным префиксом пакета auth
		authResp, err := authClient.ValidateToken(ctx, &auth.ValidateRequest{
			AccessToken: tokens[0],
		})
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "auth token invalid: %v", err)
		}

		if !authResp.IsValid {
			return nil, status.Errorf(codes.Unauthenticated, "token is no longer valid")
		}

		// Наполняем контекст настоящими ID из вашего Auth-сервиса
		ctx = context.WithValue(ctx, UserIDKey, authResp.UserId)
		ctx = context.WithValue(ctx, GroupIDKey, authResp.GroupId)

		return handler(ctx, req)
	}
}