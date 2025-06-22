package user

import (
	"context"
	"errors"

	"github.com/Anabol1ks/LiveEdit/internal/auth"
	"github.com/Anabol1ks/LiveEdit/internal/models"
	"github.com/cockroachdb/errors/grpc/status"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"gorm.io/gorm"

	liveeditv1 "github.com/Anabol1ks/LiveEdit/gen/proto/user"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Service struct {
	liveeditv1.UnimplementedUserServiceServer
	DB  *gorm.DB
	JWT *auth.JWTManager
	Log *zap.Logger
}

func (s *Service) Register(ctx context.Context, req *liveeditv1.RegisterRequest) (*liveeditv1.AuthResponse, error) {
	op := "Register"
	s.Log.Info("start", zap.String("op", op))
	if err := req.Validate(); err != nil {
		s.Log.Warn("[%s] failed", zap.String("op", op), zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "validation failed: %v", err)
	}

	var exists models.User
	if err := s.DB.Where("email = ?", req.Email).First(&exists).Error; err == nil {
		return nil, status.Error(codes.AlreadyExists, "email already registered")
	}

	passHash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to hash password: %v", err)
	}

	user := models.User{
		Email:    req.Email,
		Password: string(passHash),
		Username: req.Username,
	}

	if err := s.DB.Create(&user).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create user: %v", err)
	}

	accessToken, refreshToken, err := s.JWT.Generate(user.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to generate tokens: %v", err)
	}

	return &liveeditv1.AuthResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

func (s *Service) Login(ctx context.Context, req *liveeditv1.LoginRequest) (*liveeditv1.AuthResponse, error) {
	op := "Login"
	s.Log.Info("start", zap.String("op", op))

	if err := req.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "validation failed: %v", err)
	}

	var user models.User
	if err := s.DB.Where("email = ?", req.Email).First(&user).Error; err != nil {
		return nil, status.Error(codes.NotFound, "invalid credentials")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	accessToken, refreshToken, err := s.JWT.Generate(user.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to generate tokens: %v", err)
	}

	return &liveeditv1.AuthResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

func (s *Service) GetProfile(ctx context.Context, _ *emptypb.Empty) (*liveeditv1.ProfileResponse, error) {
	op := "GetProfile"
	s.Log.Info("start", zap.String("op", op))

	userID, ok := ctx.Value("user_id").(uint64)
	if !ok {
		return nil, status.Error(codes.Internal, "user_id not found in context")
	}

	var user models.User
	if err := s.DB.First(&user, userID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Error(codes.Internal, "db error")
	}

	return &liveeditv1.ProfileResponse{
		Id:        uint64(user.ID),
		Email:     user.Email,
		Username:  user.Username,
		CreatedAt: user.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}, nil
}

func (s *Service) RefreshToken(ctx context.Context, req *liveeditv1.RefreshRequest) (*liveeditv1.AuthResponse, error) {
	op := "RefreshToken"
	s.Log.Info("start", zap.String("op", op))

	if req.RefreshToken == "" {
		return nil, status.Error(codes.InvalidArgument, "refresh token is required")
	}

	userID, err := s.JWT.VerifyRefresh(req.RefreshToken)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid refresh token")
	}

	var user models.User
	if err := s.DB.First(&user, userID).Error; err != nil {
		return nil, status.Error(codes.NotFound, "user not found")
	}

	newAccess, newRefresh, err := s.JWT.Generate(user.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to generate new tokens")
	}

	return &liveeditv1.AuthResponse{
		AccessToken:  newAccess,
		RefreshToken: newRefresh,
	}, nil
}
