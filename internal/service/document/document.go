package document

import (
	"context"
	"time"

	"github.com/Anabol1ks/LiveEdit/internal/auth"
	"github.com/Anabol1ks/LiveEdit/internal/models"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"gorm.io/gorm"

	liveeditv1 "github.com/Anabol1ks/LiveEdit/gen/proto/document"
)

type Service struct {
	liveeditv1.UnimplementedDocumentServiceServer
	DB  *gorm.DB
	JWT *auth.JWTManager
	Log *zap.Logger
}

func (s *Service) CreateDocument(ctx context.Context, req *liveeditv1.CreateDocumentRequest) (*liveeditv1.CreateDocumentResponse, error) {
	op := "CreateDocument"
	s.Log.Info("start", zap.String("op", op))

	if err := req.Validate(); err != nil {
		s.Log.Warn("[%s] failed", zap.String("op", op), zap.Error(err))
		return nil, err
	}

	userID, ok := ctx.Value("user_id").(uint64)
	if !ok {
		return nil, status.Error(codes.Internal, "user_id not found in context")
	}

	document := models.Document{
		Title:     req.Title,
		Content:   req.Content,
		OwnerID:   uint(userID),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := s.DB.Create(&document).Error; err != nil {
		s.Log.Error("[%s] failed", zap.String("op", op), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create document: %v", err)
	}

	access := models.DocumentAccess{
		DocumentID: document.ID,
		UserID:     uint(userID),
		Role:       "OWNER",
		CreatedAt:  time.Now(),
	}

	if err := s.DB.Create(&access).Error; err != nil {
		s.Log.Error("[%s] failed to create access", zap.String("op", op), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create document access: %v", err)
	}

	return &liveeditv1.CreateDocumentResponse{
		Id: uint64(document.ID),
	}, nil
}
func (s *Service) GetDocuments(ctx context.Context, req *liveeditv1.GetDocumentsRequest) (*liveeditv1.GetDocumentsResponse, error) {
	op := "GetDocuments"
	s.Log.Info("start", zap.String("op", op))

	userID, ok := ctx.Value("user_id").(uint64)
	if !ok {
		s.Log.Warn("[%s] user_id not found in context", zap.String("op", op))
		return nil, status.Error(codes.Unauthenticated, "user_id not found in context")
	}

	var documents []struct {
		models.Document
		Role string `gorm:"column:role"`
	}

	err := s.DB.
		Table("documents").
		Select("documents.*, da.role").
		Joins("JOIN document_accesses da ON da.document_id = documents.id").
		Where("da.user_id = ?", userID).
		Find(&documents).Error
	if err != nil {
		s.Log.Error("[%s] failed to fetch documents", zap.String("op", op), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get documents: %v", err)
	}

	items := make([]*liveeditv1.DocumentItem, len(documents))
	for i, doc := range documents {
		role, ok := liveeditv1.Role_value[doc.Role]
		if !ok {
			s.Log.Warn("[%s] unknown role for document", zap.String("op", op), zap.String("role", doc.Role))
			role = int32(liveeditv1.Role_ROLE_UNSPECIFIED)
		}

		items[i] = &liveeditv1.DocumentItem{
			Id:        uint64(doc.ID),
			Title:     doc.Title,
			Role:      liveeditv1.Role(role),
			CreatedAt: doc.CreatedAt.Format("2006-01-02T15:04:05Z"),
			UpdatedAt: doc.UpdatedAt.Format("2006-01-02T15:04:05Z"),
		}
	}

	return &liveeditv1.GetDocumentsResponse{
		Documents: items,
	}, nil
}

func (s *Service) GetDocument(ctx context.Context, req *liveeditv1.GetDocumentRequest) (*liveeditv1.GetDocumentResponse, error) {
	op := "GetDocuments"
	s.Log.Info("start", zap.String("op", op))

	userID, ok := ctx.Value("user_id").(uint64)
	if !ok {
		s.Log.Warn("[%s] user_id not found in context", zap.String("op", op))
		return nil, status.Error(codes.Unauthenticated, "user_id not found in context")
	}

	var result struct {
		models.Document
		Role string `gorm:"column:role"`
	}

	err := s.DB.
		Table("documents").
		Select("documents.*, da.role").
		Joins("JOIN document_accesses da ON da.document_id = documents.id").
		Where("documents.id = ? AND da.user_id = ?", req.Id, userID).
		First(&result).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, status.Error(codes.NotFound, "document not found or access denied")
		}
		s.Log.Error("[%s] db error", zap.String("op", op), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get document")
	}

	roleVal, ok := liveeditv1.Role_value[result.Role]
	if !ok {
		s.Log.Warn("[%s] unknown role", zap.String("op", op), zap.String("role", result.Role))
		roleVal = int32(liveeditv1.Role_ROLE_UNSPECIFIED)
	}

	return &liveeditv1.GetDocumentResponse{
		Id:        uint64(result.ID),
		Title:     result.Title,
		Content:   result.Content,
		Role:      liveeditv1.Role(roleVal),
		CreatedAt: result.CreatedAt.Format("2006-01-02T15:04:05Z"),
		UpdatedAt: result.UpdatedAt.Format("2006-01-02T15:04:05Z"),
	}, nil
}

func (s *Service) DeleteDocument(ctx context.Context, req *liveeditv1.DeleteDocumentRequest) (*emptypb.Empty, error) {
	op := "DeleteDocument"
	s.Log.Info("start", zap.String("op", op))

	userID, ok := ctx.Value("user_id").(uint64)
	if !ok {
		s.Log.Warn("[%s] user_id not found in context", zap.String("op", op))
		return nil, status.Error(codes.Unauthenticated, "user_id not found in context")
	}

	var document models.Document
	err := s.DB.
		Where("id = ? AND owner_id = ?", req.Id, userID).
		First(&document).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, status.Error(codes.NotFound, "document not found or access denied")
		}
		s.Log.Error("[%s] db error", zap.String("op", op), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get document")
	}

	if err := s.DB.Delete(&document).Error; err != nil {
		s.Log.Error("[%s] failed to delete document", zap.String("op", op), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to delete document: %v", err)
	}

	return &emptypb.Empty{}, nil
}
func (s *Service) UpdateDocument(ctx context.Context, req *liveeditv1.UpdateDocumentRequest) (*liveeditv1.UpdateDocumentResponse, error) {
	op := "UpdateDocument"
	s.Log.Info("start", zap.String("op", op))

	userID, ok := ctx.Value("user_id").(uint64)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "user_id not found in context")
	}

	var access models.DocumentAccess
	err := s.DB.Where("document_id = ? AND user_id = ?", req.DocumentId, userID).First(&access).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, status.Error(codes.PermissionDenied, "access denied")
		}
		return nil, status.Error(codes.Internal, "db error")
	}

	if access.Role != "OWNER" && access.Role != "EDITOR" {
		return nil, status.Error(codes.PermissionDenied, "insufficient permissions to update document")
	}

	// Формирование данных для обновления
	updates := map[string]interface{}{}
	if req.Title != "" {
		if len(req.Title) < 1 || len(req.Title) > 255 {
			s.Log.Warn("[%s] invalid title length", zap.String("op", op), zap.String("title", req.Title))
			return nil, status.Error(codes.InvalidArgument, "title length must be between 1 and 255 characters")
		}
		updates["title"] = req.Title
	}
	if req.Content != "" {
		updates["content"] = req.Content
	}
	if len(updates) == 0 {
		s.Log.Info("[%s] no changes detected", zap.String("op", op))
		return nil, status.Error(codes.InvalidArgument, "no changes detected")
	}

	updatedAt := time.Now()
	updates["updated_at"] = updatedAt

	err = s.DB.Model(&models.Document{}).
		Where("id = ?", req.DocumentId).
		Updates(updates).Error
	if err != nil {
		s.Log.Error("[%s] update failed", zap.String("op", op), zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to update document")
	}

	return &liveeditv1.UpdateDocumentResponse{
		UpdatedAt: updatedAt.Format("2006-01-02T15:04:05Z"),
	}, nil
}
