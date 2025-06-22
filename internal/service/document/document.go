package document

import (
	"context"
	"time"

	"github.com/Anabol1ks/LiveEdit/internal/auth"
	"github.com/Anabol1ks/LiveEdit/internal/models"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
