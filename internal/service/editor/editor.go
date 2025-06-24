package editor

import (
	"errors"
	"io"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"gorm.io/gorm"

	liveeditv1 "github.com/Anabol1ks/LiveEdit/gen/proto/editor"
	"github.com/Anabol1ks/LiveEdit/internal/auth"
	"github.com/Anabol1ks/LiveEdit/internal/models"
	"google.golang.org/grpc/status"
)

type Service struct {
	liveeditv1.UnimplementedEditorServiceServer
	Log *zap.Logger
	DB  *gorm.DB
	JWT *auth.JWTManager
}

type DocumentSession struct {
	Content string
}

var sessions = make(map[uint64]*DocumentSession)
var sessionsMu sync.RWMutex

type CursorPosition struct {
	ClientID string
	Position int32
}

var cursorMap = make(map[uint64]map[string]int32) // document_id -> client_id -> position
var cursorMu sync.RWMutex

func (s *Service) EditDocument(stream liveeditv1.EditorService_EditDocumentServer) error {
	s.Log.Info("EditDocument stream started")
	defer s.Log.Info("EditDocument stream closed")

	ctx := stream.Context()
	userID, ok := ctx.Value("user_id").(uint64)
	if !ok {
		return status.Error(codes.Internal, "user_id not found in context")
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			s.Log.Error("stream receive error", zap.Error(err))
			return err
		}

		switch payload := req.Payload.(type) {
		case *liveeditv1.EditStreamRequest_Init:
			s.Log.Info("InitSync received", zap.Uint64("document_id", payload.Init.DocumentId))

			docID := payload.Init.DocumentId

			// Проверка доступа
			var access models.DocumentAccess
			if err := s.DB.Where("document_id = ? AND user_id = ?", docID, userID).First(&access).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return status.Error(codes.PermissionDenied, "no access to document")
				}
				s.Log.Error("failed to check access", zap.Error(err))
				return status.Error(codes.Internal, "failed to check document access")
			}

			// Загрузка содержимого документа
			var document models.Document
			if err := s.DB.First(&document, docID).Error; err != nil {
				s.Log.Error("failed to load document", zap.Error(err))
				return status.Error(codes.Internal, "failed to load document")
			}

			sessionsMu.Lock()
			if _, ok := sessions[docID]; !ok {
				sessions[docID] = &DocumentSession{Content: document.Content}
			}
			sessionsMu.Unlock()

			// Отправка содержимого клиенту
			err = stream.Send(&liveeditv1.EditStreamResponse{
				Payload: &liveeditv1.EditStreamResponse_Init{
					Init: &liveeditv1.InitSync{
						DocumentId: docID,
						// Дополнительно можно вернуть начальный текст
						// или метаданные, если надо
					},
				},
			})
			if err != nil {
				s.Log.Error("failed to send InitSync", zap.Error(err))
				return status.Error(codes.Internal, "failed to send InitSync")
			}

			s.Log.Info("InitSync sent", zap.Uint64("document_id", docID))
		case *liveeditv1.EditStreamRequest_Operation:
			s.Log.Info("EditOperation received", zap.String("text", payload.Operation.Text))
			op := payload.Operation
			sessionsMu.Lock()
			session, ok := sessions[op.DocumentId]
			if !ok {
				s.Log.Error("no session for document")
				sessionsMu.Unlock()
				return status.Error(codes.Internal, "no session for document")
			}

			if op.IsInsert {
				if op.Position < 0 || op.Position > int32(len(session.Content)) {
					sessionsMu.Unlock()
					return status.Error(codes.InvalidArgument, "invalid position")
				}
				session.Content = session.Content[:op.Position] + op.Text + session.Content[op.Position:]
			} else {
				end := op.Position + int32(len(op.Text))
				if op.Position < 0 || end > int32(len(session.Content)) || session.Content[op.Position:end] != op.Text {
					sessionsMu.Unlock()
					return status.Error(codes.InvalidArgument, "invalid deletion")
				}
				session.Content = session.Content[:op.Position] + session.Content[end:]
			}
			s.Log.Info("operation applied", zap.String("content", session.Content))
			sessionsMu.Unlock()

		case *liveeditv1.EditStreamRequest_Cursor:
			s.Log.Info("CursorUpdate received", zap.Int("position", int(payload.Cursor.Position)))
			cursor := payload.Cursor
			docID := cursor.DocumentId

			cursorMu.Lock()
			if _, ok := cursorMap[docID]; !ok {
				cursorMap[docID] = make(map[string]int32)
			}
			cursorMap[docID][cursor.ClientId] = cursor.Position
			cursorMu.Unlock()

			err := stream.Send(&liveeditv1.EditStreamResponse{
				Payload: &liveeditv1.EditStreamResponse_Cursor{
					Cursor: cursor,
				},
			})
			if err != nil {
				s.Log.Error("failed to send CursorUpdate", zap.Error(err))
				return status.Error(codes.Internal, "failed to send CursorUpdate")
			}

			s.Log.Info("Cursor updated", zap.String("client_id", cursor.ClientId), zap.Int32("pos", cursor.Position))
			s.Log.Debug("All cursors in doc", zap.Any("doc", cursorMap[docID]))

		default:
			s.Log.Warn("Unknown message in stream")
		}
	}
}
