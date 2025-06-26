package editor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"gorm.io/gorm"

	liveeditv1 "github.com/Anabol1ks/LiveEdit/gen/proto/editor"
	"github.com/Anabol1ks/LiveEdit/internal/auth"
	"github.com/Anabol1ks/LiveEdit/internal/models"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/status"
)

type Service struct {
	liveeditv1.UnimplementedEditorServiceServer
	Log   *zap.Logger
	DB    *gorm.DB
	JWT   *auth.JWTManager
	Redis *redis.Client
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

type ClientStream struct {
	ClientID string
	Stream   liveeditv1.EditorService_EditDocumentServer
}

var clientsMap = make(map[uint64][]ClientStream) // document_id -> list of client streams
var clientsMu sync.RWMutex

func (s *Service) EditDocument(stream liveeditv1.EditorService_EditDocumentServer) error {
	s.Log.Info("EditDocument stream started")
	defer s.Log.Info("EditDocument stream closed")

	ctx := stream.Context()
	userID, ok := ctx.Value("user_id").(uint64)
	if !ok {
		return status.Error(codes.Internal, "user_id not found in context")
	}
	clientID := fmt.Sprintf("%d-%d", userID, time.Now().UnixNano())
	docIDCh := make(chan uint64, 1)

	go s.listenCursorUpdates(stream, clientID, docIDCh)

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
			clientID := payload.Init.ClientId

			clientsMu.Lock()
			clientsMap[docID] = append(clientsMap[docID], ClientStream{
				ClientID: clientID,
				Stream:   stream,
			})
			clientsMu.Unlock()

			defer func() {
				clientsMu.Lock()
				newList := []ClientStream{}
				for _, c := range clientsMap[docID] {
					if c.Stream != stream {
						newList = append(newList, c)
					}
				}
				clientsMap[docID] = newList
				clientsMu.Unlock()
				s.Log.Info("client disconnected", zap.String("client_id", clientID), zap.Uint64("doc", docID))
			}()

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

			select {
			case docIDCh <- docID:
			default:
			}
			cursorMu.Lock()
			if _, ok := cursorMap[docID]; !ok {
				cursorMap[docID] = make(map[string]int32)
			}
			cursorMap[docID][cursor.ClientId] = cursor.Position
			cursorMu.Unlock()

			_, err = s.Redis.XAdd(ctx, &redis.XAddArgs{
				Stream: fmt.Sprintf("cursors:%d", docID),
				Values: map[string]interface{}{
					"client_id": cursor.ClientId,
					"position":  cursor.Position,
				},
			}).Result()
			if err != nil {
				s.Log.Error("failed to publish cursor to Redis", zap.Error(err))
			}

			go func(cursor *liveeditv1.CursorUpdate) {
				clientsMu.RLock()
				defer clientsMu.RUnlock()

				for _, client := range clientsMap[docID] {
					if client.ClientID == cursor.ClientId {
						continue // не отсылаем отправителю
					}

					err := client.Stream.Send(&liveeditv1.EditStreamResponse{
						Payload: &liveeditv1.EditStreamResponse_Cursor{
							Cursor: cursor,
						},
					})
					if err != nil {
						s.Log.Error("failed to send cursor update", zap.String("client_id", client.ClientID), zap.Error(err))
					}
				}
			}(cursor)

			s.Log.Info("Cursor updated", zap.String("client_id", cursor.ClientId), zap.Int32("pos", cursor.Position))
			s.Log.Debug("All cursors in doc", zap.Any("doc", cursorMap[docID]))

		default:
			s.Log.Warn("Unknown message in stream")
		}
	}
}

func (s *Service) listenCursorUpdates(stream liveeditv1.EditorService_EditDocumentServer, clientID string, docIDCh <-chan uint64) {
	var docID uint64
	var initialized bool
	lastID := "$"
	ctx := stream.Context()

	for {
		if !initialized {
			select {
			case docID = <-docIDCh:
				initialized = true
				s.Log.Info("Subscribed to cursor stream", zap.Uint64("docID", docID), zap.String("clientID", clientID))
			case <-ctx.Done():
				return
			}
		}

		streamName := fmt.Sprintf("cursors:%d", docID)
		res, err := s.Redis.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamName, lastID},
			Block:   0,
			Count:   10,
		}).Result()

		if err != nil && err != context.Canceled {
			s.Log.Error("Redis XRead error", zap.Error(err))
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, xstream := range res {
			for _, message := range xstream.Messages {
				lastID = message.ID
				msgClientID := message.Values["client_id"].(string)
				if msgClientID == clientID {
					continue
				}

				pos, _ := strconv.ParseInt(message.Values["position"].(string), 10, 32)

				err := stream.Send(&liveeditv1.EditStreamResponse{
					Payload: &liveeditv1.EditStreamResponse_Cursor{
						Cursor: &liveeditv1.CursorUpdate{
							ClientId:   msgClientID,
							Position:   int32(pos),
							DocumentId: docID,
						},
					},
				})
				if err != nil {
					s.Log.Error("Failed to forward cursor to client", zap.Error(err))
					return
				}
			}
		}
	}
}
