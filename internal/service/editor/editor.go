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
	"github.com/google/uuid"
	"google.golang.org/grpc/status"
)

type Service struct {
	liveeditv1.UnimplementedEditorServiceServer
	Log   *zap.Logger
	DB    *gorm.DB
	JWT   *auth.JWTManager
	Redis *redis.Client
}

type Operation struct {
	Position    int32
	Text        string
	IsInsert    bool
	ClientID    string
	OperationId string // уникальный идентификатор операции
}

type DocumentSession struct {
	Content    string
	History    []*Operation        // история применённых операций
	AppliedOps map[string]struct{} // set operation_id уже применённых операций
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

// Используем user_id как clientID
func (s *Service) EditDocument(stream liveeditv1.EditorService_EditDocumentServer) error {
	s.Log.Info("EditDocument stream started")
	defer s.Log.Info("EditDocument stream closed")

	ctx := stream.Context()
	userID, ok := ctx.Value("user_id").(uint64)
	if !ok {
		return status.Error(codes.Internal, "user_id not found in context")
	}
	clientID := fmt.Sprintf("%d", userID) // теперь clientID = user_id
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
			s.Log.Info("InitSync received", zap.Uint64("document_id", payload.Init.DocumentId), zap.String("client_id", clientID))

			docID := payload.Init.DocumentId
			// clientID уже определён выше
			go s.listenEditUpdates(stream, clientID, docID)

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
				sessions[docID] = &DocumentSession{Content: document.Content, AppliedOps: make(map[string]struct{})}
			}
			sessionsMu.Unlock()

			// Отправка содержимого клиенту
			err = stream.Send(&liveeditv1.EditStreamResponse{
				Payload: &liveeditv1.EditStreamResponse_Init{
					Init: &liveeditv1.InitSync{
						DocumentId: docID,
					},
				},
			})
			if err != nil {
				s.Log.Error("failed to send InitSync", zap.Error(err))
				return status.Error(codes.Internal, "failed to send InitSync")
			}

			s.Log.Info("InitSync sent", zap.Uint64("document_id", docID), zap.String("client_id", clientID))
		case *liveeditv1.EditStreamRequest_Operation:
			op := payload.Operation
			operationId := uuid.NewString()
			sessionsMu.Lock()
			session, ok := sessions[op.DocumentId]
			if !ok {
				s.Log.Error("no session for document")
				sessionsMu.Unlock()
				return status.Error(codes.Internal, "no session for document")
			}
			// Проверка: если операция уже применялась — пропускаем
			if _, exists := session.AppliedOps[operationId]; exists {
				s.Log.Warn("operation already applied (local)", zap.String("operationId", operationId))
				sessionsMu.Unlock()
				return nil
			}
			// Логирование до применения OT
			s.Log.Info("Before applyWithOT", zap.String("content", session.Content), zap.String("op", op.Text), zap.String("clientID", clientID), zap.String("operationId", operationId))
			applyWithOT(session, &Operation{
				Position:    op.Position,
				Text:        op.Text,
				IsInsert:    op.IsInsert,
				ClientID:    clientID,
				OperationId: operationId,
			})
			session.AppliedOps[operationId] = struct{}{}
			markDirty(op.DocumentId)
			s.Log.Info("After applyWithOT", zap.String("content", session.Content), zap.String("clientID", clientID), zap.String("operationId", operationId))
			sessionsMu.Unlock()

			// --- Публикация операции в Redis ---
			_, err = s.Redis.XAdd(ctx, &redis.XAddArgs{
				Stream: fmt.Sprintf("edits:%d", op.DocumentId),
				Values: map[string]interface{}{
					"client_id":    clientID,
					"is_insert":    op.IsInsert,
					"position":     op.Position,
					"text":         op.Text,
					"operation_id": operationId,
				},
			}).Result()
			trimRedisStream(s.Redis, fmt.Sprintf("edits:%d", op.DocumentId), 1000)
			if err != nil {
				s.Log.Error("failed to publish operation to Redis", zap.Error(err))
			}
			trimRedisStream(s.Redis, fmt.Sprintf("edits:%d", op.DocumentId), 1000)

		case *liveeditv1.EditStreamRequest_Cursor:
			s.Log.Info("CursorUpdate received", zap.Int("position", int(payload.Cursor.Position)))
			cursor := payload.Cursor
			docID := cursor.DocumentId

			select {
			case docIDCh <- docID:
			default:
				s.Log.Warn("Unknown message in stream")
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
			// Ограничиваем длину стрима курсоров (например, 500 событий)
			trimRedisStream(s.Redis, fmt.Sprintf("cursors:%d", docID), 500)
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

func (s *Service) listenEditUpdates(stream liveeditv1.EditorService_EditDocumentServer, clientID string, docID uint64) {
	ctx := stream.Context()
	lastID := "$"
	streamName := fmt.Sprintf("edits:%d", docID)

	for {
		res, err := s.Redis.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamName, lastID},
			Block:   0,
			Count:   10,
		}).Result()
		if err != nil && err != context.Canceled {
			s.Log.Error("Redis XRead (edit) error", zap.Error(err))
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, xstream := range res {
			for _, message := range xstream.Messages {
				lastID = message.ID

				msgClientID := message.Values["client_id"].(string)
				operationId, _ := message.Values["operation_id"].(string)
				pos, _ := strconv.ParseInt(message.Values["position"].(string), 10, 32)
				text := message.Values["text"].(string)
				isInsert, _ := strconv.ParseBool(fmt.Sprintf("%v", message.Values["is_insert"]))

				sessionsMu.Lock()
				session, ok := sessions[docID]
				if ok {
					// Проверка: если операция уже применялась — пропускаем
					if _, exists := session.AppliedOps[operationId]; exists {
						s.Log.Warn("operation already applied (redis)", zap.String("operationId", operationId))
						sessionsMu.Unlock()
						continue
					}
					s.Log.Info("Before applyWithOT (redis)", zap.String("content", session.Content), zap.String("op", text), zap.String("clientID", msgClientID), zap.String("operationId", operationId))
					applyWithOT(session, &Operation{
						Position:    int32(pos),
						Text:        text,
						IsInsert:    isInsert,
						ClientID:    msgClientID,
						OperationId: operationId,
					})
					session.AppliedOps[operationId] = struct{}{}
					s.Log.Info("After applyWithOT (redis)", zap.String("content", session.Content), zap.String("clientID", msgClientID), zap.String("operationId", operationId))
				}
				sessionsMu.Unlock()

				err := stream.Send(&liveeditv1.EditStreamResponse{
					Payload: &liveeditv1.EditStreamResponse_Operation{
						Operation: &liveeditv1.EditOperation{
							ClientId:   msgClientID,
							Position:   int32(pos),
							Text:       text,
							IsInsert:   isInsert,
							DocumentId: docID,
						},
					},
				})
				if err != nil {
					s.Log.Error("Failed to send edit operation to client", zap.Error(err))
					return
				}
			}
		}
	}
}

// OT-трансформация операции относительно другой
func transform(op, against *Operation) *Operation {
	if op.IsInsert && against.IsInsert && against.Position <= op.Position {
		op.Position += int32(len(against.Text))
	}
	if !against.IsInsert && against.Position < op.Position {
		op.Position -= int32(len(against.Text))
		if op.Position < against.Position {
			op.Position = against.Position
		}
	}
	return op
}

// Вспомогательные функции для корректной работы с Unicode (rune-based)
func insertAt(s string, pos int32, text string) string {
	r := []rune(s)
	t := []rune(text)
	if pos < 0 || pos > int32(len(r)) {
		return s
	}
	out := append(r[:pos], append(t, r[pos:]...)...)
	return string(out)
}

func deleteAt(s string, pos int32, text string) string {
	r := []rune(s)
	t := []rune(text)
	end := pos + int32(len(t))
	if pos < 0 || end > int32(len(r)) {
		return s
	}
	for i := int32(0); i < int32(len(t)); i++ {
		if r[pos+i] != t[i] {
			return s
		}
	}
	out := append(r[:pos], r[end:]...)
	return string(out)
}

// Применение операции с OT
func applyWithOT(session *DocumentSession, op *Operation) {
	for _, prev := range session.History {
		// Не трансформируем относительно своих же операций
		if prev.ClientID == op.ClientID {
			continue
		}
		op = transform(op, prev)
	}
	if op.IsInsert {
		session.Content = insertAt(session.Content, op.Position, op.Text)
	} else {
		session.Content = deleteAt(session.Content, op.Position, op.Text)
	}
	session.History = append(session.History, op)
}

// Периодическое сохранение всех сессий в БД
func (s *Service) StartAutoSave(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sessionsMu.RLock()
			for docID, session := range sessions {
				if !isDirty(docID) {
					continue
				}
				saveMu.Lock()
				last, ok := lastSaveTime[docID]
				if ok && time.Since(last) < 2*time.Second {
					saveMu.Unlock()
					continue
				}
				err := s.DB.Model(&models.Document{}).
					Where("id = ?", docID).
					Update("content", session.Content).Error
				if err != nil {
					s.Log.Error("failed to autosave document", zap.Uint64("docID", docID), zap.Error(err))
				} else {
					setClean(docID)
					lastSaveTime[docID] = time.Now()
				}
				saveMu.Unlock()
			}
			sessionsMu.RUnlock()
			s.flushInactiveSessions() // исправлено: теперь вызывается метод сервиса
		case <-stopCh:
			return
		}
	}
}

// Сброс неактивных сессий (если все клиенты вышли)
func (s *Service) flushInactiveSessions() {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	clientsMu.Lock()
	defer clientsMu.Unlock()
	for docID := range sessions {
		if len(clientsMap[docID]) == 0 {
			delete(sessions, docID)
		}
	}
}

// dirty-флаги для документов
var dirtyMap = make(map[uint64]bool)
var dirtyMu sync.RWMutex

func markDirty(docID uint64) {
	dirtyMu.Lock()
	dirtyMap[docID] = true
	dirtyMu.Unlock()
}

func isDirty(docID uint64) bool {
	dirtyMu.RLock()
	defer dirtyMu.RUnlock()
	return dirtyMap[docID]
}

func setClean(docID uint64) {
	dirtyMu.Lock()
	dirtyMap[docID] = false
	dirtyMu.Unlock()
}

// Дебаунс автосохранения: не чаще 1 раза в 2 секунды на документ
var lastSaveTime = make(map[uint64]time.Time)
var saveMu sync.Mutex

// Ограничение истории Redis Stream (тримминг)
func trimRedisStream(rdb *redis.Client, stream string, maxLen int64) {
	_, err := rdb.XTrimMaxLen(context.Background(), stream, maxLen).Result()
	if err != nil {
		// Не критично, просто логируем
		fmt.Println("failed to trim redis stream", stream, err)
	}
}

// Вызов автосохранения (например, из main.go или при инициализации сервиса)
// Пример для main.go:
// stopCh := make(chan struct{})
// go editorService.StartAutoSave(5*time.Second, stopCh)
// defer close(stopCh)
