package auth

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type JWTManager struct {
	AccessSecret  string
	RefreshSecret string
	AccessTTL     time.Duration
	RefreshTTL    time.Duration
}

type Claims struct {
	UserID uint64 `json:"user_id"`
	jwt.RegisteredClaims
}

func (j *JWTManager) Generate(userID uint) (string, string, error) {
	now := time.Now()

	accessToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(j.AccessTTL)),
			IssuedAt:  jwt.NewNumericDate(now),
			Subject:   fmt.Sprintf("%d", userID), // ⬅️ тоже сюда
		},
		UserID: uint64(userID),
	}).SignedString([]byte(j.AccessSecret))

	if err != nil {
		return "", "", err
	}

	refreshToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(j.RefreshTTL)),
			IssuedAt:  jwt.NewNumericDate(now),
			Subject:   fmt.Sprintf("%d", userID), // ⬅️ вот это добавь
		},
		UserID: uint64(userID),
	}).SignedString([]byte(j.RefreshSecret))

	if err != nil {
		return "", "", err
	}

	return accessToken, refreshToken, nil
}

func (j *JWTManager) Parse(tokenStr string, isRefresh bool) (*Claims, error) {
	secret := j.AccessSecret
	if isRefresh {
		secret = j.RefreshSecret
	}

	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		return []byte(secret), nil
	})

	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("Invalid token")
	}

	return claims, nil

}

func (j *JWTManager) VerifyRefresh(tokenStr string) (uint64, error) {
	claims := jwt.RegisteredClaims{}
	token, err := jwt.ParseWithClaims(tokenStr, &claims, func(token *jwt.Token) (interface{}, error) {
		return []byte(j.RefreshSecret), nil
	})
	if err != nil || !token.Valid {
		return 0, fmt.Errorf("invalid token: %v", err)
	}

	userID, err := strconv.ParseUint(claims.Subject, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid user ID in token")
	}
	return userID, nil
}
