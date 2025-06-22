package main

import (
	"github.com/Anabol1ks/LiveEdit/internal/config"
	"github.com/Anabol1ks/LiveEdit/internal/db"
	"github.com/Anabol1ks/LiveEdit/internal/logger"
)

func main() {
	cfg := config.Load()

	if err := logger.Init(cfg.AppEnv); err != nil {
		panic(err)
	}

	log := logger.L()
	log.Info("Logger initialized successfully")

	db.ConnectDB(cfg, log)
}
