// Package logging provides a centralized structured logging facility
// built on top of go.uber.org/zap.
package logging

import (
	"go.uber.org/zap"
)

var (
	// L is the global logger instance.
	L *zap.Logger
	// S is the global sugared logger for convenience.
	S *zap.SugaredLogger
)

func init() {
	// Default to development config; can be overridden via Init.
	L, _ = zap.NewDevelopment()
	S = L.Sugar()
}

// Init initializes the global logger with the given mode.
// When production is true a JSON-encoded, leveled logger is created;
// otherwise a human-friendly console logger is used.
func Init(production bool) {
	if production {
		L, _ = zap.NewProduction()
	} else {
		L, _ = zap.NewDevelopment()
	}
	S = L.Sugar()
}

// Sync flushes any buffered log entries. Applications should call
// this (typically via defer) before exiting.
func Sync() {
	_ = L.Sync()
}
