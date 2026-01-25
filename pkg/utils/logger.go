package utils

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	loggerSetup()
}

func loggerSetup() {
	level := os.Getenv("LOG_LEVEL")
	logLevel, err := zapcore.ParseLevel(level)
	if level == "" || err != nil {
		logLevel = zapcore.InfoLevel
	}
	var logger *zap.Logger
	if os.Getenv("DEBUG") == "true" {
		config := zap.NewDevelopmentConfig()
		if level == "" {
			config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		} else {
			config.Level = zap.NewAtomicLevelAt(logLevel)
		}
		logger, _ = config.Build()
		logger.Debug("当前为调试模式,请注意敏感信息泄漏")
	} else {
		config := zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(logLevel)
		logger, _ = config.Build()
		logger.Debug("当前为生产模式")
	}
	zap.ReplaceGlobals(logger)
}
