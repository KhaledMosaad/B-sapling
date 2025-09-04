package logger

import (
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	// Create log directory if it doesn't exist
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		// Fallback to current directory if can't create logs dir
		logDir = "."
	}

	// Create log file with absolute path
	logPath := filepath.Join(logDir, "app.log")
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// Fallback to stderr if file creation fails
		file = os.Stderr
		log.Error().Err(err).Msg("Failed to create log file, using stderr")
	}

	// Create console writer with custom formatting
	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "2006-01-02 15:04:05",
		NoColor:    false, // Set to true for production
	}

	// Create multi-writer for both file and console
	multiWriter := zerolog.MultiLevelWriter(
		file,          // JSON logs to file
		consoleWriter, // Pretty logs to console
	)

	// Configure global logger
	log.Logger = zerolog.New(multiWriter).
		With().
		Timestamp().
		Str("service", "b-sapling").
		Logger()

	// Set log level from environment variable
	zerolog.SetGlobalLevel(zerolog.TraceLevel) // Default to Info

	// Log initialization status
	log.Info().
		Str("log_file", logPath).
		Str("log_level", zerolog.GlobalLevel().String()).
		Msg("Logger initialized")

}
