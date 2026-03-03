package source

import (
	"log/slog"

	app "github.com/lib4u/fake-useragent"
)

func NewUserAgent(logger *slog.Logger) *app.UserAgent {
	ua, err := app.New()
	if err != nil {
		logger.Error("Failed to initialize user agent", "err", err)
		return nil
	}
	return ua
}

func GetRandomUserAgent(ua *app.UserAgent) string {
	if ua != nil {
		return ua.GetRandom()
	}
	return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}
