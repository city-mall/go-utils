package middleware

import (
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func JSONLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := GetDurationInMillseconds(start)

		entry := log.WithFields(log.Fields{
			"client_ip":  GetClientIP(c),
			"duration": duration,
			"method": c.Request.Method,
			"path":   c.Request.RequestURI,
			"status": c.Writer.Status(),
			"referrer":   c.Request.Referer(),
			"request_id": c.Writer.Header().Get("Request-Id"),
		})

		if c.Writer.Status() >= 500 {
			entry.Error(c.Errors.String())
		} else {
			entry.Info("")
		}
	}
}

func GetDurationInMillseconds(start time.Time) float64 {
	end := time.Now()
	duration := end.Sub(start)
	milliseconds := float64(duration) / float64(time.Millisecond)
	rounded := float64(int(milliseconds*100+.5)) / 100
	return rounded
}

func GetClientIP(c *gin.Context) string {
	requester := c.Request.Header.Get("X-Forwarded-For")

	if len(requester) == 0 {
		requester = c.Request.Header.Get("X-Real-IP")
	}

	if len(requester) == 0 {
		requester = c.Request.RemoteAddr
	}
	if strings.Contains(requester, ",") {
		requester = strings.Split(requester, ",")[0]
	}

	return requester
}
