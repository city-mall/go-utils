package middleware

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			log.Error(err)
		}

		c.Request.Body = ioutil.NopCloser(bytes.NewBuffer(body))

		c.Next()

		stop := time.Since(start)

		var data map[string]interface{}
		err = json.Unmarshal(body, &data)

		entry := log.WithFields(log.Fields{
			"ip":         GetClientIP(c),
			"latency":    float64(stop.Nanoseconds()) / 1000000.0, // in ms
			"method":     c.Request.Method,
			"path":       c.Request.RequestURI,
			"status":     c.Writer.Status(),
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
