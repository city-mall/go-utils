package postgres

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/jackc/pgx/v4/pgxpool"
	zeroLog "github.com/rs/zerolog/log"
)

type Client struct {
	db       *pgxpool.Pool
	isLoaded bool
	GO_ENV   string
	DB_URL   string
}

var (
	dbInstance Client
	dbConn     sync.Once
)

func Connect(config Client) {
	dbConn.Do(func() {
		db, err := pgxpool.Connect(context.Background(), config.DB_URL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
			log.Fatal(err)
		}

		zeroLog.Info().Msgf("DB Successfully Connected")
		dbInstance.db = db
		dbInstance.isLoaded = true
	})
}

func GetDb() (*pgxpool.Pool, error) {
	if !dbInstance.isLoaded {
		err := errors.New("MongoDB Connection not Established")
		zeroLog.Error().Err(err).Msg("")
		return nil, err
	}
	return dbInstance.db, nil
}

func Close() {
	dbInstance.db.Close()
}
