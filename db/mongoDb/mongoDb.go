package mongoDb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"log"

	zeroLog "github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Client struct {
	db       *mongo.Client
	isLoaded bool
	GO_ENV   string
	DB_URL   string
}

var (
	dbInstance Client
)

func Connect(config Client) {
	if dbInstance.isLoaded {
		return
	}
	var (
		client *mongo.Client
		err    error
	)
	if config.GO_ENV == "production" {
		caFile := "rds-combined-ca-bundle.pem"
		tlsConfig, err := getCustomTLSConfig(caFile)
		if err != nil {
			zeroLog.Panic().Err(err).Msg(err.Error())
		}
		client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(config.DB_URL).SetTLSConfig(tlsConfig))
	} else {
		client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(config.DB_URL))
	}

	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(context.TODO(), readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}

	dbInstance.db = client
	dbInstance.isLoaded = true

	zeroLog.Info().Msgf("MongoDB Connection Established")
}

func getCustomTLSConfig(caFile string) (*tls.Config, error) {
	tlsConfig := new(tls.Config)
	certs, err := ioutil.ReadFile(caFile)

	if err != nil {
		return tlsConfig, err
	}

	tlsConfig.RootCAs = x509.NewCertPool()
	ok := tlsConfig.RootCAs.AppendCertsFromPEM(certs)

	if !ok {
		return tlsConfig, errors.New("Failed parsing pem file")
	}

	return tlsConfig, nil
}

func GetDb() (*mongo.Client, error) {
	if !dbInstance.isLoaded {
		err := errors.New("MongoDB Connection not Established")
		zeroLog.Error().Err(err).Msg("")
		return nil, err
	}
	return dbInstance.db, nil
}

func Close() {
	dbInstance.db.Disconnect(context.TODO())
}
