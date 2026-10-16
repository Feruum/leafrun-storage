package storage

import (
	"context"
	"errors"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	"github.com/leafrun/leafrun-platform/events"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"net/url"
	"strings"
	"time"
)

func RunAuditRelay(ctx context.Context, getenv func(string) string) error {
	address, user, password := getenv("LEAFRUN_STORAGE_NATS_ADDRESS"), getenv("LEAFRUN_STORAGE_NATS_USER"), getenv("LEAFRUN_STORAGE_NATS_PASSWORD")
	parsed, err := url.Parse(address)
	if err != nil || (parsed.Scheme != "nats" && parsed.Scheme != "tls") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || strings.ContainsAny(address, " \t\r\n") || user == "" || password == "" {
		return errors.New("invalid Storage event relay configuration")
	}
	d, err := OpenDB(ctx, getenv("LEAFRUN_STORAGE_DATABASE_URL"))
	if err != nil {
		return errors.New("Storage relay database unavailable")
	}
	defer d.Pool.Close()
	if err = d.Ready(ctx); err != nil {
		return errors.New("Storage relay schema unavailable")
	}
	connection, err := nats.Connect(address, nats.UserInfo(user, password), nats.CustomInboxPrefix("_INBOX.storage"), nats.Name("leafrun-storage-relay"), nats.Timeout(5*time.Second), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		return errors.New("Storage event broker unavailable")
	}
	defer connection.Close()
	broker, err := jetstream.New(connection)
	if err != nil {
		return errors.New("Storage event broker unavailable")
	}
	publisher, err := events.NewOwnerAuditPublisher(events.JetStreamPublisher{Client: broker}, auditv1.ProducerStorage, nil)
	if err != nil {
		return err
	}
	relay := events.Relay{DB: d.Pool, Publisher: publisher}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		count, _ := relay.RunOnce(call)
		cancel()
		if count > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	return nil
}
