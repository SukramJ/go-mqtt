// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	mqtt "github.com/SukramJ/go-mqtt"
)

// TestPubSubQoSRoundTrip publishes and subscribes on the same connection
// at every QoS level, on both protocol versions, against both brokers.
func TestPubSubQoSRoundTrip(t *testing.T) {
	t.Parallel()
	qosLevels := []mqtt.QoS{mqtt.QoS0, mqtt.QoS1, mqtt.QoS2}

	for _, b := range brokerTable {
		for _, v := range versionTable {
			for _, q := range qosLevels {
				name := fmt.Sprintf("%s_%s_QoS%d", b.name, v, q)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					brokerAddr := brokerURL(t, b.envVar)
					c := connectClient(t, brokerAddr, v)

					topic := uniqueTopicPrefix(t) + "/roundtrip"
					coll := newMsgCollector(4)
					subRes, err := c.Subscribe(context.Background(), topic, q, coll.Handler)
					if err != nil {
						t.Fatalf("Subscribe: %v", err)
					}
					if subRes.ReasonCode.IsError() {
						t.Fatalf("Subscribe rejected: %v", subRes.ReasonCode)
					}

					payload := []byte("hello-" + name)
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := c.Publish(ctx, topic, payload, q, false); err != nil {
						t.Fatalf("Publish: %v", err)
					}

					msg := coll.Next(t, 10*time.Second)
					if msg.Topic != topic {
						t.Errorf("Topic = %q, want %q", msg.Topic, topic)
					}
					if !bytes.Equal(msg.Payload, payload) {
						t.Errorf("Payload = %q, want %q", msg.Payload, payload)
					}
					if msg.Retain {
						t.Error("Retain = true for a non-retained publish")
					}
				})
			}
		}
	}
}

// TestPubSubRetained covers the retained-message replay-on-subscribe
// behaviour on both protocol versions, plus the MQTT 5.0-only Retain
// Handling suppression option.
func TestPubSubRetained(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)

	t.Run("v3_replay", func(t *testing.T) {
		t.Parallel()
		testRetainedReplay(t, brokerAddr, mqtt.ProtocolV311)
	})
	t.Run("v5_replay", func(t *testing.T) {
		t.Parallel()
		testRetainedReplay(t, brokerAddr, mqtt.ProtocolV50)
	})
	t.Run("v5_dont_send_retained", func(t *testing.T) {
		t.Parallel()
		testRetainedSuppressed(t, brokerAddr)
	})
}

// publishRetained publishes a retained payload on topic and registers a
// best-effort cleanup that clears it again (an empty retained payload
// deletes it, §3.3.1.3) so later runs against the same long-lived broker
// don't inherit it.
func publishRetained(t *testing.T, publisher *mqtt.TCPClient, topic, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := publisher.Publish(ctx, topic, []byte(payload), mqtt.QoS1, true); err != nil {
		t.Fatalf("Publish retained: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = publisher.Publish(cctx, topic, nil, mqtt.QoS1, true)
	})
}

func testRetainedReplay(t *testing.T, brokerAddr string, v mqtt.ProtocolVersion) {
	t.Helper()
	topic := uniqueTopicPrefix(t) + "/retained"
	publisher := connectClient(t, brokerAddr, v)
	publishRetained(t, publisher, topic, "sticky")

	subscriber := connectClient(t, brokerAddr, v)
	coll := newMsgCollector(4)
	if _, err := subscriber.Subscribe(context.Background(), topic, mqtt.QoS1, coll.Handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	msg := coll.Next(t, 10*time.Second)
	if !msg.Retain {
		t.Error("Retain = false for the replayed retained message")
	}
	if string(msg.Payload) != "sticky" {
		t.Errorf("Payload = %q, want %q", msg.Payload, "sticky")
	}
}

func testRetainedSuppressed(t *testing.T, brokerAddr string) {
	t.Helper()
	topic := uniqueTopicPrefix(t) + "/retained-suppressed"
	publisher := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	publishRetained(t, publisher, topic, "sticky")

	subscriber := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	coll := newMsgCollector(4)
	if _, err := subscriber.Subscribe(context.Background(), topic, mqtt.QoS1, coll.Handler,
		mqtt.WithRetainHandling(mqtt.DontSendRetained)); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if msg, got := coll.NextOrNone(2 * time.Second); got {
		t.Fatalf("received a message despite WithRetainHandling(DontSendRetained): %+v", msg)
	}
}

// TestPubSubLargePayload round-trips a 256 KiB payload — comfortably
// inside both the client's 1 MiB default MaximumPacketSize and mosquitto/
// emqx's default limits, but large enough to exercise multi-read framing
// instead of a single syscall's worth of bytes.
func TestPubSubLargePayload(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)

	payload := make([]byte, 256*1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	c := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	topic := uniqueTopicPrefix(t) + "/large"
	coll := newMsgCollector(2)
	if _, err := c.Subscribe(context.Background(), topic, mqtt.QoS1, coll.Handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Publish(ctx, topic, payload, mqtt.QoS1, false); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := coll.Next(t, 15*time.Second)
	if len(msg.Payload) != len(payload) {
		t.Fatalf("len(Payload) = %d, want %d", len(msg.Payload), len(payload))
	}
	if !bytes.Equal(msg.Payload, payload) {
		t.Error("Payload round trip did not match byte-for-byte")
	}
}

// TestPubSubV5Properties round-trips the MQTT 5.0 PUBLISH properties this
// client exposes: ContentType, ResponseTopic, CorrelationData,
// PayloadFormatUTF8 and UserProperties. The broker MUST forward User
// Properties unaltered (§3.3.2.3.7), so an exact, order-preserving match
// is a spec requirement rather than a broker-specific quirk.
func TestPubSubV5Properties(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)

	c := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	topic := uniqueTopicPrefix(t) + "/props"
	coll := newMsgCollector(2)
	if _, err := c.Subscribe(context.Background(), topic, mqtt.QoS1, coll.Handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	wantUserProps := []mqtt.UserProperty{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.Publish(
		ctx, topic, []byte("payload"), mqtt.QoS1, false,
		mqtt.WithContentType("application/json"),
		mqtt.WithResponseTopic(topic+"/reply"),
		mqtt.WithCorrelationData([]byte("corr-1")),
		mqtt.WithUserProperties(wantUserProps...),
		mqtt.WithPayloadFormatUTF8(),
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := coll.Next(t, 10*time.Second)
	if msg.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", msg.ContentType, "application/json")
	}
	if msg.ResponseTopic != topic+"/reply" {
		t.Errorf("ResponseTopic = %q, want %q", msg.ResponseTopic, topic+"/reply")
	}
	if string(msg.CorrelationData) != "corr-1" {
		t.Errorf("CorrelationData = %q, want %q", msg.CorrelationData, "corr-1")
	}
	if !msg.PayloadFormatUTF8 {
		t.Error("PayloadFormatUTF8 = false, want true")
	}
	if !reflect.DeepEqual(msg.UserProperties, wantUserProps) {
		t.Errorf("UserProperties = %+v, want %+v", msg.UserProperties, wantUserProps)
	}
}
