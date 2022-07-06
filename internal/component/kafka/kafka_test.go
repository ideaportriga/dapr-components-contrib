/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// var (
// 	clientCertPemMock = `-----BEGIN CERTIFICATE-----
// Y2xpZW50Q2VydA==
// -----END CERTIFICATE-----`
// 	clientKeyMock = `-----BEGIN RSA PRIVATE KEY-----
// Y2xpZW50S2V5
// -----END RSA PRIVATE KEY-----`
// 	caCertMock = `-----BEGIN CERTIFICATE-----
// Y2FDZXJ0
// -----END CERTIFICATE-----`
// )

func getMetadata() map[string]string {
	return map[string]string{
		"brokers":       "myiprdapr.servicebus.windows.net:9093",
		"authType":      "password",
		"saslUsername":  "$ConnectionString",
		"saslPassword":  "Endpoint=sb://myiprdapr.servicebus.windows.net/;SharedAccessKeyName=test_key;SharedAccessKey=9LRKEaAc6AyHPgHxEvo0mw8QtKqtRRnjTFuUcUrhGBU=;EntityPath=topic1",
		"consumerGroup": "testConsumerGroup",
		"initialOffset": "oldest",
		// "authRequired": "true",
		// "skipVerify": "true",
		// "version": "1.0.0",
	}
}

func TestPublishing(t *testing.T) {
	// Comment out the following line to execute this test
	if os.Getenv("TEST_WIP") == "" {
		t.Skip("Skipping not finished test")
	}

	k := getKafka()

	t.Run("verify publishing", func(t *testing.T) {
		err := k.Init(getMetadata())
		require.NoError(t, err)
		for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
			err := k.Publish("topic1", []byte(word), nil)
			require.NoError(t, err)
		}
	})
}

func TestConsuming(t *testing.T) {
	// Comment out the following line to execute this test
	if os.Getenv("TEST_WIP") == "" {
		t.Skip("Skipping not finished test")
	}

	k := getKafka()
	err := k.Init(getMetadata())
	require.NoError(t, err)

	//k.AddTopicHandler("topic1", adaptHandler(handler))
	k.AddTopicHandler("topic1", func(ctx context.Context, msg *NewEvent) error {
		return nil
	})
	ctx := context.Background()
	err = k.Subscribe(ctx)
	require.NoError(t, err)

	// t.Run("verify consuming", func(t *testing.T) {

	// 	// k.Subscribe("topic1", func(msg *sarama.ConsumerMessage) {
	// 	// 	t.Logf("Received message: %s", string(msg.Value))
	// 	// }

	// 	// for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
	// 	// 	err := k.Publish("topic1", []byte(word), nil)
	// 	// 	require.Nil(t, err)
	// 	// }

	// 	go func() {
	// 		// Wait for context cancelation
	// 		select {
	// 		case <-ctx.Done():
	// 		case <-p.subscribeCtx.Done():
	// 		}

	// 		// Remove the topic handler before restarting the subscriber
	// 		k.RemoveTopicHandler(req.Topic)

	// 		// If the component's context has been canceled, do not re-subscribe
	// 		if p.subscribeCtx.Err() != nil {
	// 			return
	// 		}

	// 		err := k.Subscribe(ctx)
	// 		if err != nil {
	// 			p.logger.Errorf("kafka pubsub: error re-subscribing: %v", err)
	// 		}
	// 	}()

	// 	return k.Subscribe(ctx)

	// })

	// ah := adaptHandler(handler)
	// for _, t := range b.topics {
	// 	k.AddTopicHandler(t, ah)
	// }
	// ah := adaptHandler(handler)
	// k.AddTopicHandler("topic1", ah)

	// Subscribe, in a background goroutine
	// err := k.Subscribe(ctx)
	// if err != nil {
	// 	return err
	// }

	// Wait until we exit
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sigCh

}
