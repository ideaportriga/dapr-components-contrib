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

func getCustomMetadata() map[string]string {
	return map[string]string{
		"brokers":       "brokerhost:9092",
		"authType":      "krb5",
		"clientID":      "SCDX0SRVDDEV01PC",
		"consumerGroup": "cdx0-d-clients-dev01-fcg1",
		"initialOffset": "oldest",
		// "skipVerify": "true",
		"krb5ServiceName":     "kafka",
		"krb5ConfigPath":      "C:/Workspace/cdx0-middleware/keytabs/krb5.conf",
		"krb5Realm":           "{REALM}",
		"krb5UserName":        "{CLIENTID}",
		"krb5KeyTabPath":      "C:/Workspace/cdx0-middleware/keytabs/out4.keytab",
		"krb5DisablePAFXFAST": "true",
		"caCertPath":          "C:/Workspace/cdx0-middleware/keytabs/out.pem",
	}
}

func TestPublishing(t *testing.T) {
	// Comment out the following line to execute this test
	if os.Getenv("TEST_WIP") == "" {
		t.Skip("Skipping not finished test")
	}

	k := getKafka()

	t.Run("verify publishing", func(t *testing.T) {
		err := k.Init(getCustomMetadata())
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
	err := k.Init(getCustomMetadata())
	require.NoError(t, err)

	//k.AddTopicHandler("topic1", adaptHandler(handler))
	k.AddTopicHandler("topic1", func(ctx context.Context, msg *NewEvent) error {
		t.Log(string(msg.Data))
		return nil
	})
	ctx := context.Background()
	err = k.Subscribe(ctx)
	require.NoError(t, err)

	// Wait until we exit
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-sigCh
}
