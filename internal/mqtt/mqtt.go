package mqtt

import (
	"context"
	"encoding/json"
	"log"

	"inwsoft.com/queuerator/internal/config"

	mqttlib3 "github.com/eclipse/paho.mqtt.golang"
)

type MQTT3DataSourceConfig struct {
	Url      string               `json:"url"`
	Criteria config.CriteriaGroup `json:"criteria"`
	ClientId string               `json:"clientId"`
	Topics   []string             `json:"topics"`
}

var _ config.DataSourceConfig = (*MQTT3DataSourceConfig)(nil)

func (c MQTT3DataSourceConfig) CreateDataSource() (config.DataSource, error) {
	return mqtt3DataSource{
		url:      c.Url,
		criteria: c.Criteria,
		clientId: c.ClientId,
		topics:   c.Topics,
	}, nil
}

type mqtt3DataSource struct {
	url      string
	criteria config.CriteriaGroup
	clientId string
	topics   []string
}

var _ config.DataSource = mqtt3DataSource{}

func (src mqtt3DataSource) Connect(ctx context.Context) error {
	l := log.Default()
	l.SetFlags(log.LstdFlags | log.Lmicroseconds)
	opts := mqttlib3.NewClientOptions()
	opts = opts.AddBroker(src.url)
	opts = opts.SetClientID(src.clientId)

	// close := make(chan int)
	opts.SetConnectionNotificationHandler(func(c mqttlib3.Client, notification mqttlib3.ConnectionNotification) {
		switch n := notification.(type) {
		case mqttlib3.ConnectionNotificationConnected:
			l.Printf("[NOTIFICATION] connected\n")
			for _, topic := range src.topics {
				token := c.Subscribe(topic, 0, nil)
				token.Wait()
				if token.Error() != nil {
					l.Printf("failed to subscribe to topic \"%s\"\n", topic)
					panic(token.Error())
				} else {
					l.Printf("subscribed to topic \"%s\"", topic)
				}
			}

		case mqttlib3.ConnectionNotificationConnecting:
			l.Printf("[NOTIFICATION] connecting (isReconnect=%t) [%d]\n", n.IsReconnect, n.Attempt)
		case mqttlib3.ConnectionNotificationFailed:
			l.Printf("[NOTIFICATION] connection failed: %v\n", n.Reason)
		case mqttlib3.ConnectionNotificationLost:
			l.Printf("[NOTIFICATION] connection lost: %v\n", n.Reason)
		case mqttlib3.ConnectionNotificationBroker:
			l.Printf("[NOTIFICATION] broker connection: %s\n", n.Broker.String())
		case mqttlib3.ConnectionNotificationBrokerFailed:
			l.Printf("[NOTIFICATION] broker connection failed: %v [%s]\n", n.Reason, n.Broker.String())
		}
	})

	queue := make(chan []byte)
	opts.SetDefaultPublishHandler(func(c mqttlib3.Client, msg mqttlib3.Message) {
		queue <- msg.Payload()
	})

	c := mqttlib3.NewClient(opts)
	token := c.Connect()
	token.Wait()
	if token.Error() != nil {
		panic(token.Error())
	}

	for {
		select {
		case <-ctx.Done():
			c.Disconnect(250)

		case data := <-queue:
			var jsonData map[string]any
			err := json.Unmarshal(data, &jsonData)
			if err != nil {
				l.Printf("mqtt err: %s\n", err.Error())
				continue
			}

			res := src.criteria.Evaluate(jsonData)
			l.Printf("MQTT Result: %t\n", res)
		}
	}
}
