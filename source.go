package kafka

import (
	"github.com/Shopify/sarama"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
	"time"
)

var (
	returnErrorsPath = data.MustCompilePath("return_errors")
	partitionPath    = data.MustCompilePath("partition")
)

// NewSource returns a source set consumer.
//
// return_errors: default to true
//
// brokers: default to ["localhost:9092"]
//
// topic: required
//
// partition: default to 0
func NewSource(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (
	core.Source, error) {
	returnErrors := true
	if re, err := params.Get(returnErrorsPath); err == nil {
		if returnErrors, err = data.AsBool(re); err != nil {
			return nil, err
		}
	}
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = returnErrors
	brokers, err := getBrokerAddrs(params)
	if err != nil {
		return nil, err
	}

	topic := ""
	if tp, err := params.Get(topicPath); err != nil {
		return nil, err
	} else if topic, err = data.AsString(tp); err != nil {
		return nil, err
	}
	partition := int32(0)
	if pt, err := params.Get(partitionPath); err == nil {
		partID, err := data.AsInt(pt)
		if err != nil {
			return nil, err
		}
		partition = int32(partID)
	}

	rootConsumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}
	consumer, err := rootConsumer.ConsumePartition(topic, partition,
		sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}

	s := &source{
		topic:    topic,
		consumer: consumer,
	}
	return core.ImplementSourceStop(s), nil
}

type source struct {
	topic    string
	consumer sarama.PartitionConsumer
}

func (s *source) GenerateStream(ctx *core.Context, w core.Writer) error {
	// TODO need to catch consumer's error
	// <-s.consumer.Errors()
	msg := <-s.consumer.Messages()
	now := time.Now()
	t := &core.Tuple{
		ProcTimestamp: now,
		Timestamp:     now,
	}
	t.Data = data.Map{
		"topic": data.String(s.topic),
		"key":   data.Blob(msg.Key),
		"value": data.Blob(msg.Value),
	}
	return w.Write(ctx, t)
}

func (s *source) Stop(ctx *core.Context) error {
	return s.consumer.Close()
}
