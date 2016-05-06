package kafka

import (
	"fmt"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"

	"github.com/Shopify/sarama"
)

var (
	retryMaxPath     = data.MustCompilePath("retry_max")
	requiredAcksPath = data.MustCompilePath("required_acks")
	brokersPath      = data.MustCompilePath("brokers")
	topicPath        = data.MustCompilePath("topic")
	keyPath          = data.MustCompilePath("key")
	valuePath        = data.MustCompilePath("value")
)

// NewAsyncSink returns a sink set asynchronized producer.
//
// retry_max: default to 3
//
// required_ackes: default to "wait_for_all"
//
// brokers: default to ["loacalhost:9092"]
func NewAsyncSink(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (
	core.Sink, error) {
	config, err := getProducerConfig(params)
	if err != nil {
		return nil, err
	}
	brokers, err := getBrokerAddrs(params)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &asyncSink{
		producer: producer,
	}, nil
}

// NewSyncSink returns a sink set synchronized producer.
//
// retry_max: default to 3
//
// required_ackes: default to "wait_for_all"
//
// brokers: default to ["loacalhost:9092"]
func NewSyncSink(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (
	core.Sink, error) {
	config, err := getProducerConfig(params)
	if err != nil {
		return nil, err
	}
	brokers, err := getBrokerAddrs(params)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &syncSink{
		producer: producer,
	}, nil
}

func getProducerConfig(params data.Map) (*sarama.Config, error) {
	retryMax := 3
	if rm, err := params.Get(retryMaxPath); err == nil {
		max, err := data.AsInt(rm)
		if err != nil {
			return nil, err
		}
		retryMax = int(max)
	}

	requiredAcks := sarama.WaitForAll
	if ra, err := params.Get(requiredAcksPath); err == nil {
		mode, err := data.AsString(ra)
		if err != nil {
			return nil, err
		}
		switch mode {
		case "wait_for_local":
			requiredAcks = sarama.WaitForLocal
		case "wait_for_all":
			requiredAcks = sarama.WaitForAll
		}
	}

	config := sarama.NewConfig()
	config.Producer.Retry.Max = retryMax
	config.Producer.RequiredAcks = requiredAcks

	return config, nil
}

func getBrokerAddrs(params data.Map) ([]string, error) {
	brokers := []string{"localhost:9092"}
	if bs, err := params.Get(brokersPath); err == nil {
		brs, err := data.AsArray(bs)
		if err != nil {
			return nil, err
		}
		addrs := []string{}
		for _, br := range brs {
			addr, err := data.AsString(br)
			if err != nil {
				return nil, err
			}
			addrs = append(addrs, addr)
		}
		brokers = addrs
	}
	return brokers, nil
}

type asyncSink struct {
	producer sarama.AsyncProducer
}

func (s *asyncSink) Write(ctx *core.Context, t *core.Tuple) error {
	topic := ""
	if tp, err := t.Data.Get(topicPath); err != nil {
		return err
	} else if topic, err = data.AsString(tp); err != nil {
		return err
	}

	key, err := t.Data.Get(keyPath)
	if err != nil {
		return err
	}
	keyEn, err := convertToSaramaEncoder(key)
	if err != nil {
		return err
	}

	value, err := t.Data.Get(valuePath)
	if err != nil {
		return err
	}
	valueEn, err := convertToSaramaEncoder(value)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEn,
		Value: valueEn,
	}
	s.producer.Input() <- msg
	// TODO need to catch producer's error
	// <-s.producer.Errors()
	return nil
}

func (s *asyncSink) Close(ctx *core.Context) error {
	return s.producer.Close()
}

type syncSink struct {
	producer sarama.SyncProducer
}

func (s *syncSink) Write(ctx *core.Context, t *core.Tuple) error {
	topic := ""
	if tp, err := t.Data.Get(topicPath); err != nil {
		return err
	} else if topic, err = data.AsString(tp); err != nil {
		return err
	}

	value, err := t.Data.Get(valuePath)
	if err != nil {
		return err
	}
	valueEn, err := convertToSaramaEncoder(value)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: valueEn,
	}
	partition, offset, err := s.producer.SendMessage(msg)
	if err != nil {
		return err
	}
	// TODO logging is not appropriate
	ctx.Log().Infof("message is stored in partition: %d, offset %d", partition,
		offset)
	return nil
}

func (s *syncSink) Close(ctx *core.Context) error {
	return s.producer.Close()
}

func convertToSaramaEncoder(v data.Value) (sarama.Encoder, error) {
	switch v.Type() {
	case data.TypeString:
		vs, _ := data.AsString(v)
		return sarama.StringEncoder(vs), nil
	case data.TypeBlob:
		vb, _ := data.AsBlob(v)
		return sarama.ByteEncoder(vb), nil
	default:
		// TODO required to support other data type
		return nil, fmt.Errorf("%s is not supported", v.Type())
	}
}
