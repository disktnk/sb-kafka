package plugin

import (
	"github.com/disktnk/sb-kafka"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
)

func init() {
	bql.MustRegisterGlobalSourceCreator("kafka", bql.SourceCreatorFunc(kafka.NewSource))
	bql.MustRegisterGlobalSinkCreator("kafka_async", bql.SinkCreatorFunc(kafka.NewAsyncSink))
	bql.MustRegisterGlobalSinkCreator("kafka_sync", bql.SinkCreatorFunc(kafka.NewSyncSink))
}
