package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	tags "github.com/opentracing/opentracing-go/ext"
	"gitlab.360live.vn/zalopay/go-kafka-research/consumer"
	"gitlab.360live.vn/zalopay/go-kafka-research/producer"
	"gitlab.360live.vn/zpi/common/zpi-common/tracing"

	"gitlab.360live.vn/zpi/common/zpi-common/log"
)

var (
	prd    *producer.MessageDispatcher
	csm    *consumer.KafkaConsumers
	logger log.Factory
	tracer opentracing.Tracer
	runCtx context.Context
)

func main() {
	runCtx = context.Background()
	topic := "zlp-promotion-cashback"
	logger = log.NewStandardFactory("./data", "test")
	tracer = tracing.Init("be-chungtm", logger)

	command := os.Args[1]
	switch command {
	case "producer":
		StartProducer(topic)
	case "consumer":
		StartConsumer()
	default:
		fmt.Printf("Invalid command %s\n", command)
	}
	time.Sleep(100 * time.Second)
}

// StartProducer ...
func StartProducer(topic string) {

	prd, _ = producer.NewDispatcher([]string{"10.30.83.2:9092"}, sarama.NewRoundRobinPartitioner)
	go prd.Start()

	span, _ := StartSpan(runCtx, tracer, "test")
	defer span.Finish()

	c := make(map[string]string)
	tracer.Inject(span.Context(), opentracing.TextMap, opentracing.TextMapCarrier(c))

	msg := producer.Message{Topic: topic,
		Data: nil,
	}
	for headerKey, headerValue := range c {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   []byte(headerKey),
			Value: []byte(headerValue),
		})
	}

	prd.MessageChannel <- msg
}

// StartConsumer ...
func StartConsumer() {
	csm, _ = consumer.NewKafkaConsumers([]string{"10.30.83.2:9092"},
		"test-1", []string{"zlp-promotion-cashback"},
		func(message *sarama.ConsumerMessage) {
			headers := make(map[string]string)
			for _, header := range message.Headers {
				headers[string(header.Key)] = string(header.Value)
			}
			spanContext, _ := tracer.Extract(opentracing.TextMap, opentracing.TextMapCarrier(headers))
			span := tracer.StartSpan("test", opentracing.ChildOf(spanContext))
			defer span.Finish()
		}, logger)
	go csm.Start()
}

// StartSpan ...
func StartSpan(ctx context.Context, tracer opentracing.Tracer, operationName string) (opentracing.Span, context.Context) {
	var span opentracing.Span
	if span = opentracing.SpanFromContext(ctx); span != nil {
		span = tracer.StartSpan(operationName, opentracing.ChildOf(span.Context()))
		tags.SpanKindRPCClient.Set(span)
		tags.PeerService.Set(span, "worker")
	} else {
		span = tracer.StartSpan(operationName)
	}
	return span, opentracing.ContextWithSpan(context.Background(), span)
}
