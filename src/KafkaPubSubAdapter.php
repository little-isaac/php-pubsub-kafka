<?php

namespace milind\PubSub\Kafka;

use milind\PubSub\PubSubAdapterInterface;
use milind\PubSub\Utils;

class KafkaPubSubAdapter implements PubSubAdapterInterface {

    /**
     * @var \RdKafka\Producer
     */
    protected $producer;

    /**
     * @var \RdKafka\KafkaConsumer
     */
    protected $kafkaConsumer;

    /**
     * @var \RdKafka\consumer
     */
    protected $consumer;

    const FLUSH_ERROR_MESSAGE = 'librdkafka unable to perform flush, messages might be lost';

    /**
     * @param \RdKafka\Producer $producer
     * @param \RdKafka\KafkaConsumer $kafkaConsumer
     */
    public function __construct(\RdKafka\Producer $producer = null, \RdKafka\KafkaConsumer $kafkaConsumer = null, \RdKafka\Consumer $consumer = null) {
        $this->producer = $producer;
        $this->kafkaConsumer = $kafkaConsumer;
        $this->consumer = $consumer;
    }

    /**
     * Return the Kafka producer.
     *
     * @return \RdKafka\Producer
     */
    public function getProducer() {
        return $this->producer;
    }

    /**
     * Return the Kafka kafkaConsumer.
     *
     * @return \RdKafka\KafkaConsumer
     */
    public function getKafkaConsumer() {
        return $this->kafkaConsumer;
    }

    /**
     * Subscribe a handler to a channel.
     *
     * @param string $channel
     * @param callable $handler
     *
     * @throws \Exception
     */
    public function subscribe($channel, callable $handler, $extraConfig = null) {
        $this->kafkaConsumer->subscribe([$channel]);

        $isSubscriptionLoopActive = true;

        while ($isSubscriptionLoopActive) {
            $message = $this->kafkaConsumer->consume(120 * 1000);

            if ($message === null) {
                continue;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $payload = Utils::unserializeMessagePayload($message->payload);

                    if ($payload === 'unsubscribe') {
                        $isSubscriptionLoopActive = false;
                    } else {
                        call_user_func($handler, $payload);
                    }

                    $this->kafkaConsumer->commitAsync($message);

                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
            }
        }
    }

    /**
     * Consume a handler to a channel.
     *
     * @param string $channel
     * @param callable $handler
     *
     * @throws \Exception
     */
    public function consume($channel, $partition, $topicConf, callable $handler, $extraConfig = null) {
        $topic = $this->consumer->newTopic($channel, $topicConf);
        $topic->consumeStart($partition, RD_KAFKA_OFFSET_STORED);

        $isSubscriptionLoopActive = true;

        while ($isSubscriptionLoopActive) {
            $message = $topic->consume($partition, 120 * 1000);

            if ($message === null) {
                continue;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $payload = Utils::unserializeMessagePayload($message->payload);

                    if ($payload === 'unsubscribe') {
                        $isSubscriptionLoopActive = false;
                    } else {
                        call_user_func($handler, $payload);
                    }

//                    $this->consumer->commitAsync($message);

                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
            }
        }
    }

    /**
     * Publish a message to a channel.
     *
     * @param string $channel
     * @param mixed $message
     */
    public function publish($channel, $message, $extraConfig = null) {

        $topic = $this->producer->newTopic($channel);

        $topic->produce(RD_KAFKA_PARTITION_UA, 0, Utils::serializeMessage($message));

        $this->producer->poll(0);

        $this->flush();
    }

    /**
     * librdkafka flush waits for all outstanding producer requests to be handled.
     * It ensures messages produced properly.
     *
     * @param int $timeout "timeout in milliseconds"
     * @return void
     */
    protected function flush(int $timeout = 10000) {
        $result = $this->producer->flush($timeout);

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new \Exception(self::FLUSH_ERROR_MESSAGE);
        }
    }

    /**
     * Publish multiple messages to a channel.
     *
     * @param string $channel
     * @param array $messages
     */
    public function publishBatch($channel, array $messages, $extraConfig = null) {
        foreach ($messages as $message) {
            $this->publish($channel, $message, $extraConfig);
        }
    }

}
