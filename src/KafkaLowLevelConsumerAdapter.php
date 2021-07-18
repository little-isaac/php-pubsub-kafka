<?php

namespace milind\PubSub\Kafka;

use milind\PubSub\SubscriberAdapterInterface;
use milind\PubSub\Utils;

class KafkaLowLevelConsumerAdapter implements SubscriberAdapterInterface {

    /**
     * @var \RdKafka\consumer
     */
    protected $consumer;

    /**
     * @var \RdKafka\ConsumerTopic
     */
    protected $topic;
    
    /**
     * @var Integer
     */
    protected $partition;
    
    /**
     * @param \RdKafka\Consumer $consumer
     */
    public function __construct(\RdKafka\Consumer $consumer) {
        $this->consumer = $consumer;
    }

    /**
     * Return the Kafka consumer.
     *
     * @return \RdKafka\Consumer
     */
    public function getConsumer() {
        return $this->consumer;
    }

    /**
     * Subscribe a handler to a channel.
     *
     * @param string $channel
     * @param callable $handler
     *
     * @throws \Exception
     */
    public function subscribe($channel, callable $handler, $extraConf = null) {

        $topicConf = $extraConf['topicConf'] ?? new \RdKafka\TopicConf();
        $this->partition = $extraConf['partition'] ?? RD_KAFKA_PARTITION_UA;

        $consumeTimeout = $extraConf['consumeTimeout'] ?? 120 * 1000;
        $isExitOnTimeout = $extraConf['isExitOnTimeout'] ?? true;

        $this->topic = $this->consumer->newTopic($channel, $topicConf);
        $this->topic->consumeStart($this->partition, RD_KAFKA_OFFSET_STORED);

        $isSubscriptionLoopActive = true;

        while ($isSubscriptionLoopActive) {

            $message = $this->topic->consume($this->partition, $consumeTimeout);
            if ($message === null) {
                // If message is null then it means timeout 
                // Please check document for this.
                // https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/rdkafka-consumertopic.consume.html
                if ($isExitOnTimeout) {
                    $isSubscriptionLoopActive = false;
                }
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
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    if ($isExitOnTimeout) {
                        $isSubscriptionLoopActive = false;
                    }
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
            }
        }
        $this->unsubscribe();
    }
    
    public function unsubscribe() {
        $this->topic->consumeStop($this->partition);
        return true;
    }

}
