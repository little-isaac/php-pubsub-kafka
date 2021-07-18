<?php

namespace milind\PubSub\Kafka;

use milind\PubSub\SubscriberAdapterInterface;
use milind\PubSub\Utils;

class KafkaHighLevelConsumerAdapter implements SubscriberAdapterInterface {

    /**
     * @var \RdKafka\KafkaConsumer
     */
    protected $consumer;

    /**
     * @param \RdKafka\Consumer $consumer
     */
    public function __construct(\RdKafka\KafkaConsumer $consumer) {
        $this->consumer = $consumer;
    }

    /**
     * Return the Kafka kafkaConsumer.
     *
     * @return \RdKafka\KafkaConsumer
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

        $consumeTimeout = $extraConf['consumeTimeout'] ?? 120 * 1000;
        $isExitOnTimeout = $extraConf['isExitOnTimeout'] ?? true;
        $this->consumer->subscribe([$channel]);

        $isSubscriptionLoopActive = true;
        while ($isSubscriptionLoopActive) {
            $message = $this->consumer->consume($consumeTimeout);
            
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

                    $this->consumer->commitAsync($message);

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

    public function unsubscribe(){
        $this->consumer->unsubscribe();
        return true;
    }
    
}
