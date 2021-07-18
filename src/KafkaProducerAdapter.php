<?php

namespace milind\PubSub\Kafka;

use milind\PubSub\PublisherAdapterInterface;
use milind\PubSub\Utils;

class KafkaProducerAdapter implements PublisherAdapterInterface {

    /**
     * @var \RdKafka\Producer
     */
    protected $producer;

    
    const FLUSH_ERROR_MESSAGE = 'librdkafka unable to perform flush, messages might be lost';

    /**
     * @param \RdKafka\Producer $producer
     * @param \RdKafka\KafkaConsumer $kafkaConsumer
     */
    public function __construct(\RdKafka\Producer $producer) {
        $this->producer = $producer;
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
     * Publish a message to a channel.
     *
     * @param string $channel
     * @param mixed $message
     */
    public function publish($channel, $message, $extraConfig = null) {
        
        $topic = $this->producer->newTopic($channel);
        
        $pollTimeout = $extraConfig['pollTimeout'] ?? 0;
        $flushTimeout = $extraConfig['flushTimeout'] ?? 1000;
        $partition = $extraConfig['partition'] ?? RD_KAFKA_PARTITION_UA;
        $msgsFlags = $extraConfig['msgFlags'] ?? 0;
        $key = $extraConfig['key'] ?? null;
        echo "Publishing to '$channel' - '$partition' -  '$key' \n";
        $topic->produce($partition, $msgsFlags, Utils::serializeMessage($message),$key);
        
        
        $this->producer->poll($pollTimeout);

        $this->flush($flushTimeout);
    }

    /**
     * librdkafka flush waits for all outstanding producer requests to be handled.
     * It ensures messages produced properly.
     *
     * @param int $timeout "timeout in milliseconds"
     * @return void
     */
    protected function flush(int $timeout = 1000) {
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
