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

    protected $ERRORS = [
        'RD_KAFKA_RESP_ERR__BEGIN' => 'RD_KAFKA_RESP_ERR__BEGIN',
        'RD_KAFKA_RESP_ERR__BAD_MSG' => 'Local:Bad message format',
        'RD_KAFKA_RESP_ERR__BAD_COMPRESSION' => 'Local:Invalid compressed data',
        'RD_KAFKA_RESP_ERR__DESTROY' => 'Local:Broker handle destroyed',
        'RD_KAFKA_RESP_ERR__FAIL' => 'Local:Communication failure with broker',
        'RD_KAFKA_RESP_ERR__TRANSPORT' => 'Local:Broker transport failure',
        'RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE' => 'Local:Critical system resource failure',
        'RD_KAFKA_RESP_ERR__RESOLVE' => 'Local:Host resolution failure',
        'RD_KAFKA_RESP_ERR__MSG_TIMED_OUT' => 'Local:Message timed out',
        'RD_KAFKA_RESP_ERR__PARTITION_EOF' => 'Broker:No more messages',
        'RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION' => 'Local:Unknown partition',
        'RD_KAFKA_RESP_ERR__FS' => 'Local:File or filesystem error',
        'RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC' => 'Local:Unknown topic',
        'RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN' => 'Local:All broker connections are down',
        'RD_KAFKA_RESP_ERR__INVALID_ARG' => 'Local:Invalid argument or configuration',
        'RD_KAFKA_RESP_ERR__TIMED_OUT' => 'Local:Timed out',
        'RD_KAFKA_RESP_ERR__QUEUE_FULL' => 'Local:Queue full',
        'RD_KAFKA_RESP_ERR__ISR_INSUFF' => 'Local:ISR count insufficient',
        'RD_KAFKA_RESP_ERR__NODE_UPDATE' => 'Local:Broker node update',
        'RD_KAFKA_RESP_ERR__SSL' => 'Local:SSL error',
        'RD_KAFKA_RESP_ERR__WAIT_COORD' => 'Local:Waiting for coordinator',
        'RD_KAFKA_RESP_ERR__UNKNOWN_GROUP' => 'Local:Unknown group',
        'RD_KAFKA_RESP_ERR__IN_PROGRESS' => 'Local:Operation in progress',
        'RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS' => 'Local:Previous operation in progress',
        'RD_KAFKA_RESP_ERR__EXISTING_SUBSCRIPTION' => 'Local:Existing subscription',
        'RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS' => 'Local:Assign partitions',
        'RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS' => 'Local:Revoke partitions',
        'RD_KAFKA_RESP_ERR__CONFLICT' => 'Local:Conflicting use',
        'RD_KAFKA_RESP_ERR__STATE' => 'Local:Erroneous state',
        'RD_KAFKA_RESP_ERR__UNKNOWN_PROTOCOL' => 'Local:Unknown protocol',
        'RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED' => 'Local:Not implemented',
        'RD_KAFKA_RESP_ERR__AUTHENTICATION' => 'Local:Authentication failure',
        'RD_KAFKA_RESP_ERR__NO_OFFSET' => 'Local:No offset stored',
        'RD_KAFKA_RESP_ERR__END' => 'RD_KAFKA_RESP_ERR__END',
        'RD_KAFKA_RESP_ERR_UNKNOWN' => 'Unknown broker error',
        'RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE' => 'Broker:Offset out of range',
        'RD_KAFKA_RESP_ERR_INVALID_MSG' => 'Broker:Invalid message',
        'RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART' => 'Broker:Unknown topic or partition',
        'RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE' => 'Broker:Invalid message size',
        'RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE' => 'Broker:Leader not available',
        'RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION' => 'Broker:Not leader for partition',
        'RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT' => 'Broker:Request timed out',
        'RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE' => 'Broker:Broker not available',
        'RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE' => 'Broker:Replica not available',
        'RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE' => 'Broker:Message size too large',
        'RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH' => 'Broker:StaleControllerEpochCode',
        'RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE' => 'Broker:Offset metadata string too large',
        'RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION' => 'Broker:Broker disconnected before response received',
//        'RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS' => 'Broker:Group coordinator load in progress',
//        'RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE' => 'Broker:Group coordinator not available',
//        'RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP' => 'Broker:Not coordinator for group',
        'RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION' => 'Broker:Invalid topic',
        'RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE' => 'Broker:Message batch larger than configured server segment size',
        'RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS' => 'Broker:Not enough in-sync replicas',
        'RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND' => 'Broker:Message(s) written to insufficient number of in-sync replicas',
        'RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS' => 'Broker:Invalid required acks value',
        'RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION' => 'Broker:Specified group generation id is not valid',
        'RD_KAFKA_RESP_ERR_INCONSISTENT_GROUP_PROTOCOL' => 'Broker:Inconsistent group protocol',
        'RD_KAFKA_RESP_ERR_INVALID_GROUP_ID' => 'Broker:Invalid group.id',
        'RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID' => 'Broker:Unknown member',
        'RD_KAFKA_RESP_ERR_INVALID_SESSION_TIMEOUT' => 'Broker:Invalid session timeout',
        'RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS' => 'Broker:Group rebalance in progress',
        'RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE' => 'Broker:Commit offset data size is not valid',
        'RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED' => 'Broker:Topic authorization failed',
        'RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED' => 'Broker:Group authorization failed',
        'RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED' => 'Broker:Cluster authorization failed',
    ];

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
    protected function flush(int $timeout = 1000) {
        $result = $this->producer->flush($timeout);

        foreach ($this->ERRORS as $constant => $error) {
            if ($result === constant($constant)) {
                // If flush timeout error then we will again flush
                if ($result == RD_KAFKA_RESP_ERR__TIMED_OUT) {
                    return $this->flush($timeout);
                }
                throw new \Exception($error);
            }
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
