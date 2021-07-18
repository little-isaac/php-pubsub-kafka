<?php

$broker = "localhost:9092";
$conf = new RdKafka\Conf();

// Set the group id. This is required when storing offsets on the broker
$conf->set('group.id', 'php-pubsub');
$conf->set('bootstrap.servers', $broker);
$conf->set('auto.offset.reset', 'earliest');

$rk = new RdKafka\Consumer($conf);
$rk->addBrokers($broker);

$topicConf = new RdKafka\TopicConf();
$topicConf->set('auto.commit.interval.ms', 100);

// Set the offset store method to 'file'
$topicConf->set('offset.store.method', 'broker');

// Alternatively, set the offset store method to 'none'
// $topicConf->set('offset.store.method', 'none');

// Set where to start consuming messages when there is no initial offset in
// offset store or the desired offset is out of range.
// 'earliest': start from the beginning
$topicConf->set('auto.offset.reset', 'earliest');

$topic = $rk->newTopic("partition-test", $topicConf);

// Start consuming partition 0
$topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
echo "\nNULL msg received ".date("d-m-Y H:i:s",time())."\n";
while (true) {
    $message = $topic->consume(0, 10*1000);
    if($message == NULL){
        // If message is null then it means that it is timeout
        echo "\nNULL msg received ".date("d-m-Y H:i:s",time())."\n";
        continue;
    }
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            var_dump($message);
            break;
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            echo "No more messages; will wait for more\n";
            break;
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            echo "Timed out\n";
            break;
        default:
            throw new \Exception($message->errstr(), $message->err);
            break;
    }
}