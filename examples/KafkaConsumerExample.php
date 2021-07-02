<?php

include __DIR__ . '/../vendor/autoload.php';

//$broker = getenv('KAFKA_BROKER');
$broker = 'localhost:9092';

$conf = new \RdKafka\Conf();
$conf->set('group.id', 'php-pubsub');
$conf->set('bootstrap.servers', $broker);
$conf->set('enable.auto.commit', 'false');
$conf->set('auto.offset.reset', 'largest');

$consumer = new \RdKafka\KafkaConsumer($conf);

// create producer
//$conf = new \RdKafka\Conf();
//$conf->set('queue.buffering.max.ms', 20);
//$conf->set('bootstrap.servers', $broker);
//$producer = new \RdKafka\Producer($conf);
//$producer->addBrokers($broker);

$adapter = new \Superbalist\PubSub\Kafka\KafkaPubSubAdapter(null,$consumer);

$adapter->subscribe('tempTopi1c', function ($message) {
    echo $message."\n";
});
