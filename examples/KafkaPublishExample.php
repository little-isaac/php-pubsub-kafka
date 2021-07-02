<?php

include __DIR__ . '/../vendor/autoload.php';

//$broker = getenv('KAFKA_BROKER');
$broker = 'localhost:9092';

//$conf = new \RdKafka\Conf();
//$conf->set('bootstrap.servers', $broker);
//$conf->set('group.id', 'php-pubsub');
//$conf->set('enable.auto.commit', 'false');
//$conf->set('auto.offset.reset', 'largest');
//
//$consumer = new \RdKafka\KafkaConsumer($conf);

// create producer
$conf = new \RdKafka\Conf();
$conf->set('bootstrap.servers', $broker);
$conf->set('queue.buffering.max.ms', 20);

$producer = new \RdKafka\Producer($conf);
$producer->addBrokers($broker);

$adapter = new \Superbalist\PubSub\Kafka\KafkaPubSubAdapter($producer);

for ($x = 0; $x < 10; $x++) {
    echo "Publishing to tempTopic ".$x."\n";
    $adapter->publish('tempTopi1c', $x);
}
