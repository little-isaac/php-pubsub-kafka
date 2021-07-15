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

$adapter = new \milind\PubSub\Kafka\KafkaPubSubAdapter($producer);
$topic = 'partition-test';
while (true) {
    for ($x = 0; $x < 100; $x++) {
        echo "Publishing to tempTopic " . $x . "\n";
        $adapter->publish($topic, $x, $x);
    }
}
