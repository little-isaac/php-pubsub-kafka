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
//$conf->set('queue.buffering.max.ms', 20);
$conf->set('enable.idempotence', 'true');

$producer = new \RdKafka\Producer($conf);
$producer->addBrokers($broker);

$adapter = new \milind\PubSub\Kafka\KafkaProducerAdapter($producer);
$topic = 'partition-test';
while (true) {
    for ($x = 0; $x < 100; $x++) {
        $extraConf = [
            "pollTimeout" => 0,
            "flushTimeout" => 1000,
            "partition" => $x,
            "msgFlags" => 0,
            "key" => $x
        ];
        $adapter->publish($topic, $x, $extraConf);
    }
}
