<?php

include __DIR__ . '/../vendor/autoload.php';

$broker = 'localhost:9092';



/* Consum Partition Handle Automatic */
//$conf = new \RdKafka\Conf();
//$conf->set('group.id', 'php-pubsub');
//$conf->set('bootstrap.servers', $broker);
//$conf->set('enable.auto.commit', 'false');
//$conf->set('auto.offset.reset', 'largest');
//$consumer = new \RdKafka\KafkaConsumer($conf);
//$adapter = new \milind\PubSub\Kafka\KafkaPubSubAdapter(null,$consumer);
//$topic = 'partition-test';
//$adapter->subscribe($topic, function ($message) {
//    echo $message."\n";
//});

/* Consume only specific partition */

$conf = new \RdKafka\Conf();

// Set the group id. This is required when storing offsets on the broker
$conf->set('group.id', 'php-pubsub');

$rk = new RdKafka\Consumer($conf);
$rk->addBrokers($broker);

$topicConf = new RdKafka\TopicConf();
$topicConf->set('auto.commit.interval.ms', 100);

// Set the offset store method to 'file'
$adapter = new \milind\PubSub\Kafka\KafkaPubSubAdapter(null, null, $rk);
$topic = 'partition-test';
$adapter->consume($topic, $argv[1], $topicConf, function ($message) {
    echo $message . "\n";
});
