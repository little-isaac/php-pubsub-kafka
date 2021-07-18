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
$conf->set('bootstrap.servers', $broker);
$conf->set('auto.offset.reset', 'earliest');
//$conf->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
//    switch ($err) {
//        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
//            echo "Assign: ";
//            var_dump($partitions);
//            $kafka->assign($partitions);
//            break;
//
//         case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
//             echo "Revoke: ";
//             var_dump($partitions);
//             $kafka->assign(NULL);
//             break;
//
//         default:
//            throw new \Exception($err);
//    }
//});

$rk = new \RdKafka\KafkaConsumer($conf);
//$rk->addBrokers($broker);

// Set the offset store method to 'file'
$adapter = new \milind\PubSub\Kafka\KafkaHighLevelConsumerAdapter($rk);
$topic = 'partition-test';
$extraConf = [
    "consumeTimeout" => 10 * 1000,
];
$adapter->subscribe($topic, function ($message) {
    echo $message . "\n";
},$extraConf);

echo "This will executed after";
