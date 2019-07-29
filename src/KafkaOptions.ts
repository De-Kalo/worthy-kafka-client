import {v4 as uuidv4} from "uuid";
import {ConsumerOptions, KafkaClientOptions, ProducerOptions} from "kafka-node";

interface IKafkaOptions  {
    connect:KafkaClientOptions,
    consumer:ConsumerOptions
    producer:{
        options:ProducerOptions
    },
    topic: {
        replication:number,
        partitions:number
    }
}

let options:IKafkaOptions = {
    connect:{
        kafkaHost:process.env.KAFKA_HOST + ":" + process.env.KAFKA_PORT,
        connectTimeout: 5000,
        requestTimeout: 5000,
        autoConnect: true,
        connectRetryOptions: {
            retries: 3,
            factor: 3,
            minTimeout: 500,
            maxTimeout: 10000,
            randomize: true
        },
        //sslOptions: {},
        clientId: uuidv4(),
            idleConnection: 180000,
        reconnectOnIdle: true,
        maxAsyncRequests: 10
        //sasl: any   (perhaps will be needed for authentication?)
    },
    consumer:{
        groupId: process.env.SERVICE_NAME+"-"+process.env.ENV, //consumer group id, TODO  - use consumer group
        // Auto commit config
        autoCommit: true,
        autoCommitIntervalMs: 5000,
        // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms
        fetchMaxWaitMs: 100,
        // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte
        fetchMinBytes: 1,
        // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
        fetchMaxBytes: 1024 * 1024 * 4,
        // If set true, consumer will fetch message from the given offset in the payloads
        fromOffset: false,
        // If set to 'buffer', values will be returned as raw buffer objects.
        encoding: 'utf8',
        keyEncoding: 'utf8'
    },
    producer:{
        options: {
            // Configuration for when to consider a message as acknowledged, default 1
            requireAcks: 1,
            // The amount of time in milliseconds to wait for all acks before considered, default 100ms
            ackTimeoutMs: 100,
            // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 0
            partitionerType: 0
        },
    },
    topic: {
        replication:1,
        partitions:1
    }
}

// change options by environment.
switch ( process.env.STAGE ) {
    case "production":
        options.topic.replication = 3
        options.topic.partitions = 3
}

// required for clients that load process env after process is loaded.
export function reinitEnv() {
    options.connect.kafkaHost = process.env.KAFKA_HOST + ":" + process.env.KAFKA_PORT
    options.consumer.groupId = process.env.SERVICE_NAME+"-"+process.env.ENV
}
export const KafkaOptions:IKafkaOptions = options