import {ConsumerConfig, KafkaConfig, ProducerConfig} from "kafkajs";

interface IKafkaOptions  {
    connect:KafkaConfig
    consumer:ConsumerConfig
    producer:ProducerConfig
    topic: {
        replication:number,
        partitions:number
    }
}

let options:IKafkaOptions = {
    connect:{
        brokers:[],
        connectionTimeout: 5000,
        requestTimeout: 5000
    },
    consumer:{
        groupId: process.env.SERVICE_NAME+"-"+process.env.ENV, //consumer group id, TODO  - use consumer group
        allowAutoTopicCreation:false,
        maxInFlightRequests:5,
        heartbeatInterval:3000,
        sessionTimeout:10000,
        rebalanceTimeout:60000
    },
    producer:{
        allowAutoTopicCreation: false,
        idempotent: false,
        maxInFlightRequests: 5
    },
    topic: {
        replication:3,
        partitions:1
    }
}

/**
 * Run on initialization to verify all required environment variables are accounted for.
 */
function verifyEnvironment() {
    // The following variables are required for properly running the library.
    // check that all exist.
    let requiredVariables:any = {
        KAFKA_URL:"A list of comma separated values of 'host:port' strings representing kafka brokers.",
        STAGE:"The stage of the current service (production / qa / development...)",
        ENV:"The environment name. This is different from STAGE as there can be several qa or development environments.",
    }

    // when not in development we need ssl certification for kafka, and make sure the app name is available.
    if ( process.env.STAGE && process.env.STAGE !== "development" ) {
        requiredVariables["KAFKA_CLIENT_CERT_KEY"] = "A certification key for ssl connection. provided by heroku kafka plugin in non development environments."
        requiredVariables["KAFKA_CLIENT_CERT"] = "A certification for ssl connection. provided by heroku kafka plugin in non development environments."
        requiredVariables["KAFKA_TRUSTED_CERT"] = "A certificate authorization certification for ssl connection. profided by heroku kafka plugin in non development environment."
        requiredVariables["HEROKU_APP_NAME"] = "An environment variable containing the current app name. Provided by the heroku:labs dyno-runtime-metadata setting."
    }

    // go over required environment and make sure it exists.
    for ( let key in requiredVariables ) {
        if ( process.env[key] === undefined ) {
            throw new Error("Cannot find environment variable: " + key + ". Variable description:\n\t" + requiredVariables[key])
        }
    }
}

// required for clients that load process env after process is loaded.
export function reinitEnv() {

    verifyEnvironment()

    // change options by environment.
    switch ( process.env.STAGE ) {
        case "production":
        case "qa":
            options.topic.replication = 3
            options.topic.partitions = 3
            break;
        case "development":
            options.topic.replication = 1
            options.topic.partitions = 1
    }

    const kafkaUrls = process.env.KAFKA_URL.replace(/kafka\+ssl:\/\//g,"")
    options.connect.brokers = kafkaUrls.split(",")

    if ( process.env.KAFKA_CLIENT_CERT_KEY && process.env.KAFKA_CLIENT_CERT ) {
        options.connect.ssl = {
            rejectUnauthorized: false,
            key:process.env.KAFKA_CLIENT_CERT_KEY,
            cert:process.env.KAFKA_CLIENT_CERT,
            ca:[process.env.KAFKA_TRUSTED_CERT]
        }
    }

    options.consumer.groupId = process.env.SERVICE_NAME+"-"+process.env.ENV
    console.log("Connection options options are:",options.connect)
}
export const KafkaOptions:IKafkaOptions = options