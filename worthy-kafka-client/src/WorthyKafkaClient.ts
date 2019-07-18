import {Consumer, KafkaClient, Producer} from 'kafka-node'
import { KafkaTopicManager } from './KafkaTopicManager';
import {ConsumeRequest} from "./ConsumeRequest";
import {WorthyProducer} from "./WorthyProducer";
import {WorthyConsumer} from "./WorthyConsumer";
import {v4 as uuidv4} from 'uuid'

class WorthyKafkaClient {
    private kafkaHost = process.env.KAFKA_HOST
    private kafkaPort = process.env.KAFKA_PORT
    private connectionString = this.kafkaHost + ":" + this.kafkaPort
    private readonly _topicManager:KafkaTopicManager
    private readonly _client:KafkaClient
    private readonly _producer:WorthyProducer
    private readonly _consumer:WorthyConsumer

    constructor() {
        this._client = new KafkaClient({kafkaHost:this.connectionString})
        this._producer = new WorthyProducer()
        this._consumer = new WorthyConsumer(new Consumer(this._client,[],{}))
        this._topicManager = new KafkaTopicManager(this._client)
    }

    public async init(producingToTopics?:string[],topicToConsume?:ConsumeRequest[]) {
        // initialize producer if needed
        if ( producingToTopics && producingToTopics.length > 0 ) {
            // first verify all producing topics exist.
            await this._topicManager.verifyTopics(producingToTopics)
            // safe to initialize producer.
            await this._producer.init(new Producer(this._client),producingToTopics);
        }
        // initialize consumer if needed.
        if ( topicToConsume && topicToConsume.length > 0 ) {
            await this._topicManager.verifyTopics(topicToConsume.map(t => t.topicName))
            this._consumer.addTopics(topicToConsume)
        }
    }

    public produce(topic:string,key:string,payload:any,context?:string) {
        this._producer.produce(topic,key,{
            topic:topic,
            context_id:context ? context : uuidv4(),
            created_timestamp: new Date(),
            payload:payload,
            key:key,
            id: uuidv4(),
            origin_service:process.env.SERVICE_NAME,
            origin_machine_id:"",   // TODO
            origin_service_version:"v1" // TODO
        })
    }
}

export const instance = new WorthyKafkaClient()

