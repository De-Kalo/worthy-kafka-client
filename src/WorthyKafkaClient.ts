// import {Consumer, ConsumerGroup, KafkaClient, Producer} from 'kafka-node'
import {Kafka} from 'kafkajs'
import { KafkaTopicManager } from './KafkaTopicManager';
import {WorthyProducer} from "./WorthyProducer";
import {WorthyConsumer} from "./WorthyConsumer";
import {v4 as uuidv4} from 'uuid'
import {KafkaOptions, reinitEnv} from "./KafkaOptions";
import { WorthyKafkaClientDescription } from './WorthyTypes';


export class WorthyKafkaClient {
    private _topicManager:KafkaTopicManager
    private _client:Kafka
    private _producer:WorthyProducer
    private _consumer:WorthyConsumer

    private _clientSetup() {
        reinitEnv()
        this._client = new Kafka({clientId:process.env.SERVICE_NAME,brokers:[KafkaOptions.connect.kafkaHost]})
        this._producer = new WorthyProducer()
        this._consumer = new WorthyConsumer(this._client.consumer({groupId:"test"}))
        this._topicManager = new KafkaTopicManager(this._client)
    }

    public async init(clientDescription:WorthyKafkaClientDescription) {
        this._clientSetup()
        clientDescription.producing = clientDescription.producing || {}
        clientDescription.consuming = clientDescription.consuming || {}
        let producingTopics = Object.keys(clientDescription.producing)
        let consumingTopics = Object.keys(clientDescription.consuming)

        // initialize producer if needed
        if ( clientDescription.producing && producingTopics.length > 0 ) {
            // first verify all producing topics exist.
            await this._topicManager.verifyTopics(producingTopics)
            // safe to initialize producer.
            await this._producer.init(this._client.producer(),clientDescription.producing);
        }
        // initialize consumer if needed.
        if ( clientDescription.consuming && consumingTopics.length > 0 ) {
            await this._topicManager.verifyTopics(consumingTopics)
            await this._consumer.addTopics(clientDescription.consuming)
        }
    }

    public async produce(topic:string,key:string,payload:any,context?:string) {
        await this._producer.produce(topic,key,payload,context ? context : uuidv4())
    }
}

export const instance = new WorthyKafkaClient()

