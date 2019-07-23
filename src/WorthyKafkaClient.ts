import {Consumer, KafkaClient, Producer} from 'kafka-node'
import { KafkaTopicManager } from './KafkaTopicManager';
import {WorthyProducer} from "./WorthyProducer";
import {WorthyConsumer} from "./WorthyConsumer";
import {v4 as uuidv4} from 'uuid'
import {KafkaOptions} from "./KafkaOptions";
import { WorthyKafkaClientDescription } from './WorthyTypes';

export class WorthyKafkaClient {
    private readonly _topicManager:KafkaTopicManager
    private readonly _client:KafkaClient
    private readonly _producer:WorthyProducer
    private readonly _consumer:WorthyConsumer

    constructor() {
        this._client = new KafkaClient(KafkaOptions.connect)
        this._producer = new WorthyProducer()
        this._consumer = new WorthyConsumer(new Consumer(this._client,[],{}))
        this._topicManager = new KafkaTopicManager(this._client)
    }

    public async init(clientDescription:WorthyKafkaClientDescription) {
        clientDescription.producing = clientDescription.producing || {}
        clientDescription.consuming = clientDescription.consuming || {}
        let producingTopics = Object.keys(clientDescription.producing)
        let consumingTopics = Object.keys(clientDescription.consuming)

        // initialize producer if needed
        if ( clientDescription.producing && producingTopics.length > 0 ) {
            // first verify all producing topics exist.
            await this._topicManager.verifyTopics(producingTopics)
            // safe to initialize producer.
            await this._producer.init(new Producer(this._client,KafkaOptions.producer.options),clientDescription.producing);
        }
        // initialize consumer if needed.
        if ( clientDescription.consuming && consumingTopics.length > 0 ) {
            await this._topicManager.verifyTopics(consumingTopics)
            this._consumer.addTopics(clientDescription.consuming)
        }
    }

    public produce(topic:string,key:string,payload:any,context?:string) {
        this._producer.produce(topic,key,payload,context ? context : uuidv4())
    }
}

export const instance = new WorthyKafkaClient()

