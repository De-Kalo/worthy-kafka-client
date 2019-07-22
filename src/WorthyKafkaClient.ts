import {Consumer, KafkaClient, Producer} from 'kafka-node'
import { KafkaTopicManager } from './KafkaTopicManager';
import {ConsumerRequest, ProducerReuqest} from "./worthyTypes";
import {WorthyProducer} from "./WorthyProducer";
import {WorthyConsumer} from "./WorthyConsumer";
import {v4 as uuidv4} from 'uuid'
import {KafkaOptions} from "./KafkaOptions";

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

    public async init(producing?:ProducerReuqest,consuming?:ConsumerRequest) {
        // initialize producer if needed
        if ( producing && producing.length > 0 ) {
            // first verify all producing topics exist.
            await this._topicManager.verifyTopics(producing)
            // safe to initialize producer.
            await this._producer.init(new Producer(this._client,KafkaOptions.producer.options),producing);
        }
        // initialize consumer if needed.
        if ( consuming && Object.keys(consuming).length > 0 ) {
            await this._topicManager.verifyTopics(Object.keys(consuming))
            this._consumer.addTopics(consuming)
        }
    }

    public produce(topic:string,key:string,payload:any,context?:string) {
        this._producer.produce(topic,key,payload,context ? context : uuidv4())
    }
}

export const instance = new WorthyKafkaClient()

