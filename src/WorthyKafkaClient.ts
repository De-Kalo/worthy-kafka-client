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
        this._client = new Kafka(KafkaOptions.connect)
        this._producer = new WorthyProducer()
        this._consumer = new WorthyConsumer(this._client.consumer(KafkaOptions.consumer))
        this._topicManager = new KafkaTopicManager(this._client)
    }

    // from the service perspective, the topic name is just a logical name for the topic.
    // from the infrastructure perspective, we need to support different environments on the same kafka instance,
    // as well as the KAFKA_PREFIX requested by the heroku kafka plugin.
    private _normalizeTopicName(name:string) {
        return (process.env.KAFKA_PREFIX || "") + (process.env.ENV ? process.env.ENV + "." : "") + name
    }

    // re-generate the client description object with normalized topic names.
    private _normalizeTopicNames(clientDescriptionIn:WorthyKafkaClientDescription) {
        let newDescription:WorthyKafkaClientDescription = {
            consuming:{},
            producing:{}
        }

        for ( let topicName in clientDescriptionIn.consuming ) {
            let newName = this._normalizeTopicName(topicName)
            newDescription.consuming[newName] = clientDescriptionIn.consuming[topicName]
        }

        for ( let topicName in clientDescriptionIn.producing ) {
            let newName = this._normalizeTopicName(topicName)
            newDescription.producing[newName] = clientDescriptionIn.producing[topicName]
        }
        return newDescription
    }

    public async init(clientDescriptionIn:WorthyKafkaClientDescription) {
        // basic setup of required objects.
        this._clientSetup()

        // in a shared kafka environment, we need to normalize topic names and obfuscate this from the users.
        let clientDescription = this._normalizeTopicNames(clientDescriptionIn)

        clientDescription.producing = clientDescription.producing || {}
        clientDescription.consuming = clientDescription.consuming || {}
        let producingTopics = Object.keys(clientDescription.producing)
        let consumingTopics = Object.keys(clientDescription.consuming)

        // initialize producer if needed
        if ( clientDescription.producing && producingTopics.length > 0 ) {
            // first verify all producing topics exist.
            await this._topicManager.verifyTopics(producingTopics)
            // safe to initialize producer.
            await this._producer.init(this._client.producer(KafkaOptions.producer),clientDescription.producing);
        }
        // initialize consumer if needed.
        if ( clientDescription.consuming && consumingTopics.length > 0 ) {
            await this._topicManager.verifyTopics(consumingTopics)
            await this._consumer.addTopics(clientDescription.consuming)
        }
    }

    public async produce(topic:string,key:string,payload:any,context?:string) {
        let nTopic = this._normalizeTopicName(topic)
        await this._producer.produce(nTopic,key,payload,context ? context : uuidv4())
    }
}

export const instance = new WorthyKafkaClient()

