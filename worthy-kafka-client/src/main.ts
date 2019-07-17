import {KafkaClient} from 'kafka-node'
import { KafkaTopicManager } from './KafkaTopicManager';

class WorthyKafkaClient {
    private kafkaHost = process.env.KAFKA_HOST
    private kafkaPort = process.env.KAFKA_PORT
    private connectionString = this.kafkaHost + ":" + this.kafkaPort
    private topicManager:KafkaTopicManager
    private _client:KafkaClient

    private constructor() {
        this._client = new KafkaClient({kafkaHost:this.connectionString})
        this.topicManager = new KafkaTopicManager(this._client)
    }

    static get() {
        if ( instance == null ) {
            instance = new WorthyKafkaClient()
        }
        return instance
    }

    public async consume(listOfTopicNames:string[]) {
        await this.topicManager.verifyTopics(listOfTopicNames)
    }
}

let instance = WorthyKafkaClient.get() 

export {
    instance as WorthyKafkaClient
}