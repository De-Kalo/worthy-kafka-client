// import {Consumer, ConsumerGroup, KafkaClient, Producer} from 'kafka-node'
import {Kafka} from 'kafkajs'
import { KafkaTopicManager, sleep } from './KafkaTopicManager';
import {WorthyProducer} from "./WorthyProducer";
import {WorthyConsumer} from "./WorthyConsumer";
import {v4 as uuidv4} from 'uuid'
import {KafkaOptions, reinitEnv} from "./KafkaOptions";
import { WorthyKafkaClientDescription } from './WorthyTypes';
import Signals = NodeJS.Signals;
import { execSync } from 'child_process';
import { HerokuKafkaCliRunner } from './HerokuKafkaCliRunner';


export class WorthyKafkaClient {
    private _topicManager:KafkaTopicManager
    private _client:Kafka
    private _producer:WorthyProducer
    private _consumer:WorthyConsumer

    constructor() {
        /**
         * Support termination signals
         */
        ['SIGTERM', 'SIGINT', 'SIGUSR2'].map(type => {
            process.once(<Signals>type, async () => {
                try {
                    await this.shutdown()
                } finally {
                    process.kill(process.pid, type)
                }
            })
        })
    }

    private async _clientSetup() {
        // Initialize the client environment. required once.
        reinitEnv()

        // When in shared kafka environment, we need to verify the consumer group first.
        KafkaOptions.useHerokuCli ? await this.verifyConsumerGroup() : ""

        // Initialize all used objects.
        this._client = new Kafka(KafkaOptions.connect)
        this._producer = new WorthyProducer()
        this._consumer = new WorthyConsumer(this._client.consumer(KafkaOptions.consumer))
        this._topicManager = new KafkaTopicManager(this._client)

        // we need to wait for the consumer to be ready to receive messages before returning context.
        await this._consumer.waitInit()
    }

    // from the service perspective, the topic name is just a logical name for the topic.
    // from the infrastructure perspective, we need to support different environments on the same kafka instance,
    // as well as the KAFKA_PREFIX requested by the heroku kafka plugin - when using a shared kafka environment.
    private _normalizeTopicName(name:string) {
        return (process.env.KAFKA_PREFIX || "") + (process.env.ENV ? process.env.ENV + "." : "") + name
    }

    // re-generate the client description object with normalized topic names.
    private _normalizeTopicNames(clientDescriptionIn:WorthyKafkaClientDescription) {
        let newDescription:WorthyKafkaClientDescription = {
            consuming:{},
            producing:{}
        }

        if ( clientDescriptionIn ) {
            for ( let key in clientDescriptionIn ) {
                // type normalization for typescript compiler...
                let _key = <'consuming'|'producing'>key
                for ( let topicName in clientDescriptionIn[<'consuming'|'producing'>key]) {
                    let newName = this._normalizeTopicName(topicName)
                    newDescription[_key][newName] = clientDescriptionIn[_key][topicName]
                }
            }
        }

        return newDescription
    }

    /**
     * Initializes the kafka client library. must be called before first usage.
     * Should be called just once.
     * If a client runs the 'shutdown' command, the init command can be called again to reinitialize the client.
     * @param clientDescriptionIn describes the topics and keys a client is planning to consume and produce.
     */
    public async init(clientDescriptionIn:WorthyKafkaClientDescription) {
        // basic setup of required objects.
        await this._clientSetup()

        // in a shared kafka environment, we need to normalize topic names and obfuscate this from the users.
        let clientDescription = this._normalizeTopicNames(clientDescriptionIn)

        clientDescription.producing = clientDescription.producing || {}
        clientDescription.consuming = clientDescription.consuming || {}
        let producingTopics = Object.keys(clientDescription.producing)
        let consumingTopics = Object.keys(clientDescription.consuming)

        // initialize producer
        // first verify all producing topics exist.
        await this._topicManager.verifyTopics(producingTopics)
        // safe to initialize producer.
        await this._producer.init(this._client.producer(KafkaOptions.producer),clientDescription.producing);

        // initialize consumer
        await this._topicManager.verifyTopics(consumingTopics)
        await this._consumer.addTopics(clientDescription.consuming)
    }

    /**
     * 
     * @param topic the topic to produce to
     * @param key the name of the event we want to produce (currently all event names are the topic names)
     * @param payload application data to send with the event.
     * @param context a context to help tracking related requests.
     */
    public async produce(topic:string,key:string,payload:any,context?:string) {
        let nTopic = this._normalizeTopicName(topic)
        await this._producer.produce(nTopic,key,payload,context ? context : uuidv4())
    }

    /**
     * Creates a topic. This function is only required for test purposes. The library automatically creates topics
     * the client declares it wants to use.
     * @param topic the topic name to create
     */
    public async createTopic(topic:string) {
        await this._topicManager.createTopic(process.env.ENV+"."+topic)
    }

    /**
     * Deletes a topic. this function is only required for test purposes. Topics should be a constant in the system
     * and there should not be a reason (that i can think of at this point) to delete them programatically.
     * @param topic the topic to delete
     */
    public async deleteTopic(topic:string) {
        await this._topicManager.deleteTopic(process.env.ENV+"."+topic)
    }

    /**
     * Checks if a topic exists. returns boolean accordingly.
     * @param topicName the topic name to query
     */
    public async topicExists(topicName:string) {
        return await this._topicManager.topicExists(this._normalizeTopicName(topicName))
    }

    /**
     * If the consumer group this service is supposed to use doesn't exist - it will be created.
     * This function is only relevant when using shared kafka using heroku addon. On a local environment
     * the groups are created automatically.
     */
    public async verifyConsumerGroup() {
        if ( !KafkaOptions.useHerokuCli ) return

        // when using the kafka cli we dont need kafka prefix
        let groupid = KafkaOptions.consumer.groupId.replace(process.env.KAFKA_PREFIX,"")

        // verify we don't already have a consumer group.
        if ( !HerokuKafkaCliRunner.isConsumerGroup(groupid) ) {
            // create 
            HerokuKafkaCliRunner.createConsumerGroup(groupid)
            
            // wait until ready.
            do {
                await sleep(2000)
            } while (!HerokuKafkaCliRunner.isConsumerGroup(groupid))

            console.log("Consumer group " + groupid + " created.")
        } else {
            console.log("Consumer group " + groupid + " exists.")
        }
    }

    /**
     * Destroys the consumer group for the current service.
     * This function must only be called in test environments. There is no reason to programatically 
     */
    public async destroyConsumerGroup() {
        if ( KafkaOptions.useHerokuCli ) {
            HerokuKafkaCliRunner.destroyConsumerGroup(KafkaOptions.consumer.groupId)
        }
    }

    /**
     * Shutdown the client. use before terminating the process.
     */
    public async shutdown() {
        [this._consumer,this._producer,this._topicManager].forEach(async (obj) => {
            if ( obj ) {
                await obj.shutdown()
            }
        })

        if ( this._client ) {
            this._client = null
        }
    }
}

export const instance = new WorthyKafkaClient()

