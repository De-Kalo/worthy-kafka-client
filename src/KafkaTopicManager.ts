import {Admin, ITopicConfig, Kafka} from "kafkajs";
import {KafkaOptions} from "./KafkaOptions";
import { HerokuKafkaCliRunner } from "./HerokuKafkaCliRunner";

export function sleep(ms:number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export class KafkaTopicManager {
     
    private readonly _admin:Admin
    private _connected:boolean = false
    private _knownTopics:string[] = []

    /**
     * Constructor - Initializes the Admin structure from kafkajs
     * @param client
     */
    constructor(client:Kafka) {
        this._admin = client.admin()
    }

    /**
     * Must run the connect method at least once before performing any topic related operation.
     * @private
     */
    private async _connect() {
        if ( !this._connected ) {
            await this._admin.connect()
            this._connected = true
        }
    }

    /**
     * Get the list of available topics
     * @private
     */
    private async _updateTopics() {
        // topic metadata contains all topics...
        let MD = await this._admin.fetchTopicMetadata({topics:[]})
        // @ts-ignore   // there is a bug in the index.d.ts file that was fixed but not released to npm yet. TODO remove.
        this._knownTopics = MD.topics.map(x => x.name)
    }

    async verifyTopics(topics: string[]) : Promise<string[]> {
        await this._connect()
        let missing = topics.filter(name => !this._knownTopics.includes(name))

        // we have some missing topics - do we need to create them?
        if ( missing.length ) {
            // first - update topics, then call this function again with create true.
            await this._updateTopics()
            // check again for missing topics
            missing = topics.filter(name => !this._knownTopics.includes(name))
            // if create specified - create them. create is only specified after an update to the known topics has been performed.
            if ( missing.length ) {
                return await this._createTopics(missing)
            }
        }
        return []
    }
  
    private async _createTopics(topics:string[]) : Promise<string[]> {
        // make sure we're connected before running any operation.
        await this._connect()

        // prepare topic creation structure, and iterate all topics
        let topicsToCreate:ITopicConfig[] = []
        for (let topic of topics) {
            // when creating the topic we don't need the auto-generated kafka prefix.
            topic = topic.replace(process.env.KAFKA_PREFIX,"")

            // if using heroku cli - create the topic (the cli doesn't allow multiple topic creation in single command.
            // if not using the cli - add to the create list for later processing.
            if ( KafkaOptions.useHerokuCli ) {
                HerokuKafkaCliRunner.createTopic(topic)
            } else {
                topicsToCreate.push({
                    topic: topic,
                    numPartitions: KafkaOptions.topic.partitions,
                    replicationFactor: KafkaOptions.topic.replication
                })
            }
        }
        // now that we're out of the loop, create the collected list (when not using heroku cli)
        if ( !KafkaOptions.useHerokuCli ) {
            await this._admin.createTopics({topics: topicsToCreate, waitForLeaders: true})
        }

        // now that all topics were created, wait for all of them to be ready
        for ( let topic of topics ) {
            // when creating the topic we don't need the auto-generated kafka prefix.
            topic = topic.replace(process.env.KAFKA_PREFIX,"")
            await this._waitForTopic(topic)
        }

        return topics
    }


    public async deleteTopic(topic:string) {
        if ( KafkaOptions.useHerokuCli  ) {
            HerokuKafkaCliRunner.deleteTopic(topic)
        } else {
            await this._admin.deleteTopics({topics:[topic],timeout:20000})
        }

        await this._waitForTopic(topic,false)
    }

    public async topicExists(topic:string) {
        try {
            if (KafkaOptions.useHerokuCli) {
                HerokuKafkaCliRunner.topicInfo(topic)
            } else {
                await this._admin.fetchTopicMetadata({topics: [topic]})
            }
            // when the command to get info succeeds it means the topic exists.
            return true
        }
        catch (e) {
            return false
        }
    }

    public async createTopic(topic:string) {
        if ( KafkaOptions.useHerokuCli ) {
            HerokuKafkaCliRunner.createTopic(topic)
        } else {
            await this._admin.createTopics({topics: [{
                    topic: topic,
                    numPartitions: KafkaOptions.topic.partitions,
                    replicationFactor: KafkaOptions.topic.replication
                }], waitForLeaders: true})
        }

        await this._waitForTopic(topic)
    }

    private async _waitForTopic(topic:string,existence:boolean = true, timeout:number=90000) {
        let start = new Date().getTime()
        while ( await this.topicExists(topic) !== existence ) {
            // verifying timeout not passed.
            if ( new Date().getTime() - start > timeout ) {
                throw new Error("Timeout waiting for topic " + topic + " to be in state: exists:"+existence)
            }

            await sleep(1000)
        }
    }

    public async shutdown() {
        await this._admin.disconnect()
    }
}