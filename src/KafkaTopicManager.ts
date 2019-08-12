import {Admin, ITopicConfig, Kafka} from "kafkajs";
import {KafkaOptions} from "./KafkaOptions";
import {execSync} from 'child_process'

export class KafkaTopicManager {
     
    private readonly _admin:Admin
    private _connected:boolean = false
    private _knownTopics:string[] = []

    constructor(client:Kafka) {
        this._admin = client.admin()
    }

    private async _connect() {
        if ( !this._connected ) {
            await this._admin.connect()
        }
    }
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
        let topicsToCreate:ITopicConfig[] = []
        for (let topic of topics) {
            // when creating the topic we don't need the auto-generated kafka prefix.
            topic = topic.replace(process.env.KAFKA_PREFIX,"")

            console.log("Creating topic " + topic)
            // in development environment - we can create using kafka api.
            // in heroku cloud - we need to use the heroku kafka plugin via the cli. (at least while we're in a shared environment.
            if ( process.env.ENV === "development" ) {
                topicsToCreate.push({
                    topic: topic,
                    numPartitions: KafkaOptions.topic.partitions,
                    replicationFactor: KafkaOptions.topic.replication
                })
            } else {
                let result = execSync("heroku kafka:topics:create " + topic + " --partitions " + KafkaOptions.topic.partitions +
                    " --replication-factor " + KafkaOptions.topic.replication + " -a " + process.env.HEROKU_APP_NAME)
                console.log("Topic created: " + result)
            }
        }
        await this._admin.createTopics({topics:topicsToCreate, waitForLeaders:true})
        console.log("Created topics:",topics)
        return topics
    }
}