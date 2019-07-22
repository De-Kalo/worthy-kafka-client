import { CreateTopicRequest, KafkaClient } from "kafka-node";
import {KafkaOptions} from "./KafkaOptions";

export class KafkaTopicManager {
     
    private readonly client:KafkaClient
    private _knownTopics:string[] = []

    constructor(client:KafkaClient) {
        this.client = client
    }

    private async _updateTopics() {
        // return promise for async support
        return new Promise(((resolve:(v?:any) => void,reject:(v?:any) => void) => {
            // topic metadata contains all topics...
            this.client.loadMetadataForTopics([],(err,res) => {
                if (err) {
                    reject(err)
                }
                // iterate over res topics. see the 'listTopics' of Admin (node-kafka) for data structure
                this._knownTopics = Object.keys(res[1].metadata)
                resolve()
            })
        }).bind(this))
    }

    async verifyTopics(topics: string[]) : Promise<string[]> {
        let missing = topics.filter(name => !this._knownTopics.includes(name))

        // we have some missing topics - do we need to create them?
        if ( missing ) {
            // first - update topics, then call this function again with create true.
            await this._updateTopics()
            // check again for missing topics
            missing = topics.filter(name => !this._knownTopics.includes(name))
            // if create specified - create them. create is only specified after an update to the known topics has been performed.
            if ( missing ) {
                return await this._createTopics(missing)
            }
        }
        return []
    }
  
    private async _createTopics(topics:string[]) : Promise<string[]> {
        let topicsToCreate:CreateTopicRequest[] = []
        for (let topic of topics) {
            topicsToCreate.push({
                topic: topic,
                partitions: KafkaOptions.topic.partitions,
                replicationFactor: KafkaOptions.topic.replication
            })
        }
        if (topicsToCreate.length) {
            return new Promise(((resolve:(v?:any) => void,reject:(v?:any) => void) => {
                this.client.createTopics(topicsToCreate, (error, result) => {
                    // result is an array containing errors for failed creations.
                    if (result.length) {
                        console.log("Failed creating some topics",result)
                        reject(new Error(error))
                    }
                    // TODO: add creation log instead of console.
                    console.log("Created topics: " + topics.toString())
                    resolve(topics)
                });
            }).bind(this))
        } else {
            return []
        }
    }
}