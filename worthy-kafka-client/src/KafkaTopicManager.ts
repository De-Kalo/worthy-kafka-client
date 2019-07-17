import { KafkaClient } from "kafka-node";

export class KafkaTopicManager {
     
    client:KafkaClient
    knownTopics:string[] = []

    constructor(client:KafkaClient) {
        this.client = client
        this._updateTopics()
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
                this.knownTopics = Object.keys(res[1].metadata)
                resolve()
            })
        }).bind(this))
    }

    async verifyTopics(listOfTopicNames: string[],create:boolean = false) {
        let missing = listOfTopicNames.filter(x => !this.knownTopics.includes(name))

        // we have some missing topics - do we need to create them?
        if ( missing ) {
            // if create specified - create them. create is only specified after an update to the known topics has been performed.
            if ( create ) {
                await this._createTopics(missing)
            } else {
                // first - update topics, then call this function again with create true.
                await this._updateTopics()
                this.verifyTopics(missing,true)
            }
        } 
    }
  
    private async _createTopics(topicNames:string[]) {
        let topicsToCreate = []
        for (let unknownTopic of topicNames) {
            topicsToCreate.push({
                topic: unknownTopic,
                partitions: 1,
                replicationFactor: 1
            })
        }
        if (topicsToCreate.length) {
            return new Promise(((resolve,reject) => {
                this.client.createTopics(topicsToCreate, (error, result) => {
                    // result is an array of any errors if a given topic could not be created
                    if (error) {
                        reject(new Error(error))
                    }
                    // TODO: add creation log instead of console.
                    console.log("Created topics: " + topicsToCreate.toString())
                    resolve(result)
                });
            }).bind(this))
            
        }
    }
}