import { KafkaOptions } from "./KafkaOptions";
import { execSync } from "child_process";

export class HerokuKafkaCliRunner {

    static runCmd(cmd:string,confirm:boolean = false) {
        return execSync("heroku kafka:"+cmd+" -a " + process.env.HEROKU_APP_NAME + 
                    (confirm ? " --confirm " + process.env.HEROKU_APP_NAME : "")) 
    }

    static createTopic(topic:string) {
        this.runCmd("topics:create " + topic + 
            " --partitions " + KafkaOptions.topic.partitions +
            " --replication-factor " + KafkaOptions.topic.replication)
    }

    static deleteTopic(topic:string) {
        this.runCmd("topics:destroy " + topic,true)
    }

    static topicInfo(topic:string) {
        return this.runCmd("topics:info " + topic)
    }
    
    static isConsumerGroup(group:string) {
        let consumerGroups = this.runCmd("consumer-groups")
        return consumerGroups.toString().includes(group)
    }

    static createConsumerGroup(group:string) {
        this.runCmd("consumer-groups:create " +group)
    }

    static destroyConsumerGroup(group:string) {
        this.runCmd("consumer-groups:destroy " + group, true)
    }
}