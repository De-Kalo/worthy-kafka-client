
import kafka from 'kafka-node'

import {verifyTopics,client,producer,Consumer,BASE_TOPIC,REPLY_TOPIC} from './main'

function test()
{
    const testlisten = [REPLY_TOPIC]

    async function consume() {

        await verifyTopics(testlisten)

        let consumer = new Consumer(
            client,
            testlisten.map(t => {
                return {topic: t}
            })
        );

        consumer.on('message', gotMessage)
        console.log("Test Starting listener")
        testInner()
    }

    producer.on('ready', function () {
        consume()
    })

    function gotMessage(message) {
        console.log("Test Got message", message)
    }

    function testInner() {
        let postbackMessage = new kafka.KeyedMessage('postback', JSON.stringify({postback: "hey hey"}))
        let otherMessage = new kafka.KeyedMessage('lala', "lulu")
        let payloads = [
            {topic: BASE_TOPIC, messages: [postbackMessage, otherMessage]},
        ];
        producer.send(payloads, function (err, data) {
            if (err) {
                throw new Error(err)
            }
        });
    }
}
test()