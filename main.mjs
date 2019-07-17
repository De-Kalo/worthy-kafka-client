import kafka from 'kafka-node'

export const BASE_TOPIC="hello-worthy2"
export const REPLY_TOPIC="hello-worthy-reply2"
const mainlisten = [BASE_TOPIC]

let connectionString = process.env.KAFKA_HOST + ":" + process.env.KAFKA_PORT

export const Consumer = kafka.Consumer
let Producer = kafka.Producer

export const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'})
export const admin = new kafka.Admin(client);
export const producer = new Producer(client)

export async function verifyTopics(listen) {
    return new Promise((resolve, reject) => {
        admin.listTopics((err, res) => {
            let listenCopy = listen.map(i => i)
            for (let topic of Object.keys(res[1].metadata)) {
                if (listenCopy.includes(topic)) {
                    listenCopy.splice(listen.indexOf(topic), 1)
                }
            }

            let topicsToCreate = []
            for (let unknownTopic of listenCopy) {
                topicsToCreate.push({
                    topic: unknownTopic,
                    partitions: 1,
                    replicationFactor: 1
                })
            }
            if (topicsToCreate.length) {
                client.createTopics(topicsToCreate, (error, result) => {
                    // result is an array of any errors if a given topic could not be created
                    if (error) {
                        reject(new Error(error))
                    }
                    console.log("Main topics created")
                    resolve(result)
                });
            } else {
                resolve()
            }
        })
    })
}

function main()
{
    async function consume() {

        await verifyTopics(mainlisten)

        let consumer = new Consumer(
            client,
            mainlisten.map(t => {
                return {topic: t}
            })
        );

        consumer.on('message', gotMessage)
        console.log("Main starting listener")
    }

    producer.on('ready', function () {
        consume()
    })

    function produce(payloads) {
        if (!Array.isArray(payloads)) {
            payloads = [payloads]
        }

        producer.send(payloads, produceCallback)
    }

    function produceCallback(err, data) {
        if (err) {
            console.log(err)
            throw new Error("Got error on producing: " + err)
        }
        console.log("Main Produce success", data)
    }

    function gotMessage(message) {

        if (message.topic == BASE_TOPIC) {
            let response
            if (message.key === "postback") {
                console.log(message)
                response = new kafka.KeyedMessage('posting-back', JSON.parse(message.value).postback)
            } else {
                response = new kafka.KeyedMessage('got-message', "I gots me a message!")
            }
            let payload = {topic: REPLY_TOPIC, messages: response}
            produce(payload)
        } else {
            throw new Error("Got unexpected message : " + JSON.stringify(message, null, '\t'))
        }
    }
}
main()