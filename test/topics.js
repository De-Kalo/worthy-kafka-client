const {readFileSync} = require("fs");

const {WorthyKafkaClient} = require('../dist/main')

/**
 Unit testing for topic creation and deletion.
 **/

const TEST_TOPIC_NAME = "KAFKA_LIBRARY_UNIT_TEST"

function log() {
    let messages = ['\x1b[36m%s\x1b[0m',new Date().toISOString().replace(/[TZ]/g," ") + " | "]
    messages.push('\x1b[35m' + process.env.SERVICE_NAME + " | "+'\x1b[0m')
    messages.push.apply(messages,Object.values(arguments))
    console.log.apply(console,messages)
}

function setupLocalEnv() {
    log("Setting up local environment")
    process.env.ENV = 'development'
    process.env.STAGE = 'development'
    process.env.KAFKA_URL="localhost:9092"
}

function setupQaEnv() {
    log("Setting up qa environment")
    process.env.ENV = 'qa'
    process.env.STAGE = 'qa'
    process.env.KAFKA_PREFIX = "delaware-16028."


    // to get the certificates, use the following commands from your cli (assuming you have heroku kafka plugin installed):
    // heroku config:get KAFKA_CLIENT_CERT -a worthy-segment-qa > kafka_cert.pem
    // heroku config:get KAFKA_CLIENT_CERT_KEY -a worthy-segment-qa > kafka_key.pem
    // heroku config:get KAFKA_TRUSTED_CERT -a worthy-segment-qa > kafka_ca.crt
    process.env.KAFKA_CLIENT_CERT=readFileSync("kafka_cert.pem")
    process.env.KAFKA_CLIENT_CERT_KEY=readFileSync("kafka_key.pem")
    process.env.KAFKA_TRUSTED_CERT=readFileSync("kafka_ca.crt")
    process.env.KAFKA_URL="ec2-34-235-216-30.compute-1.amazonaws.com:9096,ec2-52-204-144-208.compute-1.amazonaws.com:9096,ec2-18-204-47-99.compute-1.amazonaws.com:9096,ec2-34-224-229-106.compute-1.amazonaws.com:9096,ec2-18-233-211-98.compute-1.amazonaws.com:9096,ec2-18-208-61-56.compute-1.amazonaws.com:9096,ec2-18-233-140-74.compute-1.amazonaws.com:9096,ec2-34-203-24-91.compute-1.amazonaws.com:9096"
}

async function runTests() {
    //process.env.KAFKAJS_LOG_LEVEL="debug"
    process.env.SERVICE_NAME="KafkaLibraryTester"
    process.env.SERVICE_VERSION=1.0
    process.env.HEROKU_APP_NAME="kafka-library-test-service"

    // TODO: include testing in local env!

    // Test qa env
    log("Setting up environment variables to fit qa env")
    setupQaEnv()
    await runTopicTests()
}

async function deleteTestTopic() {
    await WorthyKafkaClient.deleteTopic(TEST_TOPIC_NAME)
}

async function setup() {
    log("Checking if the test topic exists before running tests.")
    if ( await WorthyKafkaClient.topicExists(TEST_TOPIC_NAME) ) {
        log("Test topic exists, deleting it.")
        await deleteTestTopic()
    }
    else {
        log("Test topic does not exist")
    }
}

async function createTestTopic() {
    log("Create a topic")
    await WorthyKafkaClient.createTopic(TEST_TOPIC_NAME)
}

async function deleteTestTopic() {
    log("Deleting test topic")
    await WorthyKafkaClient.deleteTopic(TEST_TOPIC_NAME)

    if ( process.env.ENV == 'qa' ) {
        log("Waiting for 45 seconds for topic to be deleted from the shared environment")
        return new Promise((resolve) => setTimeout(resolve,45000))
    }
}
async function runTopicTests() {
    log("Initializing environment with no predefined topics")
    await WorthyKafkaClient.init()
    await setup()
    await createTestTopic()
    await deleteTestTopic()

    log("Re-initializing environment with predefined topic requested.")
    await WorthyKafkaClient.shutdown()
    await WorthyKafkaClient.init({
        consuming:{[TEST_TOPIC_NAME]:{default:TEST_TOPIC_NAME}}
    })
    await deleteTestTopic()
    await teardown()
    await WorthyKafkaClient.shutdown()
}

async function teardown() {
    log("Deleting consumer group")
    await WorthyKafkaClient.destroyConsumerGroup()
}

runTests().then(() => {
    log("All tests complete!")
}).catch((reason) => {
    log("Error! ")
})