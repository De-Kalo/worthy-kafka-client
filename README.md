# Worthy Kafka Client
This library was written to simplify working with Kafka in worthy micro services.
It assumes a very simple use of kafka, and exposes only the relevant methods, so that most developers
will not be required to understand kafka internals and APIs.

That said, a developer using this library still needs to understand the fundamentals of distributed
programming, implement proper error handling and recovery, and make sure the processing of all messages
is effectively idempotent - meaning that if the same exact message is received twice, the code will know how to handle
it without creating side affects.

The Worthy Kafka Client library relies on the [KafkaJS](https://kafka.js.org/) library.
## What you need to know
It is assumed that the reader is familiar with basic Kafka terminology and concepts like
producers, consumers, topics, partitions etc. If you are not familiar with kafka, here are some good starting
points:
* https://kafka.apache.org/intro - the home of kafka contains the most detailed documentation, including the system design and architecture. Interesting read, but lengthy and somewhat complicated. For advanced developers with patience.
* https://medium.com/@ujjawal.dixit/kafka-101-broker-producers-consumers-topics-and-partitions-b9bcbbdbf080 - a simplified explanation of the basic concepts with illustrations
* https://chrzaszcz.dev/2019/05/26/kafka-101/ - another introductory post, a bit more advanced.
* https://www.youtube.com/watch?v=U4y2R3v9tlY - If you prefer videos - this is a 16 min introduction to Kafka.

## Installation
* Any project that needs to use this library, will have to add a `.npmrc` file to the root of his project,
and have it contain the following:
```
//registry.npmjs.org/:_authToken=${NPM_TOKEN}
```  
* The above expects the user who runs `npm` or `yarn` commands to have an environment variable 
called `NPM_TOKEN` which contains a valid token to Worthy's npm account - `worthy-npm`. 
If you don't have this token - ask a senior programmer.

* Install using `npm` or `yarn`
```
# npm
npm install @worthy-npm/worthy-kafka-client

# yarn
yarn add @worthy-npm/worthy-kafka-client
```

## Initialization
The library was written with a 'strict' mindset in order to be able to help developers avoid 
simple errors, and be able to perform runtime verification of anything that can be verified.

For this purpose, the init function requires the declaration of all topics and event names that
the user is expecting to produce or consume.
An attempt to produce events that were not pre-declared will result in a failure.
Similarly, event names that are produced to topics that service is listening to, but that were
not declared - will not be sent to a callback and will simply disappear.

The library will verify that all declared topics exist, and if they do not - it will create them 
at service initialization time.

Here's how the init function looks:
```javascript
import { WorthyKafkaClient } from '@worthy-npm/worthy-kafka-client'

async function initKafka() {
    await WorthyKafkaClient.init({
        producing: { topic1: ['eventName1', 'eventName2'] },
        consuming: {
            topic2: {
                'eventName3': async function callback1(event) { /* callback when receiving event - eventName3} */ },
                'eventName4': async function callback2(event) { /* callback when receiving event - eventName4} */ },
                '*': async function defaultCallback(event) { /* this is not mandatory */ }
            }
        }
    })  
}

initKafka()
```
Let's discuss the code above:
1. the `WorthyKafkaClient.init` function returns a promise. If you want to make sure it is properly
initialized before moving on (and you should) - wait for the promise to fulfill.
1. The function receives a single object as input, which defines the topics and event names we
expect to consume and produce. Here are the keys of this object and their explanation:
   1. **producing** - The keys of this object define topic names we expect to produce events to. The value is
   an array of event names we intend to produce. An attempt to produce events that are not defined here will fail.
   1. **consuming** -  The keys of this object define topic names we want to consume from. The library will automatically
   listen to these topics once defined. For each topic, we can define event names and the callback functions to call
   when such an event arrives. It's possible to define a wild char ('*') event name, which will represent
   a default callback that all events that aren't specifically specified will get to.
   Note that the library doesn't support a partial wild char query such as 'prefix\*'.
   If a default callback is not defined, and the topic receives an event that was not defined 
   in the 'consuming' section - the event will be discarded.
   
Once initialization is complete, event can start arriving and callbacks will start running. 
For this reason - make sure that any pre-requisite services (like DBs) are initialized before kafka.

## Event structure
Here's the typescript definition of the event interface:
```javascript
export interface IWorthyEvent {
	topic:string						// Name of the topic to produce to
	id:string							// GUID - the event id
	eventName:string
	created:Date
	received?:Date						// filled on the consumer side upon processing - for tracking latency
	originService:string				// Origin service name.
	originServiceVersion:string			// version of the service that sent the event
	contextId:string					// operation context - used for debugging a distributed system. Events that derive
										// from the processing of other events will have the origin event id here.
	payload:any                         // application data - as received when calling the produce function.
}
```
The event passed to a consumer callback function is of the above structure.

## Producing an event
To produce an event, one needs to call the `WorthyKafkaClient.produce` function.
Here's an example:
```javascript
import {WorthyKafkaClient} from '@worthy-npm/worthy-kafka-client'

// assuming the library is already initialized in a different file..
WorthyKafkaClient.produce('myTopic', 'eventName', { myData: 123 })
``` 

The typescript signature of the produce function is as follows:
```javascript
public async produce(topic:string, eventName:string, payload:any, context?:string)
```
See the `context?` argument at the end. This argument is used for populating the `contextId`
parameter of the produced event.
According to the approach of the service you're writing, there are 2 ways you can go
to manage this parameter. Let's discuss this a bit:

## ContextId
The context id is a crucial of the architecture that we need to zealously maintain.
Distributed systems are hard to manage and especially debug. Everything happens asynchronously,
latencies can be substantial, and the order of execution of flows across different services is not 
something we can rely on. This makes it very difficult to track a specific flow, and separate it
from other flows that are happening at the same time.

The purpose of the contextId is to mitigate this difficulty. If we can 'mark' the beginning of
a flow, and pass on an identifier to all parts of the flow in all other services, we could add this
identifier to all of our logs, and then easily look only at logs related to said flow.
That's the purpose of the contextId. 

There are 2 ways we can maintain this.
1. Automatically. If an environment variable `WORTHY_KAFKA_CLIENT_AUTO_SET_CONTEXT` is set to `true`,
the library will automatically set this variable according to the last event that was received. There's 
a catch here though. In order for this to work - the developer must make sure that only one message is processed
at any given time. No loose Promises, no parallel processing in the same process, etc. This is a viable
way for a service to work, and will help make sure to keep the contextId properly. However - you must verify
a synchronized handling of messages.
1. Manually. If a developer wants to manage contextId by himself - that's fine. He can use the `contextId` 
argument when calling produce, and **not** set the above environment variable. Note that if you do
set the environment variable to 'true', the library will ignore any value you place in the contextId argument.

If you choose the manual approach, you can basically put any string in this variable, but you're
expected to use the following:
* If you have a context (from an event you are currently processing) - use the contextId of that event.
* If you do not have a context, or want to force the begining of a new context, use the following constant:
```javascript
import { WorthyKafkaClient, WORTHY_KAFKA_CLIENT_NEW_TOPIC } from '@worthy-npm/worthy-kafka-client'

// this creates a new context id based on the event id that starts it.
WorthyKafkaClient.produce('topicName', 'eventName', { some: 'data' }, WORTHY_KAFKA_CLIENT_NEW_TOPIC)
``` 

## Environment variables
The library expects and / or uses the following environment variables to exist:
* KAFKA_PREFIX - automatically defined by the heroku kafka plugin. No need to define it locally.
* WORTHY_KAFKA_CLIENT_AUTO_SET_CONTEXT - See the [ContextId](#contextid) section. Optional.
* SERVICE_NAME - name of the service you're running. Mandatory.
* STAGE - whiche stage are we on? { test | development | qa | production }. Mandatory.
* KAFKA_URL - connection string to your kafka borkers. Mandatory. Automatically defined by heroku kafka plugin when in an heroku application.
* KAFKA_CLIENT_CERT_KEY, KAFKA_CLIENT_CERT, KAFKA_TRUSTED_CERT - Required when connecting to a secure kafka
service. Automatically defined by the heroku-kafka plugin. Not needed when running against a simple local installation.
* ENV - the environment name { development | qa | demo | london | prod ... } - different from stage 
since we may have different environments in the 'qa' stage.
* WORTHY_KAFKA_CLIENT_LOG_LEVEL - the default log level for the library is 'error'. If you want
a more verbose library, you can set this to any of { debug | info | warning | error }
* KAFKAJS_LOG_LEVEL - controls logging verbosity of the underlying KafkaJS library.
* CORALOGIX_PRIVATE_KEY - this library uses the worthy-logger library - which requires this env variable.
* HEROKU_APP_NAME - the name of the heroku app - requires when running in a heroku application. 
This variable is defined automatically when installing the `dyno metadata` plugin 
(done once per application) using the CLI command: 
```
heroku labs:enable runtime-dyno-metadata -a <app name>
```
