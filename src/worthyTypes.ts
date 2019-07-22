
export type ConsumerRequest = {
    // topic name
    [key:string]:{
        // key name - 'default' as a wildchar all keys that aren't spoken for.
        [key:string]:(message:WorthyEvent) => void
    }
}

export type ProducerReuqest = string[]

export interface WorthyEvent {
    topic:string,
    key:string,
    id:string,                          // GUID (?)
    created:Date,
    originService:string                // service name.
    originServiceVersion:string         // version of the service that sent the event
    contextId:string                    // operation context - it's the developer responsibility to maintain this throughout the code.
    payload:any                         // application data.
}

