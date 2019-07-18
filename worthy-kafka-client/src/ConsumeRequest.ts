
export interface ConsumeRequest {
    topicName:string,
    callback:(key:string|null,payload:any) => void
}