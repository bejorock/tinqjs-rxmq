import { Subject } from "rxjs";

export declare type IEnvelope = {
  topic: string;
  key: string;
  content: any;
};

export declare type IIncomingEnvelope = {
  routingKey: string;
} & IEnvelope;

export declare type IQueue = {
  name: string;
  key: string;
};

export declare type ITopic = {
  topic: string;
  queue: IQueue[];
};

const outgoing = new Subject<IEnvelope>();
const incoming = new Subject<IEnvelope>();

export const outQueue = outgoing.asObservable();
export const publish = (envelope: IEnvelope) => outgoing.next(envelope);

export const inQueue = incoming.asObservable();
export const handleIncoming = (envelope: IIncomingEnvelope) =>
  incoming.next(envelope);
