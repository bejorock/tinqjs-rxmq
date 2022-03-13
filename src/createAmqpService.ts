import amqplib from "amqplib/callback_api";
import { Logger } from "./logger";
import { amqpChannel } from "./amqpChannel";
import { amqpConnect } from "./amqpConnect";
import { handleIncoming, ITopic, outQueue } from "./context";
import { amqpExchange } from "./amqpExchange";
import { amqpQueue } from "./amqpQueue";

const createConnection = (
  host: string,
  onReconnect: (conn: amqplib.Connection) => Promise<any>,
  delay: number = 100
) => {
  return amqpConnect(host)
    .then((conn) =>
      (async () => {
        // reset delay
        delay = 100;
        Logger.logger.info("amqp connection ready!");

        const cleanup = (await onReconnect(conn)) || (() => {});

        conn.on("close", () => {
          Logger.logger.info("amqp connection to amqp server lost");
          Logger.logger.info("amqp retry connecting...");

          cleanup();

          setTimeout(() => createConnection(host, onReconnect, delay), delay);
        });

        // return onReconnect(conn).catch((err) => Logger.logger.error(err));
      })()
    )
    .catch((err) => {
      Logger.logger.error(err.message);
      Logger.logger.error("amqp fail to connect, retry connecting...");
      setTimeout(() => createConnection(host, onReconnect, delay * 2), delay);
    });
};

export const createAmqpSubs = async (
  channel: amqplib.Channel,
  topic: ITopic
) => {
  const exchange = await amqpExchange(channel, topic.topic);

  for (let q of topic.queue) {
    const queue = await amqpQueue(channel, q.name, exchange.exchange, q.key);

    channel.consume(
      queue.queue,
      function (msg) {
        Logger.logger.info(`Receive message for key ${msg.fields.routingKey}`);

        let content: any = msg.content.toString("utf-8");
        try {
          content = JSON.parse(content);
        } catch (e) {
          Logger.logger.error(`Not a json message`);
        }

        handleIncoming({
          topic: topic.topic,
          key: q.key,
          routingKey: msg.fields.routingKey,
          content,
        });
      },
      {
        noAck: true,
      }
    );
  }
};

export const createAmqpService = async (host: string, topics: ITopic[]) => {
  createConnection(host, async (conn) => {
    const channel = await amqpChannel(conn);

    outQueue.subscribe({
      next: (envelope) =>
        channel.publish(
          envelope.topic,
          envelope.key,
          Buffer.from(JSON.stringify(envelope.content)),
          { persistent: true }
        ),

      error: (err) => Logger.logger.error(err),
    });

    return () => {
      channel.close(() => console.log("closed channel"));
    };
  });
};
