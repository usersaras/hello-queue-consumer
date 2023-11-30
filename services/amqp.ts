import amqplib from "amqplib";
import dotenv from "dotenv";

dotenv.config();

//can have multiple queues to perform multiple tasks
const QUEUE = "tasks";

let chan: amqplib.Channel;

(async () => {
  try {
    const conn = await amqplib.connect(process.env.CONNECTION_STRING as string);

    chan = await conn.createChannel();
    await chan.assertQueue(QUEUE, {
      durable: true, //persist this queue
    });

    console.log(`Consuming QUEUE ${QUEUE}`);

    chan.consume(QUEUE, (msg) => {
      if (msg) {
        try {
          if (
            msg.properties.headers["x-retry-count"] == 5 &&
            msg.content.toString().includes("1" || 1)
          ) {
            console.log("Solved issue");
            console.log(`Received ${msg.content.toString()}`);

            chan.ack(msg);
          } else {
            if (msg.content.toString().includes("1" || 1)) {
              throw new Error("Sim error!");
            } else {
              console.log(`Received ${msg.content.toString()}`);
              chan.ack(msg);
            }
          }
        } catch (error) {
          console.error(`Error processing job: ${error}`);

          console.log(!msg.properties.headers["x-retry-count"]);

          if (
            !msg.properties.headers["x-retry-count"] ||
            msg.properties.headers["x-retry-count"] < 8
          ) {
            const retryCount =
              (msg.properties.headers["x-retry-count"] || 0) + 1;

            console.log("Retrying ", retryCount);

            setTimeout(() => {
              chan.publish("", QUEUE, msg.content, {
                persistent: true,
                headers: {
                  "x-retry-count": retryCount,
                },
              });
              chan.ack(msg);
            }, 1000 * retryCount);
          } else {
            console.error(
              `Max retries reached for job: ${msg.content.toString()}`
            );
            chan.reject(msg, false);
          }
        }
      } else {
        console.log("Consumer cancelled!");
      }
    });
  } catch (error) {
    console.log(error);
  }
})();
