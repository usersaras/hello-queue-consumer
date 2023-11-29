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
        console.log(`Received ${msg.content.toString()}`);
        //perform intensive operations
        chan.ack(msg);
      } else {
        console.log("Consumer cancelled!");
      }
    });
  } catch (error) {
    console.log(error);
  }
})();
