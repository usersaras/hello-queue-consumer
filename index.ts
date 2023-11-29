import express from "express";
import configure from "./routers";
import "./services/amqp";
import dotenv from "dotenv";

dotenv.config();
console.log(process.env.PORT);

const app = express();
const port = process.env.PORT;

configure(app);

console.log(`Attempting to run server on port ${port}`);

app.listen(port, () => {
  console.log(`Listening on port ${port}`);
});
