const express = require("express");
const cors = require("cors");

const port = process.env.PORT;
const workerRouter = require("./routers/workerRouter");
const { startWorker } = require("./utils/workerOperations");

const app = express();
app.use(cors());
app.use(express.json());
app.use(workerRouter);

app.listen(port, async () => {
    console.log("Server connected, port:", port);
    await startWorker()
});