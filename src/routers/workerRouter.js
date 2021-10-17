const express = require("express");
const { sendMessageToQueue, pullMessagesFromQueue, deleteQueue } = require("../utils/sqs");

const router = new express.Router();


router.post("/send-message", sendMessageToQueue, async (req, res) => {
    res.send({
        messageId: req.messageId
    })
})

router.get("/pull-messages", pullMessagesFromQueue, async (req, res) => {
    res.send(req.messages)
})

router.delete("/delete-queue", deleteQueue, (req, res) => {
    res.send();
})

module.exports = router