const AWS = require("aws-sdk");

const sqs = new AWS.SQS({
    apiVersion: "2012-11-05",
    region: process.env.AWS_REGION
})


const QueueUrl = "https://sqs.eu-west-1.amazonaws.com/000447063003/crawler-queue";



// const createQueue = async (req, res, next) => {
//     const QueueName = req.body.queueName;
//     try {
//         const data = await sqs.createQueue({
//             QueueName,
//             Attributes: {
//                 "FifoQueue": "true",
//                 "ContentBasedDeduplication": "true"
//             }

//         }).promise();
//         req.queueUrl = data.QueueUrl;
//         next();
//     } catch (err) {
//         res.status(400).send({
//             status: 400,
//             error: err.message
//         })
//     }
// }

const sendMessagesToQueue = async (Messages) => {
    console.log("sendMessagesToQueue", Messages.length);
    const sendMessagesPromises = Messages.map(message => {
        return sqs.sendMessage({
            QueueUrl,
            // MessageGroupId: messageBody.crawlerId.toString(), ---FifoQueue
            MessageBody: message
        }).promise()
    })
    Promise.allSettled(sendMessagesPromises)
        .then((data) => {
            // console.log(data)
        })
        .catch((err) => {
            // console.log({ err });
        })
}

const sendMessageToQueue = async (messageBody) => {
    // const QueueUrl = req.body.queueUrl;
    const MessageBody = JSON.stringify(messageBody);

    try {
        await sqs.sendMessage({
            QueueUrl,
            // MessageGroupId: messageBody.crawlerId.toString(), ---FifoQueue
            MessageBody
        }).promise();
    } catch (err) {
        throw ({
            status: 500,
            message: err.message
        })
    }
}

const deleteMessagesFromQueue = (Messages) => {
    const deleteMessagesPromises = Messages.map(message => {
        return sqs.deleteMessage({
            QueueUrl,
            ReceiptHandle: message.ReceiptHandle
        }).promise()
    })
    Promise.allSettled(deleteMessagesPromises)
        .then((data) => {
            // console.log(data)
        })
        .catch((err) => {
            // console.log({ err });
        })
}

const pullMessagesFromQueue = async () => {

    try {
        const { Messages } = await sqs.receiveMessage({
            QueueUrl,
            MaxNumberOfMessages: 10,
            MessageAttributeNames: [
                "All"
            ],
            VisibilityTimeout: 30,
            WaitTimeSeconds: 10
        }).promise();

        if (Messages != null) {
            await deleteMessagesFromQueue(Messages)
        }
        return Messages || [];

    } catch (err) {
        res.status(500).send({
            status: 500,
            message: err.message
        })
    }
}

const deleteQueue = async (req, res, next) => {
    const QueueUrl = req.body.queueUrl;
    try {
        await sqs.deleteQueue({ QueueUrl }).promise();
        next();
    } catch (err) {
        res.status(400).send({
            status: 400,
            message: err.message
        })
    }
}

module.exports = {
    sendMessageToQueue,
    pullMessagesFromQueue,
    deleteQueue,
    sendMessagesToQueue
}