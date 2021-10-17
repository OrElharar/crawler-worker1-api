const { crawlersCounter, crawlerPartialKey } = require("../db/dbKeys");
const redisClient = require("../db/redis");
const scrape = require("./scrape");
const { pullMessagesFromQueue } = require("./sqs");

const startWorker = async () => {
    while (true) {
        const Messages = await pullMessagesFromQueue()
        console.log({ MessageLength: Messages.length });
        if (Messages.length !== 0) {
            const messages = Messages.map((Message) => {
                return JSON.parse(Message.Body)
            })
            const crawlerId = messages[0].crawlerId;
            console.log({ crawlerId, messages });
            const crawlerStatus = await getCrawlerById(crawlerId)
            const depthLvl = crawlerStatus.currentDepth
            messages.forEach((message) => {
                const urlId = message.id
                scrape(message.url)
                    .then(async (page) => {
                        await redisClient.setAsync(`${crawlerId}:${depthLvl}:${urlId}`, page);
                        await redisClient.hincrbyAsync(`crawler:${crawlerId}`, "currentDepthScannedUrls", 1);
                        const updatedCrawlerStatus = await getCrawlerById(crawlerId)
                        if (isCurrentDepthScanFinished(updatedCrawlerStatus)) {
                            handlePreperationsForNextLvl(updatedCrawlerStatus)
                        }
                        // redisClient.rpushAsync(`${crawlerId}:${depthLvl}:${urlId}`, list)
                    })
                    .catch((err) => {
                        console.log({ err });
                    })
            })
        }


    }
}

const handlePreperationsForNextLvl = async (crawler) => {

}

const isCurrentDepthScanFinished = (crawler) => {
    return (crawler.currentDepthTotalNumberOfUrls === crawler.currentDepthScannedUrls)
}

const getCrawlerById = async (crawlerId) => {
    const crawlerStatus = await redisClient.hgetallAsync(`crawler:${crawlerId}`);
    return crawlerStatus;
}

const postPageOnRedis = async (crawlerId, depthLvl, parentId, page) => {

}

module.exports = {
    startWorker
}