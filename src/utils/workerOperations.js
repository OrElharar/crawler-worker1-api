const { urlObjPartialKey, treeLvlPartialKey } = require("../db/dbKeys");
const redisClient = require("../db/redis");
const scrape = require("./scrape");
const { pullMessagesFromQueue, sendMessageToQueue } = require("./sqs");

const startWorker = async () => {
    while (true) {
        const Messages = await pullMessagesFromQueue()
        // console.log({ MessageLength: Messages.length });
        if (Messages.length !== 0) {
            const messages = Messages.map((Message) => {
                return JSON.parse(Message.Body)
            })
            const crawlerId = messages[0].crawlerId;
            const crawlerStatus = await getCrawlerById(crawlerId)
            const depthLvl = crawlerStatus.currentDepth;
            messages.forEach(async (message) => {
                // console.log({ message });
                const urlId = message.id
                const url = message.url
                const isCrawlerScannedUrl = await isCrawlerAlreadyScannedUrl(url, crawlerId);
                if (!isCrawlerScannedUrl) {
                    const urlJson = await redisClient.getAsync("*:" + url);
                    if (urlJson == null) {
                        scrape(url)
                            .then(async (page) => {
                                const pageKey = `${crawlerId}:${depthLvl}:${urlId}:${urlObjPartialKey}`;
                                // console.log({ page });
                                await redisClient.setAsync(pageKey, page);
                                await redisClient.setAsync(`${crawlerId}:${url}`, JSON.stringify({
                                    hasVisited: true,
                                    originalId: urlId,
                                    originalCrawlerId: crawlerId,
                                    originalDepthLvl: depthLvl
                                }));

                                await incrementScannedUrls(crawlerId)


                                const updatedCrawlerStatus = await getCrawlerById(crawlerId)
                                if (isCurrentDepthScanFinished(updatedCrawlerStatus) || isCrawlingFinished(updatedCrawlerStatus)) {
                                    await handlePreperationsForNextLvl(updatedCrawlerStatus)
                                }
                                // redisClient.rpushAsync(`${crawlerId}:${depthLvl}:${urlId}`, list)
                            })
                            .catch(async (err) => {
                                console.log({ err });
                                // await redisClient.hincrbyAsync(`crawler:${crawlerId}`, "currentDepthDeadEnds", 1);
                                // await handleWorkerAfterScrapingChecks(crawlerId)
                            })
                    }
                    else {
                        console.log("Cashing Url");
                        const cashedUrl = JSON.parse(urlJson);
                        const originalCrawlerId = cashedUrl.originalCrawlerId;
                        const originalId = cashedUrl.originalId;
                        const originalDepthLvl = cashedUrl.depthLvl
                        const pageJson = await redisClient.getAsync(`${originalCrawlerId}:${originalDepthLvl}:${originalId}:${urlObjPartialKey}`);
                        const pageKey = `${crawlerId}:${depthLvl}:${urlId}:${urlObjPartialKey}`;
                        await redisClient.setAsync(pageKey, pageJson);
                        await incrementScannedUrls(crawlerId)
                        await handleWorkerAfterScrapingChecks(crawlerId)

                        // const updatedCrawlerStatus = await getCrawlerById(crawlerId)
                        // if (isCurrentDepthScanFinished(updatedCrawlerStatus) || isCrawlingFinished(updatedCrawlerStatus)) {
                        //     handlePreperationsForNextLvl(updatedCrawlerStatus)
                        // }


                    }
                }
                else {
                    await redisClient.hincrbyAsync(`crawler:${crawlerId}`, "currentDepthDeadEnds", 1);
                    await handleWorkerAfterScrapingChecks(crawlerId)
                    // const updatedCrawlerStatus = await getCrawlerById(crawlerId);
                    // if (isCurrentDepthScanFinished(updatedCrawlerStatus)) {
                    //     handlePreperationsForNextLvl(updatedCrawlerStatus)
                    // }
                }
            })
        }


    }
}

const incrementScannedUrls = async (crawlerId) => {
    await redisClient.hincrbyAsync(`crawler:${crawlerId}`, "currentDepthScannedUrls", 1);
    await redisClient.hincrbyAsync(`crawler:${crawlerId}`, "totalNumberOfScannedUrls", 1);
}

const handleWorkerAfterScrapingChecks = async (crawlerId) => {
    const updatedCrawlerStatus = await getCrawlerById(crawlerId);
    if (isCurrentDepthScanFinished(updatedCrawlerStatus)) {
        await handlePreperationsForNextLvl(updatedCrawlerStatus)
    }
}

const isCrawlerAlreadyScannedUrl = async (url, crawlerId) => {
    const response = await redisClient.getAsync(`${crawlerId}:${url}`)
    return response != null;
}

const isCrawlingFinished = (crawler) => {
    return crawler.totalNumberOfScannedUrls === crawler.maxNumberOfPages || crawler.maxDepth === crawler.currentDepth
}

const handlePreperationsForNextLvl = async (crawler) => {
    crawlerId = crawler.id;
    const firstId = parseInt(crawler.currentDepthFirstUrlId);
    const currentDepthTotalNumberOfUrls = parseInt(crawler.currentDepthTotalNumberOfUrls);
    const lastId = firstId + currentDepthTotalNumberOfUrls - 1;
    const currentDepth = parseInt(crawler.currentDepth);
    const nextDepth = currentDepth + 1
    let nextDepthTotalNumberOfUrl = 0;
    const currentDepthTree = [];

    for (let i = firstId; i <= lastId; i++) {
        const pagePartialKey = `${crawlerId}:${currentDepth}:${i}:${urlObjPartialKey}`;
        const pageJson = await redisClient.getAsync(pagePartialKey);
        if (pageJson != null) {
            const page = JSON.parse(pageJson);
            currentDepthTree.push({ id: i, page })
            const links = page.links;
            // console.log({ links });
            // console.log({ id: i, links });
            for (let j = 0; j < links.length; j++) {
                const linkId = lastId + j + 1;
                const url = links[j];
                nextDepthTotalNumberOfUrl++;
                await sendMessageToQueue({
                    url,
                    crawlerId,
                    urlDepth: nextDepth,
                    id: linkId,
                    parentId: i
                })
            }
        }
    }
    const isCrowlingDone = (isCrawlingFinished(crawler) || nextDepthTotalNumberOfUrl === 0)
    await saveCurrentDepthTreeOnRedis(currentDepthTree, currentDepth, crawlerId, isCrowlingDone);
    console.log("Updating crawler after finished depth lvl:", currentDepth);
    await updateAndSaveCrawler(crawler, nextDepthTotalNumberOfUrl);
}

const saveCurrentDepthTreeOnRedis = async (currentDepthTree, currentDepth, crawlerId, isCrowlingDone) => {
    const jsonCurrentDepthTree = JSON.stringify({ currentDepthTree, isCrowlingDone });
    console.log({ currentDepthTree, isCrowlingDone });
    await redisClient.setAsync(`${crawlerId}:${currentDepth}:${treeLvlPartialKey}`, jsonCurrentDepthTree);
}

const updateAndSaveCrawler = async (crawler, nextDepthTotalNumberOfUrl) => {
    crawler.currentDepth = parseInt(crawler.currentDepth) + 1;
    crawler.currentDepthFirstUrlId = parseInt(crawler.currentDepthTotalNumberOfUrls) + parseInt(crawler.currentDepthFirstUrlId)
    crawler.currentDepthTotalNumberOfUrls = nextDepthTotalNumberOfUrl;
    crawler.currentDepthScannedUrls = 0;
    try {
        await saveCrawler(crawler);
    } catch (err) {

    }
}

const saveCrawler = async (data) => {
    // console.log({ saveCrawlerData: data });
    const hashKey = `crawler:${data.id}`;
    const hashArray = [];
    for (let [key, value] of Object.entries(data)) {
        hashArray.push(key);
        hashArray.push(value)
    }

    await redisClient.hmset(hashKey, hashArray);
}

const isCurrentDepthScanFinished = (crawler) => {
    return (crawler.currentDepthTotalNumberOfUrls === crawler.currentDepthScannedUrls ||
        parseInt(crawler.currentDepthDeadEnds) + parseInt(crawler.currentDepthScannedUrls) === parseInt(crawler.currentDepthTotalNumberOfUrls))
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