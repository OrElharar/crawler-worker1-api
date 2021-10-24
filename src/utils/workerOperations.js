const { urlObjPartialKey, treeLvlPartialKey, partialListKey } = require("../db/dbKeys");
const redisClient = require("../db/redis");
const scrape = require("./scrape");
const { pullMessagesFromQueue, sendMessagesToQueue } = require("./sqs");


// const startWorker = async () => {
//     while (true) {
//         try {
//             const Messages = await pullMessagesFromQueue()
//             if (Messages.length !== 0) {
//                 const messages = Messages.map((Message) => {
//                     return JSON.parse(Message.Body)
//                 })
//                 await handleMessages(messages)
//             }
//         } catch (err) {
//             console.log(err);
//         }
//     }
// }
const startWorker = async () => {

    try {
        const Messages = await pullMessagesFromQueue()
        if (Messages.length !== 0) {
            const messages = Messages.map((Message) => {
                return JSON.parse(Message.Body)
            })
            await handleMessages(messages)
            continueWork()

        }
    } catch (err) {
        console.log(err);
        continueWork()
    }

}

const continueWork = () => {
    startWorker()
        .then()
        .catch((err) => console.log({ err }))
}

const handleMessages = async (messages) => {
    const handleMessagesPromises = messages.map((message) => handleNextMessage(message))

    Promise.allSettled(handleMessagesPromises)
        .then((data) => {
            // console.log(data)
        })
        .catch((err) => {
            // console.log({ err });
        })

}
const getPageKey = (message) => {
    const pageKey = `${message.crawlerId}:${message.depthLvl}:${message.Id}:${urlObjPartialKey}`;
    return pageKey

}

const handleNextMessage = async (message) => {
    const crawlerId = message.crawlerId;
    const crawlerStatus = await getCrawlerById(crawlerId);
    const depthLvl = parseInt(crawlerStatus.currentDepth);
    const isCrawlerScannedUrl = await isCrawlerAlreadyScannedUrl(message.url, message.crawlerId);
    if (!isCrawlerScannedUrl) {
        const pageJson = await getPageByMessage(message)
        const pageKey = `${crawlerId}:${depthLvl}:${message.id}:${urlObjPartialKey}`;
        await redisClient.setAsync(pageKey, pageJson);
        // const pagesListKey = `${crawlerId}:${depthLvl}:${partialListKey}`;
        // await redisClient.rpush(pagesListKey, pageJson);

        await incrementScannedUrls(crawlerId);

    }
    else {
        await redisClient.hincrbyAsync(`crawler:${crawlerId}`, "currentDepthDeadEnds", 1);
    }
    await handleWorkerAfterScrapingChecks(crawlerId);
}

const getPageByMessage = async (message) => {
    const urlId = message.id;
    const url = message.url
    const parentId = message.parentId;
    const crawlerId = message.crawlerId;
    const depthLvl = message.urlDepth;
    const urlJson = await redisClient.getAsync("*:" + url);
    if (urlJson == null) {
        const pageJson = await scrape(url, parentId, depthLvl, urlId);
        await redisClient.setAsync(`${crawlerId}:${url}`, JSON.stringify({
            hasVisited: true,
            originalId: urlId,
            originalCrawlerId: crawlerId,
            originalDepthLvl: depthLvl
        }));
        return pageJson
    }
    else {
        const cachedUrl = JSON.parse(urlJson);
        const originalCrawlerId = cachedUrl.originalCrawlerId;
        const originalId = cachedUrl.originalId;
        const originalDepthLvl = cachedUrl.depthLvl
        const pageJson = await redisClient.getAsync(`${originalCrawlerId}:${originalDepthLvl}:${originalId}:${urlObjPartialKey}`);
        return pageJson
    }

}




const incrementScannedUrls = async (crawlerId) => {
    await redisClient.hincrbyAsync(`crawler:${crawlerId}`, "currentDepthScannedUrls", 1);
    await redisClient.hincrbyAsync(`crawler:${crawlerId}`, "totalNumberOfScannedUrls", 1);
}

const handleWorkerAfterScrapingChecks = async (crawlerId) => {
    const updatedCrawlerStatus = await getCrawlerById(crawlerId);
    const isCurrentDepthScanDone = isCurrentDepthScanFinished(updatedCrawlerStatus)
    if (isCurrentDepthScanDone) {
        await handlePreperationsForNextLvlImproved(updatedCrawlerStatus)
    }
}

const isCrawlerAlreadyScannedUrl = async (url, crawlerId) => {
    const response = await redisClient.getAsync(`${crawlerId}:${url}`)
    return response != null;
}

const isCrawlingFinished = (crawler) => {
    return (
        (crawler.totalNumberOfScannedUrls === crawler.maxNumberOfPages) ||
        (crawler.maxDepth === crawler.currentDepth &&
            parseInt(crawler.currentDepthTotalNumberOfUrls) === parseInt(crawler.currentDepthScannedUrls) + parseInt(crawler.currentDepthDeadEnds))
    )
}

const isCurrentDepthScanFinished = (crawler) => {
    return (crawler.currentDepthTotalNumberOfUrls === crawler.currentDepthScannedUrls ||
        parseInt(crawler.currentDepthDeadEnds) + parseInt(crawler.currentDepthScannedUrls) === parseInt(crawler.currentDepthTotalNumberOfUrls))
}



const handlePreperationsForNextLvl = async (crawler) => {
    crawlerId = crawler.id;
    const firstId = parseInt(crawler.currentDepthFirstUrlId);
    const currentDepthTotalNumberOfUrls = parseInt(crawler.currentDepthTotalNumberOfUrls);
    const lastId = firstId + currentDepthTotalNumberOfUrls - 1;
    const currentDepth = parseInt(crawler.currentDepth);
    const nextDepth = currentDepth + 1;
    const pagesListKey = `${crawlerId}:${currentDepth}:${partialListKey}`;
    const pagesListJson = await redisClient.lrangeAsync(pagesListKey, 0, -1);
    if (pagesListJson.length >= 1) {
        const pagesList = pagesListJson.map((pageJson) => JSON.parse(pageJson));
        arrangeTreeByPages(pagesList, lastId, nextDepth, crawler)

    }
    else {
        const pagesList = [JSON.parse(pagesListJson)];
        console.log({ pagesList });
        arrangeTreeByPages(pagesList, lastId, nextDepth, crawler)
    }
}


const handlePreperationsForNextLvlImproved = async (crawler) => {
    const crawlerId = crawler.id;
    const firstId = parseInt(crawler.currentDepthFirstUrlId);
    const currentDepthTotalNumberOfUrls = parseInt(crawler.currentDepthTotalNumberOfUrls);
    const lastId = firstId + currentDepthTotalNumberOfUrls - 1;
    const currentDepth = parseInt(crawler.currentDepth);
    const nextDepth = currentDepth + 1;
    const pagesListKey = `${crawlerId}:${currentDepth}:${partialListKey}`;

    for (let i = firstId; i <= lastId; i++) {
        const pageJson = await redisClient.getAsync(`${crawlerId}:${currentDepth}:${i}:${urlObjPartialKey}`);
        if (pageJson != null)
            await redisClient.rpush(pagesListKey, pageJson);
    }
    const pagesListJson = await redisClient.lrangeAsync(pagesListKey, 0, -1);
    if (pagesListJson.length >= 1) {
        const pagesList = pagesListJson.map((pageJson) => JSON.parse(pageJson));
        sendUrlsChildrenToQueue(pagesList, lastId, nextDepth, crawler)

    }
    else {
        const pagesList = [JSON.parse(pagesListJson)];
        sendUrlsChildrenToQueue(pagesList, lastId, nextDepth, crawler)
    }
}


const sendUrlsChildrenToQueue = async (sortedPages, lastId, nextDepth, crawler) => {
    const currentDepth = crawler.currentDepth
    const crawlerId = crawler.id
    const messagesToSend = [];
    let nextDepthTotalNumberOfUrl = 0;
    const remainedNumberOfPagesToScan = parseInt(crawler.maxNumberOfPages) - parseFloat(crawler.totalNumberOfScannedUrls);
    for (let j = 0; j < sortedPages.length; j++) {
        const page = sortedPages[j]
        const links = page.links;
        for (let i = 0; i < links.length && nextDepthTotalNumberOfUrl < remainedNumberOfPagesToScan; i++) {
            nextDepthTotalNumberOfUrl++;
            const link = links[i];
            const linkId = lastId + nextDepthTotalNumberOfUrl;
            const newPage = {
                url: link,
                crawlerId,
                urlDepth: nextDepth,
                id: linkId,
                parentId: page.id
            }
            messagesToSend.push(JSON.stringify(newPage))
        }


    }
    const isCrowlingDone = (isCrawlingFinished(crawler) || nextDepthTotalNumberOfUrl === 0)
    if (!isCrowlingDone) {
        await sendMessagesToQueue(messagesToSend);
    }

    await saveCurrentDepthTreeOnRedis(sortedPages, currentDepth, crawlerId, isCrowlingDone);
    await updateAndSaveCrawler(crawler, nextDepthTotalNumberOfUrl);
}

const arrangeTreeByPages = async (allPages, lastId, nextDepth, crawler) => {
    const sortedPages = mergeSort(allPages)

    const crawlerId = crawler.id
    const currentDepth = crawler.currentDepth
    const messagesToSend = [];
    let nextDepthTotalNumberOfUrl = 0;
    const remainedNumberOfPagesToScan = parseInt(crawler.maxNumberOfPages) - parseFloat(crawler.totalNumberOfScannedUrls);

    for (let j = 0; j < sortedPages.length; j++) {
        const page = sortedPages[j]
        const links = page.links;
        for (let i = 0; i < links.length && nextDepthTotalNumberOfUrl < remainedNumberOfPagesToScan; i++) {
            nextDepthTotalNumberOfUrl++;
            const link = links[i];
            const linkId = lastId + nextDepthTotalNumberOfUrl;
            const newPage = {
                url: link,
                crawlerId,
                urlDepth: nextDepth,
                id: linkId,
                parentId: page.id
            }
            messagesToSend.push(JSON.stringify(newPage))
        }


    }
    const isCrowlingDone = (isCrawlingFinished(crawler) || nextDepthTotalNumberOfUrl === 0)
    if (!isCrowlingDone) {
        await sendMessagesToQueue(messagesToSend);
    }

    await saveCurrentDepthTreeOnRedis(sortedPages, currentDepth, crawlerId, isCrowlingDone);
    await updateAndSaveCrawler(crawler, nextDepthTotalNumberOfUrl);
}


const mergeSort = (arr) => {
    const half = parseInt(arr.length / 2)
    if (arr.length <= 1) {
        return arr
    }
    const firstPartOfArr = arr.splice(0, half);
    const secondPartOfArr = arr;
    return merge(mergeSort(firstPartOfArr), mergeSort(secondPartOfArr))
}

const merge = (arr1, arr2) => {
    const sortedArr = [];
    while (arr1.length > 0 && arr2.length > 0) {
        if (arr1[0].parentId < arr2[0].parentId || (arr1[0].parentId === arr2[0].parentId && arr1[0].id < arr2[0].id)) {
            sortedArr.push(arr1.shift());
        } else {
            sortedArr.push(arr2.shift());
        }
    }
    return [...sortedArr, ...arr1, ...arr2]
}


const saveCurrentDepthTreeOnRedis = async (currentDepthTree, currentDepth, crawlerId, isCrowlingDone) => {
    const jsonCurrentDepthTree = JSON.stringify({ currentDepthTree, isCrowlingDone });
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
const getCrawlerById = async (crawlerId) => {
    const crawlerStatus = await redisClient.hgetallAsync(`crawler:${crawlerId}`);
    return crawlerStatus;
}
const saveCrawler = async (data) => {
    const hashKey = `crawler:${data.id}`;
    const hashArray = [];
    for (let [key, value] of Object.entries(data)) {
        hashArray.push(key);
        hashArray.push(value)
    }

    await redisClient.hmset(hashKey, hashArray);
}



module.exports = {
    startWorker
}