const Axios = require("axios");
const cheerio = require("cheerio");


const getFullUrl = (url) => {
    if (url.includes("http")) {
        return url;
    }
    else {
        return `http://${url}`
    }
}
const isLinkValid = (url) => {
    const urlRegex = /^(?:http(s)?:\/\/)?[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&'\(\)\*\+,;=.]+$/
    return urlRegex.test(url)
}
const scrape = async (url, parentId, depth, urlId) => {
    try {
        const urlResponse = await Axios.get(url);
        const $ = cheerio.load(urlResponse.data);
        const title = $("title").text();
        const links = [];
        $("a").each((i, el) => {
            const link = $(el).attr("href");
            if (link != null) {
                const fullLink = getFullUrl(link);
                if (isLinkValid(fullLink))
                    links.push(fullLink);
            }

        })
        const page = {
            title,
            url,
            links,
            parentId,
            depth,
            id: urlId

        };

        return JSON.stringify(page)
    }
    catch (err) {
        // console.log("Error message:", { message: err.message, url })
        return JSON.stringify({
            title: "Url not found",
            links: [],
            url,
            parentId,
            depth,
            id: urlId
        })
    }
}

// scrape("https://www.instagram.com/or.elharar/").then((page) => {
//     console.log({ page });
// }).catch((err) => {
//     console.log({ err });
// })
module.exports = scrape

