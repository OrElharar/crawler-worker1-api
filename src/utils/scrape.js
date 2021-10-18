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
const scrape = async (url) => {
    try {
        const urlResponse = await Axios.get(url);
        const $ = cheerio.load(urlResponse.data);
        const title = $("title").text();
        const links = [];
        $("a").each((i, el) => {
            const link = getFullUrl($(el).attr("href"));
            if (isLinkValid(link))
                links.push(link);
        })
        // console.log({ title, url, links });
        const page = { title, url, links };
        return JSON.stringify(page)
    }
    catch (err) {
        throw {
            status: 500,
            message: err.message
        }
    }
}

// scrape("https://www.instagram.com/or.elharar/").then((page) => {
//     console.log({ page });
// }).catch((err) => {
//     console.log({ err });
// })
module.exports = scrape

