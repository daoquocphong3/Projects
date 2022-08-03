import scrapy
from shutil import which
from scrapy_selenium import SeleniumRequest
from scrapy.crawler import CrawlerProcess


SELENIUM_DRIVER_NAME = 'chrome'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
# '--headless' if using chrome instead of firefox
SELENIUM_DRIVER_ARGUMENTS = ['-headless']

DOWNLOADER_MIDDLEWARES = {
    'scrapy_selenium.SeleniumMiddleware': 800
}

main_url = 'https://thecoffeehouse.com'


class SoundcloudSpider(scrapy.Spider):
    name = 'thecoffeehouse'

    def start_requests(self):
        url = 'https://thecoffeehouse.com/collections/all'
        yield SeleniumRequest(url=url, callback=self.parse)

    def parse(self, response):
        blocks = response.css('.cha.block_menu_item.loaded')
        # print(li)
        for block in blocks:
            menu_items = block.css('div.menu_item')
            for menu_item in menu_items:
                yield{
                    'name': menu_item.css('a::text').get(),
                    'price': menu_item.css('div.price_product_item::text').get(),
                    'link': main_url + menu_item.css('a').attrib['href'],
                }
        print(len(blocks))


process = CrawlerProcess(
    settings={
        'FEED_URI': 'Crawl_HTML_data.json',
        'FEED_FORMAT': 'json'
    }
)
process.crawl(SoundcloudSpider)
process.start()
