# -*- coding: utf-8 -*-
import scrapy
import dateparser
import datetime
import re

NUMBER_OF_PAGES = 1

class PagetraversingSpider(scrapy.Spider):
    name = 'SeekingAlphaTechPageTraversing'
    allowed_domains = ['seekingalpha.com']
    start_urls = ["https://seekingalpha.com/market-news/tech?page={}".format(i + 1) for i in range(NUMBER_OF_PAGES)]

    def parse(self, response):
        for post in response.css('.mc'):
            item = dict()
            item['category']='tech'
            item['title'] = post.css(
                '.tiny-share-widget::attr(data-linked)').extract_first()

            item['ref_url'] = post.css(
                '.tiny-share-widget::attr(data-url)').extract_first()

            date_string = post.css('.item-date::text').extract_first()
            item['crawled_at'] = datetime.datetime.now()
            item['date_string_at_crawled'] = date_string
            item['timestamp'] = dateparser.parse(date_string) if date_string else None

            item['contents'] = post.css(
                'div.bullets > ul > li ::text').getall()

            related_indexes = set()
            for href in post.css('a::attr(href)').getall():
                match = re.match(r"\/symbol\/(?P<index>\w+)", href)
                if (match):
                    related_indexes.add(match.group('index'))
            item['related_indexes'] = list(related_indexes)
            yield item
