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

            contents = []
            positive_phrases = []
            negative_phrases = []
            for line in post.css('div.bullets > ul > li'):
                contents.append(''.join(line.css('::text').getall()))

                pos = line.css('font[color*=green]::text').getall()
                positive_phrases.extend(pos)

                neg = line.css('font[color*=red]::text').getall()
                negative_phrases.extend(neg)
            item['contents'] = contents
            item['positive_phrases'] = positive_phrases
            item['negative_phrases'] = negative_phrases

            related_symbols_set = set()
            for href in post.css('a::attr(href)').getall():
                match = re.search(r"\/symbol\/(?P<symbol>\w+)", href)
                if (match):
                    related_symbols_set.add(match.group('symbol'))
            related_symbols = list(related_symbols_set)
            item['related_symbols'] = related_symbols

            main_symbol_from_post = post.css(
                '.media-left > a::text').extract_first()

            main_symbol_from_related_symbols = related_symbols[0] if len(related_symbols) == 1 else None
            item['main_symbol'] = main_symbol_from_post or main_symbol_from_related_symbols

            yield item
