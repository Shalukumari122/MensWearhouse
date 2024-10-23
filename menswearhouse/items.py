# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Store_Details_Item(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = scrapy.Field()

class Store_cat_links_Item(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = scrapy.Field()

class Store_subcat_links_Item(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = scrapy.Field()

class storeLocatorLinks_Items(scrapy.Item):
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = scrapy.Field()

