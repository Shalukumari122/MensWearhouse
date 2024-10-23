import gzip
import hashlib
import json
import os
from datetime import datetime
from scrapy import Selector
import pymysql
import scrapy
from scrapy.cmdline import execute

from menswearhouse.items import Store_Details_Item


def get_banner(json_ld):
    if 'name' in json_ld.keys():
        provider=json_ld['name']
        return provider
    else:
        return 'N/A'
def get_lat(json_ld):
    if 'geo' in json_ld.keys():
        if 'latitude' in json_ld['geo'].keys():
            Latitude=json_ld['geo']['latitude']
        else:
            Latitude='N/A'
    else:
        Latitude='N/A'
    return Latitude

def get_long(json_ld):
    if 'geo' in json_ld.keys():
        if 'longitude' in json_ld['geo'].keys():
            Longitude=json_ld['geo']['longitude']
        else:
            Longitude='N/A'
    else:
        Longitude='N/A'
    return Longitude

def get_street(json_ld):
    if 'address' in json_ld.keys():
        if 'addressCountry' in json_ld['address'].keys():
            Street=json_ld['address']['streetAddress']
        else:
            Street="N/A"
    else:
        Street = "N/A"
    return Street


def get_city(json_ld):
    if 'address' in json_ld.keys():
        if 'addressCountry' in json_ld['address'].keys():
            City=json_ld['address']['addressLocality']
        else:
            City="N/A"
    else:
        City = "N/A"
    return City


def get_country(json_ld):
    if 'address' in json_ld.keys():
        if 'addressCountry' in json_ld['address'].keys():
            Country=json_ld['address']['addressCountry']
        else:
            Country="N/A"
    else:
        Country = "N/A"
    return Country

def get_zipcode(json_ld):
    if 'address' in json_ld.keys():
        if 'addressCountry' in json_ld['address'].keys():
            Zip_Code=json_ld['address']['postalCode']
        else:
            Zip_Code="N/A"
    else:
        Zip_Code = "N/A"
    return Zip_Code

def get_name(json_ld):
    if 'address' in json_ld.keys():
        if 'addressCountry' in json_ld['address'].keys():
            Name=json_ld['address']['streetAddress']
        else:
            Name="N/A"
    else:
        Name = "N/A"
    return Name

def get_phone(json_ld):
    if 'telephone' in json_ld.keys():
        Phone=json_ld['telephone']
    else:
        Phone='N/A'
    return Phone

# monday:-08:00 - 20:00 | tuesday:-08:00 - 20:00 | wednesday:-08:00 - 20:00 | thursday:-08:00 - 20:00 | friday:-08:00 - 20:00 | saturday:-09:00 - 16:00 | sunday:-09:00 - 16:00
def get_opening_hours(json_ld):
    if 'openingHoursSpecification' in json_ld.keys():
        opening_hours = json_ld['openingHoursSpecification']
        formatted_hours = []

        for each_opening_hour in opening_hours:
            # If the key is missing, return 'N/A'
            day = each_opening_hour.get('dayOfWeek', 'N/A')
            open_time = each_opening_hour.get('opens', 'N/A')
            close_time = each_opening_hour.get('closes', 'N/A')

            # Handle if 'dayOfWeek' is a list, extract the first day (or join multiple days)
            if isinstance(day, list):
                day = ', '.join(day)  # If it's a list, join it into a string

            # Format each day's opening hours as 'day:-open - close'
            formatted_hours.append(f"{day.lower()}:-{open_time} - {close_time}")

        # Join all days with ' | '
        return ' | '.join(formatted_hours)

    return 'N/A'


def get_direction(response):
    direction=response.xpath('//div[@class="directions"]/a/@href').extract_first()
    if direction:
        return direction
    else:
        return 'N/A'


class StoreDetailsSpider(scrapy.Spider):
    name = "store_details"
    # allowed_domains = ["'www.menswearhouse.com'"]
    # start_urls = ["https://'www.menswearhouse.com'"]
    def __init__(self):
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='mens_wearhouse_db'
        )
        self.cursor = self.conn.cursor()

    def start_requests(self):
        query = "select *from storelocatorlinks where status='pending'"
        # query = "select *from storelocatorlinks where `Store No.`= 316"

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            storeLocatorLink=row[4]
            subcat_name=row[3]
            subcat_link=row[2]
            state=row[1]

            unique_id = hashlib.sha256((storeLocatorLink).encode()).hexdigest()

            update_query = """
                            UPDATE storelocatorlinks
                            SET unique_id = %s
                            WHERE storeLocatorLink = %s 
                        """
            self.cursor.execute(update_query, (unique_id, storeLocatorLink))
            self.conn.commit()
            print("Row updated successfully.")

            page_save_path = r'C:\paga_save\live_project\menswearhouse'
            full_file_path = os.path.join(page_save_path, f"{unique_id}.html.gz")

            if not os.path.isfile(full_file_path):

                cookies = {
                    'OTGPPConsent': 'DBABLA~BAAAAAAAAgA.QA',
                    'tracker_device_is_opt_in': 'true',
                    '_evga_2566': '{%22uuid%22:%2263f76ea4a1db707a%22}',
                    '_sfid_545b': '{%22anonymousId%22:%2263f76ea4a1db707a%22%2C%22consents%22:[]}',
                    '__ogfpid': 'b4a2d007-6ebf-4397-b861-3915a67aa306',
                    '_qubitTracker': 'ov8gcmixlz4-0m202z81d-54brjps',
                    'uuid': '8af25971-723d-49f5-8c79-c7083c9f641f',
                    'qb_generic': ':ZJq4/Vv:.menswearhouse.com',
                    '_caid': 'c92970f5-8dbe-4eb9-8bcf-aaf4fdf42555',
                    '_cavisit': '1926ae3f68a|',
                    '__spdt': 'f3bc232110444e458ffe9eee26ef2fae',
                    'tracker_device': 'efa3435a-a5b7-4d45-b80d-435909047d4f',
                    '_fbp': 'fb.1.1728370177867.998967294524775655',
                    '_scid': '6Y6WtwE6sj5WXuR-jnf5BNmTUjF4W_wD',
                    'mdLogger': 'false',
                    'kampyle_userid': '9d8d-2835-0162-84c2-8777-6177-1472-6f12',
                    '_tt_enable_cookie': '1',
                    '_ttp': 'a0zyCB5VfHdvzLX-pCmxfgsh1jc',
                    '_ScCbts': '%5B%5D',
                    '_abck': 'CD201A23DB03CDC315761AD507007BEC~0~YAAQJF3SF0foOmaSAQAAtinkagwYxC1yfB2DFCkaISCRQX1GUaA4oG/HmJ59D16gZUvSbeXT14azkxjVzgYENqD10jl9hSLj3eu6Iu7+Bi4DGuaM3wb/W8aEzhgwHx4w8FMa/hM6LaJa/413ToodUJ4tztsE+/+8uAokntfi3sJsi57Vawuo6qof7iOGAYkN34PNTkkUEXzG20VjMPBtNhiBHMsNew8eDpoeLlgON122DTiNAzxr4DbOIawWN0U5QOwn30I2NN/MPDZKfSMwTsZeO4OtBoI/+SO7OFXTsIz4ReymSiTOMtFupnIJ3Fd9xYXwGFNz+ptSDqs/cpAJhKvACiJHjLTRAPSdS9P/2qyiB2SCodF79Nh6ikUKkydOqrKD6+tnqtvMFwO7Lg+/Te/kzg9R+OUvGVlaM7D0zMrCMwUJzXLI7sK/fvQjy52Ukv/WCqNaVX3/pznWeqCwWw==~-1~||0||~-1',
                    '_pin_unauth': 'dWlkPU9EbGtOMlk1WkRBdE1qZ3paaTAwT0RneExUZ3hOelV0TWpGaFpqaGxOelZtT0RjMw',
                    'tkbl_session': 'c5d3b591-7db4-41d9-bbb7-748a35c26241',
                    '_gid': 'GA1.2.284748098.1728370179',
                    '_gcl_au': '1.1.1511323504.1728370179',
                    'tfpsi': 'b61c0932-dbf0-4c66-9d9c-a72c5aabd3fd',
                    '_clck': 'rtrgnf%7C2%7Cfpu%7C0%7C1742',
                    'styliticsWidgetSession': '8b835622-f73b-47c6-a438-84fe947fe158',
                    '_sctr': '1%7C1728325800000',
                    'AKA_A2': 'A',
                    'baseUrl': 'https://www.menswearhouse.com/api',
                    'akacd_TMW': '3905823326~rv=21~id=ac3f827479a111190b71120f9f91daf1',
                    'bm_ss': 'ab8e18ef4e',
                    'page_store_locator': '%7B%22current_zipcode%22%3A%2277019%22%7D',
                    'xyz_cr_1200_et_111': '=NaN&cr=1200&wegc=&et=111&ap=',
                    'qb_241282_bounceUrl': '/',
                    'show_sale_icon': 'tooltip_flag',
                    'OptanonAlertBoxClosed': '2024-10-08T06:55:32.991Z',
                    'bm_mi': '3B8680F088C137A9ED6151F172CDCB67~YAAQJF3SF4mhPGaSAQAAFI3rahnwBlz7sW15BpB74ideDHpitF5gVbYt1LS2+YQ90HvV65P0D0xMTOSjZytE34yZQ0VxSWafI9sMWc7U7kAZH4eXZUjOZrmVFlzzEAguVEDHmDtMpm8zMZqNZfa/XpGDCgjh8cV/QZsKg1AmJqvZ5BtDkMn6758bWwA63vF3dTvh9J95lgd0KS6Iw0b+Oze79vxHVGr8yYUUAlr3mVFS/Ou2TETlhmr6HxJhlBLy2CTTg2PrzoPu7XX2NHj1VwGH4zMSDPUP31MwQbFWVCrQbo167DCPknmQ8S99yGUq/yBW3oA=~1',
                    'bm_so': 'D1F10D19C2B4433684488099F55177E59057FE7925CE6B6E507E1D712267321A~YAAQJF3SF4uhPGaSAQAAFI3ragFH9eD3/jMeuz6hlOM3nixP+YIgpTJpKuh2RLu1AcJlHCxlzE3o+WeubOuUKlB0ftYF1+bZikVsY+tV6mNjFE7y9jSZ/3GtxDyPUBrhXcl+JT2K2GTB8aJKWe2+GQ5AIstTJDtpjg9hinV7Y80Cl5A+cIIfV8v4oRi+aGoCwx5eMCAQCUWeLHK2j+5Pgksn8eGqx8Y9tUPomzmAZ8Aq4lhEZY1fk/Hz8u+O1zPU6nVPVlFEL1LvqZ1/MLYBxCU2aa5Ja4jm1ToxhhVLeLXgHkVOZChu4wDh8xIWJVIi79w6NHbaec5GCy5hl7DTia3r0aE4kX0kYpTuHCzOHWh525ko388IGmZslzAffBTfQAbEEpQEypLzG36ejyH3WqPly9q5lqJqSq5eWPvMgxWPiONVkfs5i2zymYU1mNSPgKymFprMtl3N70WdH4BrLKTI5aA+',
                    'ak_bmsc': 'A3ED5052C9C8ABFC5FE462EB355BBFB7~000000000000000000000000000000~YAAQJF3SF+WjPGaSAQAAA5jrahlYgfQqbIIf+LsSadytm1RLYMSDsPECb7CuL47/FEh6mGU4LeWwfvIygtcystpSEZk77hb7M5yXlsfml8ig0g1JDuU4aVY2xuxPdmUOMT0x0lZGwgdZcda/QGowy0JX9Y8MAKhkTin0PoRs2H0oz8aLY6mwDKlBikTHmOyTvPbO/8USQydgbMq90IU0/7PMd4OSZN8dXx4vvpzld71B6OOJfkp3a+1vVugpKxiN9HQ98Z4bd0Zy6YB8abQnLbSM65qvMQrU+v7Qpiqthb3W+HbGgM2hh+X0pwTzY6dvVOnaoZ3W8Q6hJbaGy9TJtGh9nV+FuBVkpZypFL10QuoyLVscBy5g4P4bVW0yWtBE779bNLmJLuLc6r3aoTBpMrk2nmp+K6p2ZCl1lTCYMMd29wM0w8+oQHjqWi5+Wh6f5djvMi32Jb5xUxlFM6m1NYLAVcOQjW5Pjs3rikbq+E3nEuKoK/4=',
                    'bm_lso': 'D1F10D19C2B4433684488099F55177E59057FE7925CE6B6E507E1D712267321A~YAAQJF3SF4uhPGaSAQAAFI3ragFH9eD3/jMeuz6hlOM3nixP+YIgpTJpKuh2RLu1AcJlHCxlzE3o+WeubOuUKlB0ftYF1+bZikVsY+tV6mNjFE7y9jSZ/3GtxDyPUBrhXcl+JT2K2GTB8aJKWe2+GQ5AIstTJDtpjg9hinV7Y80Cl5A+cIIfV8v4oRi+aGoCwx5eMCAQCUWeLHK2j+5Pgksn8eGqx8Y9tUPomzmAZ8Aq4lhEZY1fk/Hz8u+O1zPU6nVPVlFEL1LvqZ1/MLYBxCU2aa5Ja4jm1ToxhhVLeLXgHkVOZChu4wDh8xIWJVIi79w6NHbaec5GCy5hl7DTia3r0aE4kX0kYpTuHCzOHWh525ko388IGmZslzAffBTfQAbEEpQEypLzG36ejyH3WqPly9q5lqJqSq5eWPvMgxWPiONVkfs5i2zymYU1mNSPgKymFprMtl3N70WdH4BrLKTI5aA+^1728370669354',
                    'bounceClientVisit4803v': 'N4IgNgDiBcIBYBcEQM4FIDMBBNAmAYnvgO6kB0AtgKYB2KxVAhgE5wD2ArilWQMZsUimfCmYowCAJYATTABEsAMwoAhAPJtmAR2YBJABoBrAAy8qAWgBGAcQAsAGQDCurMwgBORgCVdAVQAKvggA7HK8ALLGagBecooA0rheisQA6ubuuioAmgAe9gDKIAA0IMwwIKTEZADmbGw1YDz8FCUgkigA+nWd3Cgokmw0MIqMYNylHd0QvVT9g8PQo+NUAL5AA',
                    'bm_s': 'YAAQJF3SF0GyPGaSAQAAQsbragIeb4BrF8Tl0enp6U1e8jeDZ9JjCtIFBSsgFFYs4I3uYeCIFgY4cQ6KNcSyGWL0d14XugalDggFMnB2k2y/oDkgv/s9n9Qxn3O2bSmv0QKEE1RDr67/ACzHNQ0RN56jGRQJWZq5IOJxd27dKOjIn68ZwJBKkZzGu77+JDjOcPpiZ5O1ns7OOCRbcE7pcu4DGldlol+kibTmgor+yCZAMO/H1rAdNsomHZPVe81N5+3IGc2LIhKXzTIbQlZmkwecq0CB0e1xTajp0170YBEfoEnNaFgXd6HBsEoAMBTu4N7HwNpR4cz05d2OA4Gke7ddVbrtXfxSJs52xjo=',
                    'qb_241282_bounce': '2',
                    'dtm_token_sc': 'AQAL5TOEBpNc1wFK71o0AQA-FwABAQCTa-5o5wEBAJNr7mjn',
                    'dtm_token': 'AQAL5TOEBpNc1wFK71o0AQA-FwABAQCTa-5o5wEBAJNr7mjn',
                    'DECLINED_DATE': '1728371503393',
                    'kampylePageLoadedTimestamp': '1728371503405',
                    'kampyleUserSession': '1728372290471',
                    'kampyleUserSessionsCount': '10',
                    'kampyleUserPercentile': '88.38699675134765',
                    'bm_sz': 'CDBAD502FBEC94C7B87D91D3FDDDB09D~YAAQtjtAF7K7uzKSAQAAlMMIaxlwNhMoLkt7d4YPKOwVGbWdl15HEr9AikkPZ1rcY2el9NUjD0AWpS8GVzJbdOw8VahsQMFJLNbDIW7tCdotH8XMaNvlEWBL7p99QZpJvIlz/Z8r+c5OiL25buFDuDSpVxXUy6hGqqyi2oyCZL1sdO3A1HuH7Bl23Jq4N0+/IOxUelJo+GyAqPJsf9FYSFmC1GvV+YOQeOdOUABRHLskF8EnU/f7rkq7AYlYoI75teoI8kAmH3b2wQFM8Kgqosei+oBJpWMXOf/RlO7/NGQXEFhQmsG6AV5wZ5Z9A5r61N4l3REg3i9uZKd4C6s1fdrMOPobDBBcoh6MjwrRkAdwlUQDr1J/I2wX5+Xp9p6PFCMqwPKVofXpAI7ZxNBRRRTM1Qrvrw5besjqk8CxOnct/3+Ye0MPMueeg5YQQl5kjHIIxuiVaIwjs+PztqG1BNX8lpIZOK3l1yGNU0mALtb5TO/v+aF1yuGBAHwn5HpXIEqZ4X7kF8IEWL/JW3z7A3d4r21cP81ooa+R/IyTpmyuOk0=~4604209~3552056',
                    '_ga_K1TVK44QHB': 'GS1.1.1728370681.1.1.1728372577.50.0.2041217341',
                    'bm_sv': '765B895B19E066A7720B9683455F6D99~YAAQtjtAFwC8uzKSAQAAS8kIaxlvR4XMwVHmm/DNVUg5UbiMMVGC/DI5+GBs1IJBdTpneZwfCG8/oprDfdJ4KLA6bSmPsMZnnYSvnNfdsrRdqOjvqVJLXtxUdjrU6V33YLuUy00LoclLS9IoKBkugxlY0OugRXQb0zwF9YqOH925JgwxW7LWqHZTx5AGctA7tqETZngvaCN2/shu5ngl2WcvbgCRa5mKWzyIK+DEjvYBcReGxwVQoCJxR4plR4fjYEHvtvzB2lU=~1',
                    'OptanonConsent': 'isGpcEnabled=0&datestamp=Tue+Oct+08+2024+12%3A59%3A38+GMT%2B0530+(India+Standard+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=3858a981-3841-4324-9d01-f40bb5a93130&interactionCount=2&landingPath=NotLandingPage&GPPCookiesCount=1&groups=TB01%3A1%2CTB02%3A1%2CTB04%3A1%2CTB05%3A0%2CTB03%3A1&AwaitingReconsent=false&geolocation=IN%3BGJ',
                    'RT': '"z=1&dm=www.menswearhouse.com&si=1c698350-fc62-40d2-9471-4cbf9b58f96b&ss=m202z4v6&sl=h&tt=2efe&bcn=%2F%2F684d0d43.akstat.io%2F&ld=1fkty"',
                    '_ga': 'GA1.2.1177378052.1728370179',
                    '_uetsid': '7ca84940854111efa04c77be25279b84',
                    '_uetvid': '7ca85790854111ef92124311f0e59d43',
                    '_scid_r': '4g6WtwE6sj5WXuR-jnf5BNmTUjF4W_wDSW8GUg',
                    'kampyleSessionPageCounter': '6',
                    'qb_permanent': 'ov8gcmixlz4-0m202z81d-54brjps:17:7:1:5:0::0:1:0:BnBNYD:BnBN9k:::::27.109.10.106:ahmedabad:148387:india:IN:23.01:72.51:surat%20metropolitan%20region:356008:gujarat:10002:migrated|1728370179247:GDKy==D=CyGF=GM::ZJrCJ7t:ZJrBAqf:0:0:0::0:0:.menswearhouse.com:0',
                    'qb_session': '17:1:31:GDKy=D:0:ZJq4/nQ:0:0:0:0:.menswearhouse.com',
                    '_clsk': '3wr79n%7C1728372580726%7C18%7C1%7Cx.clarity.ms%2Fcollect',
                    '_ga_MHWJKZ4QP6': 'GS1.1.1728370177.1.1.1728372580.47.0.0',
                    'shq': '638639693963493280%5E01926ae9-7208-44fc-b34f-584e6a4a3ed4%5E01926ae9-7208-4454-a9a5-4214e6ffa8c8%5E0%5E27.109.10.106',
                }

                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'no-cache',
                    # 'cookie': 'OTGPPConsent=DBABLA~BAAAAAAAAgA.QA; tracker_device_is_opt_in=true; _evga_2566={%22uuid%22:%2263f76ea4a1db707a%22}; _sfid_545b={%22anonymousId%22:%2263f76ea4a1db707a%22%2C%22consents%22:[]}; __ogfpid=b4a2d007-6ebf-4397-b861-3915a67aa306; _qubitTracker=ov8gcmixlz4-0m202z81d-54brjps; uuid=8af25971-723d-49f5-8c79-c7083c9f641f; qb_generic=:ZJq4/Vv:.menswearhouse.com; _caid=c92970f5-8dbe-4eb9-8bcf-aaf4fdf42555; _cavisit=1926ae3f68a|; __spdt=f3bc232110444e458ffe9eee26ef2fae; tracker_device=efa3435a-a5b7-4d45-b80d-435909047d4f; _fbp=fb.1.1728370177867.998967294524775655; _scid=6Y6WtwE6sj5WXuR-jnf5BNmTUjF4W_wD; mdLogger=false; kampyle_userid=9d8d-2835-0162-84c2-8777-6177-1472-6f12; _tt_enable_cookie=1; _ttp=a0zyCB5VfHdvzLX-pCmxfgsh1jc; _ScCbts=%5B%5D; _abck=CD201A23DB03CDC315761AD507007BEC~0~YAAQJF3SF0foOmaSAQAAtinkagwYxC1yfB2DFCkaISCRQX1GUaA4oG/HmJ59D16gZUvSbeXT14azkxjVzgYENqD10jl9hSLj3eu6Iu7+Bi4DGuaM3wb/W8aEzhgwHx4w8FMa/hM6LaJa/413ToodUJ4tztsE+/+8uAokntfi3sJsi57Vawuo6qof7iOGAYkN34PNTkkUEXzG20VjMPBtNhiBHMsNew8eDpoeLlgON122DTiNAzxr4DbOIawWN0U5QOwn30I2NN/MPDZKfSMwTsZeO4OtBoI/+SO7OFXTsIz4ReymSiTOMtFupnIJ3Fd9xYXwGFNz+ptSDqs/cpAJhKvACiJHjLTRAPSdS9P/2qyiB2SCodF79Nh6ikUKkydOqrKD6+tnqtvMFwO7Lg+/Te/kzg9R+OUvGVlaM7D0zMrCMwUJzXLI7sK/fvQjy52Ukv/WCqNaVX3/pznWeqCwWw==~-1~||0||~-1; _pin_unauth=dWlkPU9EbGtOMlk1WkRBdE1qZ3paaTAwT0RneExUZ3hOelV0TWpGaFpqaGxOelZtT0RjMw; tkbl_session=c5d3b591-7db4-41d9-bbb7-748a35c26241; _gid=GA1.2.284748098.1728370179; _gcl_au=1.1.1511323504.1728370179; tfpsi=b61c0932-dbf0-4c66-9d9c-a72c5aabd3fd; _clck=rtrgnf%7C2%7Cfpu%7C0%7C1742; styliticsWidgetSession=8b835622-f73b-47c6-a438-84fe947fe158; _sctr=1%7C1728325800000; AKA_A2=A; baseUrl=https://www.menswearhouse.com/api; akacd_TMW=3905823326~rv=21~id=ac3f827479a111190b71120f9f91daf1; bm_ss=ab8e18ef4e; page_store_locator=%7B%22current_zipcode%22%3A%2277019%22%7D; xyz_cr_1200_et_111==NaN&cr=1200&wegc=&et=111&ap=; qb_241282_bounceUrl=/; show_sale_icon=tooltip_flag; OptanonAlertBoxClosed=2024-10-08T06:55:32.991Z; bm_mi=3B8680F088C137A9ED6151F172CDCB67~YAAQJF3SF4mhPGaSAQAAFI3rahnwBlz7sW15BpB74ideDHpitF5gVbYt1LS2+YQ90HvV65P0D0xMTOSjZytE34yZQ0VxSWafI9sMWc7U7kAZH4eXZUjOZrmVFlzzEAguVEDHmDtMpm8zMZqNZfa/XpGDCgjh8cV/QZsKg1AmJqvZ5BtDkMn6758bWwA63vF3dTvh9J95lgd0KS6Iw0b+Oze79vxHVGr8yYUUAlr3mVFS/Ou2TETlhmr6HxJhlBLy2CTTg2PrzoPu7XX2NHj1VwGH4zMSDPUP31MwQbFWVCrQbo167DCPknmQ8S99yGUq/yBW3oA=~1; bm_so=D1F10D19C2B4433684488099F55177E59057FE7925CE6B6E507E1D712267321A~YAAQJF3SF4uhPGaSAQAAFI3ragFH9eD3/jMeuz6hlOM3nixP+YIgpTJpKuh2RLu1AcJlHCxlzE3o+WeubOuUKlB0ftYF1+bZikVsY+tV6mNjFE7y9jSZ/3GtxDyPUBrhXcl+JT2K2GTB8aJKWe2+GQ5AIstTJDtpjg9hinV7Y80Cl5A+cIIfV8v4oRi+aGoCwx5eMCAQCUWeLHK2j+5Pgksn8eGqx8Y9tUPomzmAZ8Aq4lhEZY1fk/Hz8u+O1zPU6nVPVlFEL1LvqZ1/MLYBxCU2aa5Ja4jm1ToxhhVLeLXgHkVOZChu4wDh8xIWJVIi79w6NHbaec5GCy5hl7DTia3r0aE4kX0kYpTuHCzOHWh525ko388IGmZslzAffBTfQAbEEpQEypLzG36ejyH3WqPly9q5lqJqSq5eWPvMgxWPiONVkfs5i2zymYU1mNSPgKymFprMtl3N70WdH4BrLKTI5aA+; ak_bmsc=A3ED5052C9C8ABFC5FE462EB355BBFB7~000000000000000000000000000000~YAAQJF3SF+WjPGaSAQAAA5jrahlYgfQqbIIf+LsSadytm1RLYMSDsPECb7CuL47/FEh6mGU4LeWwfvIygtcystpSEZk77hb7M5yXlsfml8ig0g1JDuU4aVY2xuxPdmUOMT0x0lZGwgdZcda/QGowy0JX9Y8MAKhkTin0PoRs2H0oz8aLY6mwDKlBikTHmOyTvPbO/8USQydgbMq90IU0/7PMd4OSZN8dXx4vvpzld71B6OOJfkp3a+1vVugpKxiN9HQ98Z4bd0Zy6YB8abQnLbSM65qvMQrU+v7Qpiqthb3W+HbGgM2hh+X0pwTzY6dvVOnaoZ3W8Q6hJbaGy9TJtGh9nV+FuBVkpZypFL10QuoyLVscBy5g4P4bVW0yWtBE779bNLmJLuLc6r3aoTBpMrk2nmp+K6p2ZCl1lTCYMMd29wM0w8+oQHjqWi5+Wh6f5djvMi32Jb5xUxlFM6m1NYLAVcOQjW5Pjs3rikbq+E3nEuKoK/4=; bm_lso=D1F10D19C2B4433684488099F55177E59057FE7925CE6B6E507E1D712267321A~YAAQJF3SF4uhPGaSAQAAFI3ragFH9eD3/jMeuz6hlOM3nixP+YIgpTJpKuh2RLu1AcJlHCxlzE3o+WeubOuUKlB0ftYF1+bZikVsY+tV6mNjFE7y9jSZ/3GtxDyPUBrhXcl+JT2K2GTB8aJKWe2+GQ5AIstTJDtpjg9hinV7Y80Cl5A+cIIfV8v4oRi+aGoCwx5eMCAQCUWeLHK2j+5Pgksn8eGqx8Y9tUPomzmAZ8Aq4lhEZY1fk/Hz8u+O1zPU6nVPVlFEL1LvqZ1/MLYBxCU2aa5Ja4jm1ToxhhVLeLXgHkVOZChu4wDh8xIWJVIi79w6NHbaec5GCy5hl7DTia3r0aE4kX0kYpTuHCzOHWh525ko388IGmZslzAffBTfQAbEEpQEypLzG36ejyH3WqPly9q5lqJqSq5eWPvMgxWPiONVkfs5i2zymYU1mNSPgKymFprMtl3N70WdH4BrLKTI5aA+^1728370669354; bounceClientVisit4803v=N4IgNgDiBcIBYBcEQM4FIDMBBNAmAYnvgO6kB0AtgKYB2KxVAhgE5wD2ArilWQMZsUimfCmYowCAJYATTABEsAMwoAhAPJtmAR2YBJABoBrAAy8qAWgBGAcQAsAGQDCurMwgBORgCVdAVQAKvggA7HK8ALLGagBecooA0rheisQA6ubuuioAmgAe9gDKIAA0IMwwIKTEZADmbGw1YDz8FCUgkigA+nWd3Cgokmw0MIqMYNylHd0QvVT9g8PQo+NUAL5AA; bm_s=YAAQJF3SF0GyPGaSAQAAQsbragIeb4BrF8Tl0enp6U1e8jeDZ9JjCtIFBSsgFFYs4I3uYeCIFgY4cQ6KNcSyGWL0d14XugalDggFMnB2k2y/oDkgv/s9n9Qxn3O2bSmv0QKEE1RDr67/ACzHNQ0RN56jGRQJWZq5IOJxd27dKOjIn68ZwJBKkZzGu77+JDjOcPpiZ5O1ns7OOCRbcE7pcu4DGldlol+kibTmgor+yCZAMO/H1rAdNsomHZPVe81N5+3IGc2LIhKXzTIbQlZmkwecq0CB0e1xTajp0170YBEfoEnNaFgXd6HBsEoAMBTu4N7HwNpR4cz05d2OA4Gke7ddVbrtXfxSJs52xjo=; qb_241282_bounce=2; dtm_token_sc=AQAL5TOEBpNc1wFK71o0AQA-FwABAQCTa-5o5wEBAJNr7mjn; dtm_token=AQAL5TOEBpNc1wFK71o0AQA-FwABAQCTa-5o5wEBAJNr7mjn; DECLINED_DATE=1728371503393; kampylePageLoadedTimestamp=1728371503405; kampyleUserSession=1728372290471; kampyleUserSessionsCount=10; kampyleUserPercentile=88.38699675134765; bm_sz=CDBAD502FBEC94C7B87D91D3FDDDB09D~YAAQtjtAF7K7uzKSAQAAlMMIaxlwNhMoLkt7d4YPKOwVGbWdl15HEr9AikkPZ1rcY2el9NUjD0AWpS8GVzJbdOw8VahsQMFJLNbDIW7tCdotH8XMaNvlEWBL7p99QZpJvIlz/Z8r+c5OiL25buFDuDSpVxXUy6hGqqyi2oyCZL1sdO3A1HuH7Bl23Jq4N0+/IOxUelJo+GyAqPJsf9FYSFmC1GvV+YOQeOdOUABRHLskF8EnU/f7rkq7AYlYoI75teoI8kAmH3b2wQFM8Kgqosei+oBJpWMXOf/RlO7/NGQXEFhQmsG6AV5wZ5Z9A5r61N4l3REg3i9uZKd4C6s1fdrMOPobDBBcoh6MjwrRkAdwlUQDr1J/I2wX5+Xp9p6PFCMqwPKVofXpAI7ZxNBRRRTM1Qrvrw5besjqk8CxOnct/3+Ye0MPMueeg5YQQl5kjHIIxuiVaIwjs+PztqG1BNX8lpIZOK3l1yGNU0mALtb5TO/v+aF1yuGBAHwn5HpXIEqZ4X7kF8IEWL/JW3z7A3d4r21cP81ooa+R/IyTpmyuOk0=~4604209~3552056; _ga_K1TVK44QHB=GS1.1.1728370681.1.1.1728372577.50.0.2041217341; bm_sv=765B895B19E066A7720B9683455F6D99~YAAQtjtAFwC8uzKSAQAAS8kIaxlvR4XMwVHmm/DNVUg5UbiMMVGC/DI5+GBs1IJBdTpneZwfCG8/oprDfdJ4KLA6bSmPsMZnnYSvnNfdsrRdqOjvqVJLXtxUdjrU6V33YLuUy00LoclLS9IoKBkugxlY0OugRXQb0zwF9YqOH925JgwxW7LWqHZTx5AGctA7tqETZngvaCN2/shu5ngl2WcvbgCRa5mKWzyIK+DEjvYBcReGxwVQoCJxR4plR4fjYEHvtvzB2lU=~1; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Oct+08+2024+12%3A59%3A38+GMT%2B0530+(India+Standard+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=3858a981-3841-4324-9d01-f40bb5a93130&interactionCount=2&landingPath=NotLandingPage&GPPCookiesCount=1&groups=TB01%3A1%2CTB02%3A1%2CTB04%3A1%2CTB05%3A0%2CTB03%3A1&AwaitingReconsent=false&geolocation=IN%3BGJ; RT="z=1&dm=www.menswearhouse.com&si=1c698350-fc62-40d2-9471-4cbf9b58f96b&ss=m202z4v6&sl=h&tt=2efe&bcn=%2F%2F684d0d43.akstat.io%2F&ld=1fkty"; _ga=GA1.2.1177378052.1728370179; _uetsid=7ca84940854111efa04c77be25279b84; _uetvid=7ca85790854111ef92124311f0e59d43; _scid_r=4g6WtwE6sj5WXuR-jnf5BNmTUjF4W_wDSW8GUg; kampyleSessionPageCounter=6; qb_permanent=ov8gcmixlz4-0m202z81d-54brjps:17:7:1:5:0::0:1:0:BnBNYD:BnBN9k:::::27.109.10.106:ahmedabad:148387:india:IN:23.01:72.51:surat%20metropolitan%20region:356008:gujarat:10002:migrated|1728370179247:GDKy==D=CyGF=GM::ZJrCJ7t:ZJrBAqf:0:0:0::0:0:.menswearhouse.com:0; qb_session=17:1:31:GDKy=D:0:ZJq4/nQ:0:0:0:0:.menswearhouse.com; _clsk=3wr79n%7C1728372580726%7C18%7C1%7Cx.clarity.ms%2Fcollect; _ga_MHWJKZ4QP6=GS1.1.1728370177.1.1.1728372580.47.0.0; shq=638639693963493280%5E01926ae9-7208-44fc-b34f-584e6a4a3ed4%5E01926ae9-7208-4454-a9a5-4214e6ffa8c8%5E0%5E27.109.10.106',
                    'pragma': 'no-cache',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
                }

                url=storeLocatorLink
                yield scrapy.Request(url=url,
                                     # cookies=cookies,
                                     # headers=headers,
                                     callback=self.parse,
                                     meta={
                                            "url":storeLocatorLink,
                                           "subcat_name":subcat_name,
                                           "subcat_link":subcat_link,
                                           "state":state,
                                         "full_file_path":full_file_path,
                                         "unique_id":unique_id
                                           }
                                     )

            else:
                print(full_file_path)
                yield scrapy.Request(
                    url="file:///" + full_file_path,
                    callback=self.parse,
                    meta={
                          "url": storeLocatorLink,
                          "subcat_name": subcat_name,
                          "subcat_link": subcat_link,
                          "state": state,
                        "full_file_path": full_file_path,
                        "unique_id": unique_id
                          }
                )

    def parse(self, response):
        url = response.meta['url']
        state = response.meta['state']
        subcat_link = response.meta['subcat_link']
        subcat_name = response.meta['subcat_name']
        full_file_path=response.meta['full_file_path']
        unique_id=response.meta['unique_id']
        item = Store_Details_Item()

        try:
            json_ld=json.loads(response.xpath('//script[@type="application/ld+json"][2]/text()').extract_first().strip().replace('\n','').replace('\t',''))
            if not os.path.isfile(full_file_path) and response.status==200:
                with gzip.open(full_file_path, "wb") as f:
                    f.write(response.body)


            Name=get_name(json_ld)
            Latitude=get_lat(json_ld)
            Longitude=get_long(json_ld)
            Street=get_street(json_ld)
            City=get_city(json_ld)
            Country=get_country(json_ld)
            Zip_Code=get_zipcode(json_ld)
            Address=f"{Street}, {City}, {Country}, {Zip_Code}"
            Phone=get_phone(json_ld)
            Open_Hour=get_opening_hours(json_ld)
            if '- -' in Open_Hour:
                Open_Hour= 'N/A'
            else:
                Open_Hour=Open_Hour
            URL=url
            Email='N/A'
            Provider ="Men's Wearhouse Wholesale"
            Banner=get_banner(json_ld)
            today_date = datetime.today().strftime('%d-%m-%Y')
            Updated_Date = today_date

            Direction_URL=get_direction(response)
            item['Name']=Name
            item['Latitude']=Latitude
            item['Longitude']=Longitude
            item['Street'] = Street
            item['City'] = City
            item['Country'] = Country
            item['Zip_Code'] = Zip_Code
            item['Address'] = Address
            item['Phone'] = Phone
            item['Open_Hour'] = Open_Hour
            item['URL'] = URL
            item['Email'] = Email
            item['Provider'] = Provider
            item['Banner'] = Banner
            item['Updated_Date'] = Updated_Date
            if Open_Hour == 'N/A':
                item['Status'] = 'close'
            else:
                item['Status'] = 'open'

            item['Direction_URL'] = Direction_URL
            item['state'] = state
            item['subcat_link'] = subcat_link
            item['subcat_name'] = subcat_name
            item['unique_id'] = unique_id


            yield item

        except Exception as e:
            if os.path.isfile(full_file_path):
                with gzip.open(full_file_path, 'rb') as f:
                    extracted_content = f.read()
            extracted_selector = Selector(text=extracted_content.decode('utf-8'))
            json_ld = json.loads(extracted_selector.xpath('//script[@type="application/ld+json"][2]/text()').extract_first().strip().replace('\n',
                                                                                                                  '').replace( '\t', ''))

            Name = get_name(json_ld)
            Latitude = get_lat(json_ld)
            Longitude = get_long(json_ld)
            Street = get_street(json_ld)
            City = get_city(json_ld)
            Country = get_country(json_ld)
            Zip_Code = get_zipcode(json_ld)
            Address = f"{Street}, {City}, {Country}, {Zip_Code}"
            Phone = get_phone(json_ld)
            Open_Hour = get_opening_hours(json_ld)
            if '- -' in Open_Hour:
                Open_Hour= 'N/A'
            else:
                Open_Hour= Open_Hour
            URL = url
            Email = 'N/A'
            Provider ="Men's Wearhouse Wholesale"
            Banner = get_banner(json_ld)
            today_date = datetime.today().strftime('%d-%m-%Y')
            Updated_Date = today_date

            Direction_URL = get_direction(extracted_selector)
            item['Name'] = Name
            item['Latitude'] = Latitude
            item['Longitude'] = Longitude
            item['Street'] = Street
            item['City'] = City
            item['Country'] = Country
            item['Zip_Code'] = Zip_Code
            item['Address'] = Address
            item['Phone'] = Phone
            item['Open_Hour'] = Open_Hour
            item['URL'] = URL
            item['Email'] = Email
            item['Provider'] = Provider
            item['Banner'] = Banner
            item['Updated_Date'] = Updated_Date
            if Open_Hour == 'N/A':

                item['Status'] = 'close'
            else:
                item['Status'] = 'open'
            item['Direction_URL'] = Direction_URL
            item['state'] = state
            item['subcat_link'] = subcat_link
            item['subcat_name'] = subcat_name
            item['unique_id'] = unique_id

            yield item

if __name__ == '__main__':
    execute('scrapy crawl store_details'.split())
