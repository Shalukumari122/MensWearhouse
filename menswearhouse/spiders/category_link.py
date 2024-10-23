import pymysql
import scrapy
from scrapy.cmdline import execute
from fake_useragent import UserAgent
ua = UserAgent()


from menswearhouse.items import Store_cat_links_Item


class CategoryLinkSpider(scrapy.Spider):
    name = "category_link"
    allowed_domains = ["www.menswearhouse.com"]
    start_urls = ["https://www.menswearhouse.com"]
    def __init__(self):
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='mens_wearhouse_db'
        )
        self.cursor = self.conn.cursor()

    def start_requests(self):


        cookies = {
            '_evga_2566': '{%22uuid%22:%2269b6af6d1a7ccf6a%22}',
            'tracker_device_is_opt_in': 'true',
            'tracker_device': 'bce25493-cfb5-4c55-935c-445320546cdf',
            'OTGPPConsent': 'DBABLA~BAAAAAAAAgA.QA',
            '_gcl_au': '1.1.27769973.1728390310',
            '__ogfpid': '34bb3f6b-be57-4329-a99d-d30de261ce3b',
            'uuid': '5f74a697-a3fd-45b1-919e-5695d4d92110',
            '_sfid_545b': '{%22anonymousId%22:%2269b6af6d1a7ccf6a%22%2C%22consents%22:[]}',
            '_gid': 'GA1.2.73111737.1728390311',
            '_caid': 'db2e2499-d13a-4d2c-ab5c-9f3560b2246e',
            '__spdt': '9472a2ed174a4c01bbce37feee3e4b71',
            '_scid': 'VBkfJFTIUV3EwFQCGUDVdJ4u267NHcBa',
            'mdLogger': 'false',
            'kampyle_userid': '8242-4585-fe61-dece-1c43-9b7c-2410-cce3',
            '_tt_enable_cookie': '1',
            '_ttp': 'tf8wrc6qjnoushJC41G0p2ol9i_',
            '_pin_unauth': 'dWlkPU9UVXdPVEk1TXprdFpEaGtNaTAwTnpFeUxXRmtPREl0Wm1FM016RTBaakE0Tm1Jdw',
            'tkbl_session': '21eccb8a-3c5d-45cf-b7df-92a6070dd785',
            '_fbp': 'fb.1.1728390313030.121540280461962365',
            '_ScCbts': '%5B%5D',
            '_sctr': '1%7C1728325800000',
            '_qubitTracker': '3wpmldbl1ta-0m20eyu8i-kok0lt4',
            'qb_generic': ':ZJsFzuQ:.menswearhouse.com',
            'OptanonAlertBoxClosed': '2024-10-08T12:25:24.765Z',
            'xyz_cr_1200_et_111': '=NaN&cr=1200&wegc=&et=111&ap=',
            'akacd_TMW': '3905903797~rv=66~id=c484ebb26435e46b932c9fc4acc4ad83',
            'baseUrl': 'https%3A%2F%2Fwww.menswearhouse.com%2Fapi',
            'page_store_locator': '%7B%22current_zipcode%22%3A%2277019%22%7D',
            '_clck': 'lm1p7v%7C2%7Cfpv%7C0%7C1742',
            'dtm_token_sc': 'AQAIomInQpgSLgESPtseAQA-FwABAQCTcSxEbgEBAJNxLERu',
            'dtm_token': 'AQAIomInQpgSLgESPtseAQA-FwABAQCTcSxEbgEBAJNxLERu',
            'AKA_A2': 'A',
            '_abck': 'C3524F4078B680CB204BEF51AB9BC7C0~0~YAAQBtYsF4O8yGqSAQAAzmugcAxs/LPK+WxPNRMXaAebO315nnit1uqGXr0D3OJx58XeCgZXAKC7zMBajjw+kHoNPgKbj1Msbk3vPEHxrGJwivQxubBJtPFVhTAkXt5wzMY34P5DV9Ch9MGjzUKA8LruH87iWXN/ca4ufDX0f2jzI32fH9Lap7f1xlVgCTYCqmzFwTCUSVWq1PJvCeKtJTxAbgCDBi120WsHFvtQjKA/1FTj14UuoaZCi5TJ/I8BKXUPRxt0/dvNtUUXOWqGurmk/Jod0CpPrm7EtzSOQCI9pBr/+kelrvgklJZn1/GwRl7O9Q3D1vJLX66uaw87l8ao7vD8m+7pR0mUZDDREYXoBykZRC/ngm3XWfNePGMBxRZRFTP/Y4LgvUbVH4AnWROGKZ0fBBL86MO9+zwHsDhm0faixk+H70/YOhx8USDyknreNmbI+9CnpwTFcK0x/A==~-1~-1~-1',
            'ak_bmsc': '84A09A2E795D903EEA3FF4B969851123~000000000000000000000000000000~YAAQBtYsF/i/yGqSAQAAdXWgcBkUq4y7BqXFIl7Oz4GRVtoLhowTwbiLaPzPDuZx17hqnZdkjq6BuLexdFl0hqDRrI/C2TmhzkxtsTLLIQ0e4XWvakW75DaL+IY3/OZp5Y0jAXdCcLUN4Ns0UbQxhfI+JZiCJkMlUDTUgWhMrONlVXpi0PjRQqdL8QI5iBJKVXaZ8YWtejsRXKmaYztnJlyHCLai9OneejWFiQFF9nFZLztbTaQTF8M5kGNFhFX/f4NQaee4IWrZC5VSNgMaa12ZRa0yden932IK5Th5gxUb3HbSDCkhYGeFq4DSP6MBTg6VSJnS3NpAmI5PWZeHnekPUWfwSA67gDZRrctK/fA2n008bxVaWSg/e1IDy3pdpwznRYTk05PZLI2MShY/tCgAsWa6S0YQ/TUFl8k6vp32nBk1W6AxuCy4zMWC1HHkOiFr0PnG9nPTs8+vGK1ryre7+g==',
            '_cavisit': '19270a04bb1|',
            'bounceClientVisit4803v': 'N4IgNgDiBcIBYBcEQM4FIDMBBNAmAYnvgO6kB0AtgKYB2KxVAhgE5wD2ArilWQMZsUiKBG2ZUAtGDa9GI5iAA0IebEUgAligD6AczZbuKFOrY0YAM0ZhuSzbogGqRk2eiXrVAL5A',
            'tfpsi': '0c75f518-b8b6-4fc8-ad37-a6b12ffb55f1',
            '__idcontext': 'eyJjb29raWVJRCI6IjJuQzl2YTlLM2dFUFBGRlNRTm54REZxS09MTiIsImRldmljZUlEIjoiMm5DOXZWa3E2YWVpVlhDcXZBSktCV3lqOXJ1IiwiaXYiOiIiLCJ2IjoiIn0%3D',
            'BA': '"ba=6569462&be=1327343.52&l=121&le=10.52&ip=95.142.120.0&t=1728467527"',
            'bm_sz': '6505377F98EFC737AB32D99AF2FC8863~YAAQFtYsFywemGiSAQAARSGzcBlPwuKZ+YwFkcmZKPzZE3w1+hdmFATJdy0UVbSi4EuzMiIWOETrXAe8EsgJbL42H5uwE+fcoLdUt7uI5LVKT2gmZtS1sLeyYAI1h93GHfGqBjctyR0/v9GClJx/WCwitL4MuxrskR37zxou54AIi6X9nhAmzyTCySV0AdpeAmzs2CRQHrFrVZyiJRw+dAtonidapW11FHfWWbaTYsQuiJ6fyIIEYT8FVKL57BVZiEW3BwyJzNntiCTBCe/ZfkqDE7mxVQYhGZn6KykZluCa6tLBRHoqQlTzhNEEP8f6jyLEBLv2Sig6EKyUvy5D60sTfMt0JCntMkq1XTsNRCnkx3uutFJmd9V4ji9BkK2zJ0MZ1OphL5/LVpyU8Y7omuOxkmFRR/MSci49RkCzZfuDamcYATcOXN3HMcxc~3749186~4342839',
            '_ga_K1TVK44QHB': 'GS1.1.1728466403.7.1.1728467628.59.0.824478866',
            'ADRUM_BT': 'R:43|i:3413674|g:57ab5fc0-b9e0-48a1-af27-ed70cf387418410921|e:2|n:tailored_7ab4fb5a-bc42-4886-9014-fa91e5a32512',
            'bm_sv': 'A73EC9560109333CC0C434C0CA929272~YAAQFtYsF5UemGiSAQAAfyezcBkmTQ+5+/oPbtblszbf9SAd8zjPjpjMMydAaD3HHtr17PsdwYu2emRooY7qPLNwNrWcr9igCqxPo1d3BausqZUOi0Dc3EQeTMDscMan9bhTD4LMmnyatWuq9uG7R62GmCKWiX9RAN76txkp+eExW+a+XEUDueS2NEDimEhm56+b2tJCKOCEpq2Sd+jY9PITkyMXBJeX/qE6GjME4sivHk7tPVKtLm7raSEUyPR3uQN1DttEjw==~1',
            'OptanonConsent': 'isGpcEnabled=0&datestamp=Wed+Oct+09+2024+15%3A23%3A49+GMT%2B0530+(India+Standard+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=30c68090-a4e0-4f6e-93da-42a317c43dba&interactionCount=1&landingPath=NotLandingPage&GPPCookiesCount=1&groups=TB01%3A1%2CTB02%3A1%2CTB04%3A1%2CTB05%3A0%2CTB03%3A1&geolocation=IN%3BGJ&AwaitingReconsent=false',
            'RT': '"z=1&dm=www.menswearhouse.com&si=1c698350-fc62-40d2-9471-4cbf9b58f96b&ss=m21oxc2t&sl=2&tt=aoj&bcn=%2F%2F684d0d4b.akstat.io%2F&ld=2nns"',
            '_ga': 'GA1.2.978142608.1728390304',
            '_gat_UA-30527783-1': '1',
            'qb_permanent': '3wpmldbl1ta-0m20eyu8i-kok0lt4:16:4:4:4:0::0:1:0:BnBSSs:BnBlKv:A::::95.142.120.24:sturgeon%20bay:16021:united%20states:US:44.84:-87.4:green%20bay-appleton:658:wisconsin:50:migrated|1728466409771:::ZJwsv0N:ZJwoFfz:0:0:0::0:0:.menswearhouse.com:0',
            'qb_session': '4:1:6::0:ZJwoFfz:0:0:0:0:.menswearhouse.com',
            '_uetsid': '5d874b30857011efb74857f67b80356b',
            '_uetvid': '5d883170857011ef80804163968d23f0',
            'kampyleUserSession': '1728467631635',
            'kampyleUserSessionsCount': '12',
            'kampyleSessionPageCounter': '1',
            'kampyleUserPercentile': '79.29705420357772',
            '_scid_r': 'bZkfJFTIUV3EwFQCGUDVdJ4u267NHcBa1Djiwg',
            '_ga_MHWJKZ4QP6': 'GS1.1.1728466403.5.1.1728467632.48.0.0',
            '_clsk': 'th0nqz%7C1728467632890%7C4%7C1%7Co.clarity.ms%2Fcollect',
            'shq': '638640644504298563%5E01926fb5-6e21-44c3-8c01-113c56e39666%5E019270a0-947c-4104-b07b-725d518f8504%5E0%5E27.109.10.106',
        }

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            # 'cookie': '_evga_2566={%22uuid%22:%2269b6af6d1a7ccf6a%22}; tracker_device_is_opt_in=true; tracker_device=bce25493-cfb5-4c55-935c-445320546cdf; OTGPPConsent=DBABLA~BAAAAAAAAgA.QA; _gcl_au=1.1.27769973.1728390310; __ogfpid=34bb3f6b-be57-4329-a99d-d30de261ce3b; uuid=5f74a697-a3fd-45b1-919e-5695d4d92110; _sfid_545b={%22anonymousId%22:%2269b6af6d1a7ccf6a%22%2C%22consents%22:[]}; _gid=GA1.2.73111737.1728390311; _caid=db2e2499-d13a-4d2c-ab5c-9f3560b2246e; __spdt=9472a2ed174a4c01bbce37feee3e4b71; _scid=VBkfJFTIUV3EwFQCGUDVdJ4u267NHcBa; mdLogger=false; kampyle_userid=8242-4585-fe61-dece-1c43-9b7c-2410-cce3; _tt_enable_cookie=1; _ttp=tf8wrc6qjnoushJC41G0p2ol9i_; _pin_unauth=dWlkPU9UVXdPVEk1TXprdFpEaGtNaTAwTnpFeUxXRmtPREl0Wm1FM016RTBaakE0Tm1Jdw; tkbl_session=21eccb8a-3c5d-45cf-b7df-92a6070dd785; _fbp=fb.1.1728390313030.121540280461962365; _ScCbts=%5B%5D; _sctr=1%7C1728325800000; _qubitTracker=3wpmldbl1ta-0m20eyu8i-kok0lt4; qb_generic=:ZJsFzuQ:.menswearhouse.com; OptanonAlertBoxClosed=2024-10-08T12:25:24.765Z; xyz_cr_1200_et_111==NaN&cr=1200&wegc=&et=111&ap=; akacd_TMW=3905903797~rv=66~id=c484ebb26435e46b932c9fc4acc4ad83; baseUrl=https%3A%2F%2Fwww.menswearhouse.com%2Fapi; page_store_locator=%7B%22current_zipcode%22%3A%2277019%22%7D; _clck=lm1p7v%7C2%7Cfpv%7C0%7C1742; dtm_token_sc=AQAIomInQpgSLgESPtseAQA-FwABAQCTcSxEbgEBAJNxLERu; dtm_token=AQAIomInQpgSLgESPtseAQA-FwABAQCTcSxEbgEBAJNxLERu; AKA_A2=A; _abck=C3524F4078B680CB204BEF51AB9BC7C0~0~YAAQBtYsF4O8yGqSAQAAzmugcAxs/LPK+WxPNRMXaAebO315nnit1uqGXr0D3OJx58XeCgZXAKC7zMBajjw+kHoNPgKbj1Msbk3vPEHxrGJwivQxubBJtPFVhTAkXt5wzMY34P5DV9Ch9MGjzUKA8LruH87iWXN/ca4ufDX0f2jzI32fH9Lap7f1xlVgCTYCqmzFwTCUSVWq1PJvCeKtJTxAbgCDBi120WsHFvtQjKA/1FTj14UuoaZCi5TJ/I8BKXUPRxt0/dvNtUUXOWqGurmk/Jod0CpPrm7EtzSOQCI9pBr/+kelrvgklJZn1/GwRl7O9Q3D1vJLX66uaw87l8ao7vD8m+7pR0mUZDDREYXoBykZRC/ngm3XWfNePGMBxRZRFTP/Y4LgvUbVH4AnWROGKZ0fBBL86MO9+zwHsDhm0faixk+H70/YOhx8USDyknreNmbI+9CnpwTFcK0x/A==~-1~-1~-1; ak_bmsc=84A09A2E795D903EEA3FF4B969851123~000000000000000000000000000000~YAAQBtYsF/i/yGqSAQAAdXWgcBkUq4y7BqXFIl7Oz4GRVtoLhowTwbiLaPzPDuZx17hqnZdkjq6BuLexdFl0hqDRrI/C2TmhzkxtsTLLIQ0e4XWvakW75DaL+IY3/OZp5Y0jAXdCcLUN4Ns0UbQxhfI+JZiCJkMlUDTUgWhMrONlVXpi0PjRQqdL8QI5iBJKVXaZ8YWtejsRXKmaYztnJlyHCLai9OneejWFiQFF9nFZLztbTaQTF8M5kGNFhFX/f4NQaee4IWrZC5VSNgMaa12ZRa0yden932IK5Th5gxUb3HbSDCkhYGeFq4DSP6MBTg6VSJnS3NpAmI5PWZeHnekPUWfwSA67gDZRrctK/fA2n008bxVaWSg/e1IDy3pdpwznRYTk05PZLI2MShY/tCgAsWa6S0YQ/TUFl8k6vp32nBk1W6AxuCy4zMWC1HHkOiFr0PnG9nPTs8+vGK1ryre7+g==; _cavisit=19270a04bb1|; bounceClientVisit4803v=N4IgNgDiBcIBYBcEQM4FIDMBBNAmAYnvgO6kB0AtgKYB2KxVAhgE5wD2ArilWQMZsUiKBG2ZUAtGDa9GI5iAA0IebEUgAligD6AczZbuKFOrY0YAM0ZhuSzbogGqRk2eiXrVAL5A; tfpsi=0c75f518-b8b6-4fc8-ad37-a6b12ffb55f1; __idcontext=eyJjb29raWVJRCI6IjJuQzl2YTlLM2dFUFBGRlNRTm54REZxS09MTiIsImRldmljZUlEIjoiMm5DOXZWa3E2YWVpVlhDcXZBSktCV3lqOXJ1IiwiaXYiOiIiLCJ2IjoiIn0%3D; BA="ba=6569462&be=1327343.52&l=121&le=10.52&ip=95.142.120.0&t=1728467527"; bm_sz=6505377F98EFC737AB32D99AF2FC8863~YAAQFtYsFywemGiSAQAARSGzcBlPwuKZ+YwFkcmZKPzZE3w1+hdmFATJdy0UVbSi4EuzMiIWOETrXAe8EsgJbL42H5uwE+fcoLdUt7uI5LVKT2gmZtS1sLeyYAI1h93GHfGqBjctyR0/v9GClJx/WCwitL4MuxrskR37zxou54AIi6X9nhAmzyTCySV0AdpeAmzs2CRQHrFrVZyiJRw+dAtonidapW11FHfWWbaTYsQuiJ6fyIIEYT8FVKL57BVZiEW3BwyJzNntiCTBCe/ZfkqDE7mxVQYhGZn6KykZluCa6tLBRHoqQlTzhNEEP8f6jyLEBLv2Sig6EKyUvy5D60sTfMt0JCntMkq1XTsNRCnkx3uutFJmd9V4ji9BkK2zJ0MZ1OphL5/LVpyU8Y7omuOxkmFRR/MSci49RkCzZfuDamcYATcOXN3HMcxc~3749186~4342839; _ga_K1TVK44QHB=GS1.1.1728466403.7.1.1728467628.59.0.824478866; ADRUM_BT=R:43|i:3413674|g:57ab5fc0-b9e0-48a1-af27-ed70cf387418410921|e:2|n:tailored_7ab4fb5a-bc42-4886-9014-fa91e5a32512; bm_sv=A73EC9560109333CC0C434C0CA929272~YAAQFtYsF5UemGiSAQAAfyezcBkmTQ+5+/oPbtblszbf9SAd8zjPjpjMMydAaD3HHtr17PsdwYu2emRooY7qPLNwNrWcr9igCqxPo1d3BausqZUOi0Dc3EQeTMDscMan9bhTD4LMmnyatWuq9uG7R62GmCKWiX9RAN76txkp+eExW+a+XEUDueS2NEDimEhm56+b2tJCKOCEpq2Sd+jY9PITkyMXBJeX/qE6GjME4sivHk7tPVKtLm7raSEUyPR3uQN1DttEjw==~1; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Oct+09+2024+15%3A23%3A49+GMT%2B0530+(India+Standard+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=30c68090-a4e0-4f6e-93da-42a317c43dba&interactionCount=1&landingPath=NotLandingPage&GPPCookiesCount=1&groups=TB01%3A1%2CTB02%3A1%2CTB04%3A1%2CTB05%3A0%2CTB03%3A1&geolocation=IN%3BGJ&AwaitingReconsent=false; RT="z=1&dm=www.menswearhouse.com&si=1c698350-fc62-40d2-9471-4cbf9b58f96b&ss=m21oxc2t&sl=2&tt=aoj&bcn=%2F%2F684d0d4b.akstat.io%2F&ld=2nns"; _ga=GA1.2.978142608.1728390304; _gat_UA-30527783-1=1; qb_permanent=3wpmldbl1ta-0m20eyu8i-kok0lt4:16:4:4:4:0::0:1:0:BnBSSs:BnBlKv:A::::95.142.120.24:sturgeon%20bay:16021:united%20states:US:44.84:-87.4:green%20bay-appleton:658:wisconsin:50:migrated|1728466409771:::ZJwsv0N:ZJwoFfz:0:0:0::0:0:.menswearhouse.com:0; qb_session=4:1:6::0:ZJwoFfz:0:0:0:0:.menswearhouse.com; _uetsid=5d874b30857011efb74857f67b80356b; _uetvid=5d883170857011ef80804163968d23f0; kampyleUserSession=1728467631635; kampyleUserSessionsCount=12; kampyleSessionPageCounter=1; kampyleUserPercentile=79.29705420357772; _scid_r=bZkfJFTIUV3EwFQCGUDVdJ4u267NHcBa1Djiwg; _ga_MHWJKZ4QP6=GS1.1.1728466403.5.1.1728467632.48.0.0; _clsk=th0nqz%7C1728467632890%7C4%7C1%7Co.clarity.ms%2Fcollect; shq=638640644504298563%5E01926fb5-6e21-44c3-8c01-113c56e39666%5E019270a0-947c-4104-b07b-725d518f8504%5E0%5E27.109.10.106',
            # 'pragma': 'no-cache',
            # 'priority': 'u=0, i',
            'user-agent': ua.random,
        }

        yield scrapy.Request(url='https://www.menswearhouse.com/store-locator',
                             # cookies=cookies,
                             # headers=headers,
                             callback=self.parse,
                             dont_filter=True,
                             # meta={"state":state}
                             )

    def parse(self, response):
        print()
        # state=response.meta['state']
        item=Store_cat_links_Item()
        query = "select *from states_store_locator where status='pending'"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            state = row[1]
            link=response.xpath(f'//div[@class="sb-directory"]/ul[@id="stateslist"]/li/a[contains(text(), "{state}")]/@href').extract_first()
            if link:
                link=f'https://www.menswearhouse.com{link}'
                item['cat_link']=link
                item['state'] =state
                yield item
            else:
                print(f"No link found for {state}")


if __name__ == '__main__':
    execute('scrapy crawl category_link'.split())
