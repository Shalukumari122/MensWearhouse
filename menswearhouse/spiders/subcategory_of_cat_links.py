import pymysql
import scrapy
from scrapy.cmdline import execute

from menswearhouse.items import storeLocatorLinks_Items


class SubcategoryOfCatLinksSpider(scrapy.Spider):
    name = "subcategory_of_cat_links"
    # allowed_domains = ["www.menswearhouse.com"]
    # start_urls = ["https://www.menswearhouse.com"]

    def __init__(self):
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='mens_wearhouse_db'
        )
        self.cursor = self.conn.cursor()

    def start_requests(self):
        query="select *from store_subcat_links where status='pending'"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            state=row[1]
            subcat_name=row[2]
            subcat_link=row[3]

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
                'BA': '"ba=11662103&be=4831660.79&l=105&le=35.77&ip=27.109.10.0&t=1728390674"',
                'akacd_TMW': '3905903797~rv=66~id=c484ebb26435e46b932c9fc4acc4ad83',
                '_abck': 'C3524F4078B680CB204BEF51AB9BC7C0~0~YAAQzjtAFzY06m+SAQAA6Di1bwyyJ8+isfD0YnlXyJbqF29CXNTUpwxnpx9fpA/272XOHjOgUpa/gKMkotrTINJpSu+7bjz2R0KFqR+vDTFQsQmQs0vDLKAGzUBKUWi8bluad2DpfGWRDKWOUiObh7syDC9EXaXSRj9Gd2LzU8dZfwbhq6NQIr2DccEZ4UEZi5wugIDFYGck4QfCMrLhByEkdKPZvGJMKM9c4YmiRhhgotCmf2LwzlgafdYB3HW2Iz5zhdAL+cnxD5iS42gnU2tL2WCL1f7dZ19BWps2biFV9LTb3Bal5H0L94lNkPbLpzfhs60is2MjORzj3xJ/wptkTIuIuMYgfeShD3DHC9BkjabnuawhHj2xOsU9kV1JQ83aMXGpAqZ7upI6Kei4NuGxWotnBYQdXq0YfnPcfUFSJmm7aRtp7k6WnGvyc+lAfVY3OwmGTvYEW70VabpnIQ==~-1~-1~-1',
                'baseUrl': 'https%3A%2F%2Fwww.menswearhouse.com%2Fapi',
                'page_store_locator': '%7B%22current_zipcode%22%3A%2277019%22%7D',
                '_cavisit': '1926fb52501|',
                'tfpsi': '02a1ca5a-4445-43ab-88fe-a16d46fda69b',
                '_clck': 'lm1p7v%7C2%7Cfpv%7C0%7C1742',
                'bounceClientVisit4803v': 'N4IgNgDiBcIBYBcEQM4FIDMBBNAmAYnvgO6kB0AtgKYB2KxVAhgE5wD2ArilWQMZsUiKBG2ZUAtGDa9GI5iAA0IebEUgAligD6AczZbuKFOrY0YAM0ZhuSzbogGqRk2eiXrVAL5A',
                'bm_mi': '9E87A983D9CE77A93D48033E2812C637~YAAQFtYsF/eBgGiSAQAA2f7NbxnHPMabqpEc8xQp4bW2KYPAjk55xSJgEmYKHv0cQl2wDNgTsfGFd3w8qXMv6TOsmU0e2PEMvALd9tS7IownghoNPVr936JmBvQhPNZyjcaGAAmDr8bb9E52Yj4SWnU0v11JM6yQJ3wW4Ybw99WrZxwtNVbyK5m5p3Cam3IDpUpKs1VZmLrXE4c/V8HeIslAIqjYI8ET0EfJvPmZYsnzIszoHl3B4UKiViQpHL6SVTb8yRafgTk8jv9UQFVJj4bC6Rq+FtJQlPBGb2H8TEH8fMb9nWElDjQzmWcvE1M4SF8nbL78+lq8+psaZVn0tHqLWL7Z~1',
                'ak_bmsc': '0BB8DAB5B1E1824BDEE022EB287C019D~000000000000000000000000000000~YAAQFtYsFwKEgGiSAQAAKxLObxlfocUGfmJFPizCkCasJ5XP2rUcLznDp7CY+7GHPxTPa2XKi0GnlwOiKTDMryA05S35bgOjw/hJn8y3t1k8VuU+s6nq0EEboW3ZamvG2sQ1jLy06OzEtbIfQHnlip3qav9SkjNKoHh5n4P92os33S53Q+v2Tii+0pU9m7FeYHLthBQz65ypYWrgRi0Be2pDJzAZ43XWuRv08JPrDDbHsJ2nPklV0AJv0WKGsjFuqn2e/0mu1weWZDVG7DqXfhlnt3mdn4bHNC/G10PqltMrMTfMGJvi9Fs9p/84OUz5PQ83OiRnz+qNKrhIkwo7SBXfrxdyDWthl7VaWxvv+VtcA4WIZKdrE8vek9TF8NZQSRzhInS2ioknjqFrOKwHxMFo2Sfp5zRxDHBQhMJ5yWNNxLgakcln6ux9quTmavqIkmQkIw40OekCK1LuKMmNjhE6oVI3K6XvRCLzZYjz9raiA0j/dxGnIISfS/mvStrlCguOP1DV',
                'AKA_A2': 'A',
                'kampyleUserSession': '1728452623013',
                'kampyleUserSessionsCount': '5',
                'kampyleUserPercentile': '41.4526705204141',
                'bm_sz': '03986F941D896018ED7030CB11DAA8F5~YAAQFtYsF9zRgGiSAQAAQwPRbxnTgE/BsT4a1D2XBWwUYxNrV0b1d39nLEKmnJUrCJNtlppnnI85hgfTAFqFYWmFNL4q1Dd8n97ef/M3sM9YOTJ6kURgQLxs6ptovcn0GPChIFmtOdpFKJ4YVVtP22uua6xJqH+eFioVOzoTfRcdM0rA8pBaU8k/GrZr9wQ2LSkmjQNreTB7tLmyJtKK9xQ69vm+oOGmiiBguhynKg0Dgv2NQ+w9IkspW3hROy0LRQvGdsVl8AFruvJYwT+0BFXp19LUjV5YuoNnmbla4QsArD3c+xsi9SDiMOeim4aMN/rNPv7W5r+l6APBmqSNO1dZD5ysy9Hyc1xvOvBfuzfAVV9H8WNtLp/WMNAamuXg6iurXle8NYej2Ui+BUDXeGTczIvVBmN5oI9eC3pa6cdbAvWENU05EaEw55Rz~3293748~3748409',
                '_ga_K1TVK44QHB': 'GS1.1.1728450988.5.1.1728452809.60.0.2080093365',
                'bm_sv': '3E20456AE02D6BF9C1FA5A6B5287175B~YAAQFtYsF8fTgGiSAQAAMxbRbxlA4Hw7KC1OpdcJEusZdD0YRe1hYOgSt0rvsVosqAzV/7B3/chjxZARawPvyTl+mm0z3ceAKNuRBcjhYRpAqQyift3/AN2oqufV542B4wlckjfeHIst/wOLTtUkovIWU1hyL4Lc65khumcKYcx0bqeggAILJ1+ZyA76gp2irNKPP+MAf2dQT2Wyt3+HigQyqkAb9tfmTUTZJHUt+K3yBynQJdedDBy0JS6SO2lQys+PeNPtkQ==~1',
                'OptanonConsent': 'isGpcEnabled=0&datestamp=Wed+Oct+09+2024+11%3A16%3A54+GMT%2B0530+(India+Standard+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=30c68090-a4e0-4f6e-93da-42a317c43dba&interactionCount=1&landingPath=NotLandingPage&GPPCookiesCount=1&groups=TB01%3A1%2CTB02%3A1%2CTB04%3A1%2CTB05%3A0%2CTB03%3A1&geolocation=IN%3BGJ&AwaitingReconsent=false',
                '_ga': 'GA1.2.978142608.1728390304',
                '_scid_r': 'Y5kfJFTIUV3EwFQCGUDVdJ4u267NHcBa1DjiuQ',
                'qb_permanent': '3wpmldbl1ta-0m20eyu8i-kok0lt4:7:3:2:2:0::0:1:0:BnBSSs:BnBhjQ:A::::27.109.10.106:ahmedabad:148387:india:IN:23.01:72.51:surat%20metropolitan%20region:356008:gujarat:10002:migrated|1728450998679:::ZJv0OuY:ZJvtSvL:0:0:0::0:0:.menswearhouse.com:0',
                'qb_session': '3:1:7::0:ZJvtSvL:0:0:0:0:.menswearhouse.com',
                'kampyleSessionPageCounter': '2',
                '_uetsid': '5d874b30857011efb74857f67b80356b',
                '_uetvid': '5d883170857011ef80804163968d23f0',
                '_ga_MHWJKZ4QP6': 'GS1.1.1728450988.3.1.1728452817.52.0.0',
                'shq': '638640496323184552%5E01926fb5-6e21-44c3-8c01-113c56e39666%5E01926fce-3a8d-4151-b8ea-45805761d172%5E0%5E27.109.10.106',
                'RT': '"z=1&dm=www.menswearhouse.com&si=1c698350-fc62-40d2-9471-4cbf9b58f96b&ss=m21f36z7&sl=0&tt=0&bcn=%2F%2F684d0d42.akstat.io%2F"',
                '_clsk': 'xbdt2l%7C1728454353078%7C8%7C1%7Cx.clarity.ms%2Fcollect',
            }

            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                # 'cookie': '_evga_2566={%22uuid%22:%2269b6af6d1a7ccf6a%22}; tracker_device_is_opt_in=true; tracker_device=bce25493-cfb5-4c55-935c-445320546cdf; OTGPPConsent=DBABLA~BAAAAAAAAgA.QA; _gcl_au=1.1.27769973.1728390310; __ogfpid=34bb3f6b-be57-4329-a99d-d30de261ce3b; uuid=5f74a697-a3fd-45b1-919e-5695d4d92110; _sfid_545b={%22anonymousId%22:%2269b6af6d1a7ccf6a%22%2C%22consents%22:[]}; _gid=GA1.2.73111737.1728390311; _caid=db2e2499-d13a-4d2c-ab5c-9f3560b2246e; __spdt=9472a2ed174a4c01bbce37feee3e4b71; _scid=VBkfJFTIUV3EwFQCGUDVdJ4u267NHcBa; mdLogger=false; kampyle_userid=8242-4585-fe61-dece-1c43-9b7c-2410-cce3; _tt_enable_cookie=1; _ttp=tf8wrc6qjnoushJC41G0p2ol9i_; _pin_unauth=dWlkPU9UVXdPVEk1TXprdFpEaGtNaTAwTnpFeUxXRmtPREl0Wm1FM016RTBaakE0Tm1Jdw; tkbl_session=21eccb8a-3c5d-45cf-b7df-92a6070dd785; _fbp=fb.1.1728390313030.121540280461962365; _ScCbts=%5B%5D; _sctr=1%7C1728325800000; _qubitTracker=3wpmldbl1ta-0m20eyu8i-kok0lt4; qb_generic=:ZJsFzuQ:.menswearhouse.com; OptanonAlertBoxClosed=2024-10-08T12:25:24.765Z; xyz_cr_1200_et_111==NaN&cr=1200&wegc=&et=111&ap=; BA="ba=11662103&be=4831660.79&l=105&le=35.77&ip=27.109.10.0&t=1728390674"; akacd_TMW=3905903797~rv=66~id=c484ebb26435e46b932c9fc4acc4ad83; _abck=C3524F4078B680CB204BEF51AB9BC7C0~0~YAAQzjtAFzY06m+SAQAA6Di1bwyyJ8+isfD0YnlXyJbqF29CXNTUpwxnpx9fpA/272XOHjOgUpa/gKMkotrTINJpSu+7bjz2R0KFqR+vDTFQsQmQs0vDLKAGzUBKUWi8bluad2DpfGWRDKWOUiObh7syDC9EXaXSRj9Gd2LzU8dZfwbhq6NQIr2DccEZ4UEZi5wugIDFYGck4QfCMrLhByEkdKPZvGJMKM9c4YmiRhhgotCmf2LwzlgafdYB3HW2Iz5zhdAL+cnxD5iS42gnU2tL2WCL1f7dZ19BWps2biFV9LTb3Bal5H0L94lNkPbLpzfhs60is2MjORzj3xJ/wptkTIuIuMYgfeShD3DHC9BkjabnuawhHj2xOsU9kV1JQ83aMXGpAqZ7upI6Kei4NuGxWotnBYQdXq0YfnPcfUFSJmm7aRtp7k6WnGvyc+lAfVY3OwmGTvYEW70VabpnIQ==~-1~-1~-1; baseUrl=https%3A%2F%2Fwww.menswearhouse.com%2Fapi; page_store_locator=%7B%22current_zipcode%22%3A%2277019%22%7D; _cavisit=1926fb52501|; tfpsi=02a1ca5a-4445-43ab-88fe-a16d46fda69b; _clck=lm1p7v%7C2%7Cfpv%7C0%7C1742; bounceClientVisit4803v=N4IgNgDiBcIBYBcEQM4FIDMBBNAmAYnvgO6kB0AtgKYB2KxVAhgE5wD2ArilWQMZsUiKBG2ZUAtGDa9GI5iAA0IebEUgAligD6AczZbuKFOrY0YAM0ZhuSzbogGqRk2eiXrVAL5A; bm_mi=9E87A983D9CE77A93D48033E2812C637~YAAQFtYsF/eBgGiSAQAA2f7NbxnHPMabqpEc8xQp4bW2KYPAjk55xSJgEmYKHv0cQl2wDNgTsfGFd3w8qXMv6TOsmU0e2PEMvALd9tS7IownghoNPVr936JmBvQhPNZyjcaGAAmDr8bb9E52Yj4SWnU0v11JM6yQJ3wW4Ybw99WrZxwtNVbyK5m5p3Cam3IDpUpKs1VZmLrXE4c/V8HeIslAIqjYI8ET0EfJvPmZYsnzIszoHl3B4UKiViQpHL6SVTb8yRafgTk8jv9UQFVJj4bC6Rq+FtJQlPBGb2H8TEH8fMb9nWElDjQzmWcvE1M4SF8nbL78+lq8+psaZVn0tHqLWL7Z~1; ak_bmsc=0BB8DAB5B1E1824BDEE022EB287C019D~000000000000000000000000000000~YAAQFtYsFwKEgGiSAQAAKxLObxlfocUGfmJFPizCkCasJ5XP2rUcLznDp7CY+7GHPxTPa2XKi0GnlwOiKTDMryA05S35bgOjw/hJn8y3t1k8VuU+s6nq0EEboW3ZamvG2sQ1jLy06OzEtbIfQHnlip3qav9SkjNKoHh5n4P92os33S53Q+v2Tii+0pU9m7FeYHLthBQz65ypYWrgRi0Be2pDJzAZ43XWuRv08JPrDDbHsJ2nPklV0AJv0WKGsjFuqn2e/0mu1weWZDVG7DqXfhlnt3mdn4bHNC/G10PqltMrMTfMGJvi9Fs9p/84OUz5PQ83OiRnz+qNKrhIkwo7SBXfrxdyDWthl7VaWxvv+VtcA4WIZKdrE8vek9TF8NZQSRzhInS2ioknjqFrOKwHxMFo2Sfp5zRxDHBQhMJ5yWNNxLgakcln6ux9quTmavqIkmQkIw40OekCK1LuKMmNjhE6oVI3K6XvRCLzZYjz9raiA0j/dxGnIISfS/mvStrlCguOP1DV; AKA_A2=A; kampyleUserSession=1728452623013; kampyleUserSessionsCount=5; kampyleUserPercentile=41.4526705204141; bm_sz=03986F941D896018ED7030CB11DAA8F5~YAAQFtYsF9zRgGiSAQAAQwPRbxnTgE/BsT4a1D2XBWwUYxNrV0b1d39nLEKmnJUrCJNtlppnnI85hgfTAFqFYWmFNL4q1Dd8n97ef/M3sM9YOTJ6kURgQLxs6ptovcn0GPChIFmtOdpFKJ4YVVtP22uua6xJqH+eFioVOzoTfRcdM0rA8pBaU8k/GrZr9wQ2LSkmjQNreTB7tLmyJtKK9xQ69vm+oOGmiiBguhynKg0Dgv2NQ+w9IkspW3hROy0LRQvGdsVl8AFruvJYwT+0BFXp19LUjV5YuoNnmbla4QsArD3c+xsi9SDiMOeim4aMN/rNPv7W5r+l6APBmqSNO1dZD5ysy9Hyc1xvOvBfuzfAVV9H8WNtLp/WMNAamuXg6iurXle8NYej2Ui+BUDXeGTczIvVBmN5oI9eC3pa6cdbAvWENU05EaEw55Rz~3293748~3748409; _ga_K1TVK44QHB=GS1.1.1728450988.5.1.1728452809.60.0.2080093365; bm_sv=3E20456AE02D6BF9C1FA5A6B5287175B~YAAQFtYsF8fTgGiSAQAAMxbRbxlA4Hw7KC1OpdcJEusZdD0YRe1hYOgSt0rvsVosqAzV/7B3/chjxZARawPvyTl+mm0z3ceAKNuRBcjhYRpAqQyift3/AN2oqufV542B4wlckjfeHIst/wOLTtUkovIWU1hyL4Lc65khumcKYcx0bqeggAILJ1+ZyA76gp2irNKPP+MAf2dQT2Wyt3+HigQyqkAb9tfmTUTZJHUt+K3yBynQJdedDBy0JS6SO2lQys+PeNPtkQ==~1; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Oct+09+2024+11%3A16%3A54+GMT%2B0530+(India+Standard+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=30c68090-a4e0-4f6e-93da-42a317c43dba&interactionCount=1&landingPath=NotLandingPage&GPPCookiesCount=1&groups=TB01%3A1%2CTB02%3A1%2CTB04%3A1%2CTB05%3A0%2CTB03%3A1&geolocation=IN%3BGJ&AwaitingReconsent=false; _ga=GA1.2.978142608.1728390304; _scid_r=Y5kfJFTIUV3EwFQCGUDVdJ4u267NHcBa1DjiuQ; qb_permanent=3wpmldbl1ta-0m20eyu8i-kok0lt4:7:3:2:2:0::0:1:0:BnBSSs:BnBhjQ:A::::27.109.10.106:ahmedabad:148387:india:IN:23.01:72.51:surat%20metropolitan%20region:356008:gujarat:10002:migrated|1728450998679:::ZJv0OuY:ZJvtSvL:0:0:0::0:0:.menswearhouse.com:0; qb_session=3:1:7::0:ZJvtSvL:0:0:0:0:.menswearhouse.com; kampyleSessionPageCounter=2; _uetsid=5d874b30857011efb74857f67b80356b; _uetvid=5d883170857011ef80804163968d23f0; _ga_MHWJKZ4QP6=GS1.1.1728450988.3.1.1728452817.52.0.0; shq=638640496323184552%5E01926fb5-6e21-44c3-8c01-113c56e39666%5E01926fce-3a8d-4151-b8ea-45805761d172%5E0%5E27.109.10.106; RT="z=1&dm=www.menswearhouse.com&si=1c698350-fc62-40d2-9471-4cbf9b58f96b&ss=m21f36z7&sl=0&tt=0&bcn=%2F%2F684d0d42.akstat.io%2F"; _clsk=xbdt2l%7C1728454353078%7C8%7C1%7Cx.clarity.ms%2Fcollect',
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

            yield scrapy.Request(url=subcat_link,
                                 # cookies=cookies,
                                 #    headers=headers,
                                 callback=self.parse,
                                 meta={"state":state,
                                       "subcat_link":subcat_link,
                                       "subcat_name":subcat_name})
    def parse(self, response):
        print()
        state=response.meta['state']
        subcat_link=response.meta['subcat_link']
        subcat_name=response.meta['subcat_name']

        item=storeLocatorLinks_Items()
        storeLocatorLinks=response.xpath('//div[@class="sb-directory"]/ul/li')
        for each_storeLocatorLink in storeLocatorLinks:
            storeLocatorLink=each_storeLocatorLink.xpath('.//div[@class="nearby_details"]//div[@class="location_nearby_info_ctas"]//a[@id="storeLocatorLink"]/@href').extract_first()
            item['state']=state
            item['subcat_link'] = subcat_link
            item['subcat_name'] = subcat_name
            item['storeLocatorLink']=f'https://www.menswearhouse.com{storeLocatorLink}'
            yield item

        try:
            update_query = "UPDATE store_subcat_links SET status = 'Done' WHERE status = 'pending' and subcat_link = %s"
            print(f"Updating with state: {subcat_link}")  # Debugging
            self.cursor.execute(update_query, (subcat_link,))
            self.conn.commit()

        except Exception as e:
            print(f"Error updating store_subcat_links: {e}, State: {subcat_link}")


if __name__ == '__main__':
    execute('scrapy crawl subcategory_of_cat_links'.split())


