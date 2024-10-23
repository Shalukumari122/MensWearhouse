import pymysql
import scrapy
from scrapy.cmdline import execute

from menswearhouse.items import Store_subcat_links_Item


class SubcategoryLinksSpider(scrapy.Spider):
    name = "subcategory_links"
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
        query="select *from store_cat_links where status='pending' "
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            cat_link=row[1]
            state=row[2]

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
                'kampyleUserSession': '1728450996626',
                'kampyleUserSessionsCount': '4',
                'kampyleSessionPageCounter': '1',
                'kampyleUserPercentile': '6.960052747211587',
                'tfpsi': '02a1ca5a-4445-43ab-88fe-a16d46fda69b',
                '_clck': 'lm1p7v%7C2%7Cfpv%7C0%7C1742',
                'qb_permanent': '3wpmldbl1ta-0m20eyu8i-kok0lt4:5:1:2:2:0::0:1:0:BnBSSs:BnBhG1:A::::27.109.10.106:ahmedabad:148387:india:IN:23.01:72.51:surat%20metropolitan%20region:356008:gujarat:10002:migrated|1728450998679:::ZJvtTGX:ZJvtSvL:0:0:0::0:0:.menswearhouse.com:0',
                'qb_session': '1:1:5::0:ZJvtSvL:0:0:0:0:.menswearhouse.com',
                'bounceClientVisit4803v': 'N4IgNgDiBcIBYBcEQM4FIDMBBNAmAYnvgO6kB0AtgKYB2KxVAhgE5wD2ArilWQMZsUiKBG2ZUAtGDa9GI5iAA0IebEUgAligD6AczZbuKFOrY0YAM0ZhuSzbogGqRk2eiXrVAL5A',
                '_clsk': 'xbdt2l%7C1728451000806%7C1%7C1%7Cx.clarity.ms%2Fcollect',
                'shq': '638640478143966161%5E01926fb5-6e21-44c3-8c01-113c56e39666%5E01926fb5-6e21-4823-bb1b-fbac15fe4d87%5E0%5E27.109.10.106',
                'bm_mi': '9E87A983D9CE77A93D48033E2812C637~YAAQFtYsF8RvgGiSAQAAtnjNbxkh/+wncV3qv20fEuebCLibs1HigN1XC660N8tmey5bMHNuGJSCg8rRztaWiSUC7v/DtfwgZGeHCVSVWUyVcEN45BR86lT2F3Liil6TbtnSa4YYIdZLOLfPJimIS/q537ygfSkTrlrFfMmDGaoktmTestCS9tlqMsRjawLanu63vOmxekFXkQqHXV/ancZNdr3yq4pDy6PjaVM3VVoJwHjmD6qsEbGI0GdyNfaBYZbIi3TPLT7sU/YDCd0DNLq4Q1HPD9jPXbp880DYkCUh9PZst2hgnIfzSDEcXxdLry3Ypw3lDZqL6pjEX2bB05ZczeFd~1',
                'bm_sz': '03986F941D896018ED7030CB11DAA8F5~YAAQFtYsF8ZvgGiSAQAAtnjNbxmxJzeYfHWzXNIp7F8EwKcC61vyH0RNdAGFJp+q6u71TmsgdfigmGNA+GBXW90RWws3C+uh6gSW0wADavx4156NbzntDSfdrQXWEL/rmvIwLgyhrEe6LwELioeeCi7i2zbpdvAtRGf5ndo+IE0cQuHoYPti5r9ne0nRbfKZ9Zrd/X0SQQPii9kPBY0NaYcEED0SzuR1sT+kjQqpTs3Zr5sw04uRZiPHzjL0oX7yQecTJyd/TwDSm+UVP+dpipWKZo4+WsFGfyu1yB+00nyrvMj/iBt1H7Ra335T+JL3Ir1LeWgHItMAC3teRc37hGHKnyfSKUgN62fDXGuGuVMShbhXUAgrzARjcg/OhbnvJv/9aYW/2CiVtUNxWv/F8Gxw0tNiZT/xwRS4Ceddoh8=~3293748~3748409',
                '_ga_K1TVK44QHB': 'GS1.1.1728450988.5.1.1728452588.60.0.2080093365',
                'ADRUM_BT': 'R:46|i:3413674|g:57ab5fc0-b9e0-48a1-af27-ed70cf387418386793|e:2|n:tailored_7ab4fb5a-bc42-4886-9014-fa91e5a32512',
                'ak_bmsc': '356B56C875B5DCF1C9BE3065057A5511~000000000000000000000000000000~YAAQFtYsFxp4gGiSAQAAf6zNbxm2pbHEivX/53QjktPjEsCumRE8ECH3kQKBfnfx0SSmF3Ym4Ydd1mZQqDDv0psync+jIiNKC8WVSs6dcSKnoTgezX85v3mRfKjmW5bpeODGnLI9HuKZYfr1Wel+/ClMVBiOJUH7CX/DhD9inoe0mLgny+dBuc3c+Sihr/OZVze1zYhkCN3cFjArL4FX3QpXNlJ1DgcjXI+p9zC9iOVEDmWeqMo7qpI0U9fGFqfXJVhNKNI0xLxgKvBno+nvUJOnuUdxqAjUqGj2r/V9MMCpdp8p/Dt8UvVP8mSJGx2YSLO7QWlChFJNM3xU9xqH3+iKNRRN/PJNhOYEOVNr8vq3N3Mhegd02AsIlty4f1q374rhbCafX7Jwkw6S17YEfCjHs145i375db43zRsFo87n+8y7pegHu9qXnHv9Ldaaix6P6Alhf19579Du7DWQchXwXPm8x73hlpUtnX3OkzaFODEfi0Cc2OgigMIDDJiSBg0yS8DmXPJ3KkkQ',
                'bm_sv': '3E20456AE02D6BF9C1FA5A6B5287175B~YAAQFtYsFxt4gGiSAQAAf6zNbxl6KFkq2zhVoXBWCMUBdg7MJ5bIDoZfr+bCLGBlWAj3hW0ZmoZcn87xPidnmvLrokA2aY6nnv8FV9DI4JOOxj+4xomwxl6Mt6zHZyjlK36IznYALdO54/c0K/d1iHltw5zF2kYxhE9zDCMUAmVp9p8Yo+sB2Yf58HoWh+GIKXO7sOPB+jq3MSyZ5pWDllx9683Ygg+or4d49l4EhZmVBbJpSm8JFETxxkqED1qUZxgtu0SPgw==~1',
                'OptanonConsent': 'isGpcEnabled=0&datestamp=Wed+Oct+09+2024+11%3A13%3A10+GMT%2B0530+(India+Standard+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=30c68090-a4e0-4f6e-93da-42a317c43dba&interactionCount=1&landingPath=NotLandingPage&GPPCookiesCount=1&groups=TB01%3A1%2CTB02%3A1%2CTB04%3A1%2CTB05%3A0%2CTB03%3A1&geolocation=IN%3BGJ&AwaitingReconsent=false',
                'RT': '"z=1&dm=www.menswearhouse.com&si=1c698350-fc62-40d2-9471-4cbf9b58f96b&ss=m21f36z7&sl=1&tt=iea&bcn=%2F%2F684d0d45.akstat.io%2F&ld=yhlz"',
                '_ga_MHWJKZ4QP6': 'GS1.1.1728450988.3.1.1728452593.55.0.0',
                '_ga': 'GA1.2.978142608.1728390304',
                '_gat_UA-30527783-1': '1',
                '_scid_r': 'YZkfJFTIUV3EwFQCGUDVdJ4u267NHcBa1Djitw',
                '_uetsid': '5d874b30857011efb74857f67b80356b',
                '_uetvid': '5d883170857011ef80804163968d23f0',
            }

            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                # 'cookie': '_evga_2566={%22uuid%22:%2269b6af6d1a7ccf6a%22}; tracker_device_is_opt_in=true; tracker_device=bce25493-cfb5-4c55-935c-445320546cdf; OTGPPConsent=DBABLA~BAAAAAAAAgA.QA; _gcl_au=1.1.27769973.1728390310; __ogfpid=34bb3f6b-be57-4329-a99d-d30de261ce3b; uuid=5f74a697-a3fd-45b1-919e-5695d4d92110; _sfid_545b={%22anonymousId%22:%2269b6af6d1a7ccf6a%22%2C%22consents%22:[]}; _gid=GA1.2.73111737.1728390311; _caid=db2e2499-d13a-4d2c-ab5c-9f3560b2246e; __spdt=9472a2ed174a4c01bbce37feee3e4b71; _scid=VBkfJFTIUV3EwFQCGUDVdJ4u267NHcBa; mdLogger=false; kampyle_userid=8242-4585-fe61-dece-1c43-9b7c-2410-cce3; _tt_enable_cookie=1; _ttp=tf8wrc6qjnoushJC41G0p2ol9i_; _pin_unauth=dWlkPU9UVXdPVEk1TXprdFpEaGtNaTAwTnpFeUxXRmtPREl0Wm1FM016RTBaakE0Tm1Jdw; tkbl_session=21eccb8a-3c5d-45cf-b7df-92a6070dd785; _fbp=fb.1.1728390313030.121540280461962365; _ScCbts=%5B%5D; _sctr=1%7C1728325800000; _qubitTracker=3wpmldbl1ta-0m20eyu8i-kok0lt4; qb_generic=:ZJsFzuQ:.menswearhouse.com; OptanonAlertBoxClosed=2024-10-08T12:25:24.765Z; xyz_cr_1200_et_111==NaN&cr=1200&wegc=&et=111&ap=; BA="ba=11662103&be=4831660.79&l=105&le=35.77&ip=27.109.10.0&t=1728390674"; akacd_TMW=3905903797~rv=66~id=c484ebb26435e46b932c9fc4acc4ad83; _abck=C3524F4078B680CB204BEF51AB9BC7C0~0~YAAQzjtAFzY06m+SAQAA6Di1bwyyJ8+isfD0YnlXyJbqF29CXNTUpwxnpx9fpA/272XOHjOgUpa/gKMkotrTINJpSu+7bjz2R0KFqR+vDTFQsQmQs0vDLKAGzUBKUWi8bluad2DpfGWRDKWOUiObh7syDC9EXaXSRj9Gd2LzU8dZfwbhq6NQIr2DccEZ4UEZi5wugIDFYGck4QfCMrLhByEkdKPZvGJMKM9c4YmiRhhgotCmf2LwzlgafdYB3HW2Iz5zhdAL+cnxD5iS42gnU2tL2WCL1f7dZ19BWps2biFV9LTb3Bal5H0L94lNkPbLpzfhs60is2MjORzj3xJ/wptkTIuIuMYgfeShD3DHC9BkjabnuawhHj2xOsU9kV1JQ83aMXGpAqZ7upI6Kei4NuGxWotnBYQdXq0YfnPcfUFSJmm7aRtp7k6WnGvyc+lAfVY3OwmGTvYEW70VabpnIQ==~-1~-1~-1; baseUrl=https%3A%2F%2Fwww.menswearhouse.com%2Fapi; page_store_locator=%7B%22current_zipcode%22%3A%2277019%22%7D; _cavisit=1926fb52501|; kampyleUserSession=1728450996626; kampyleUserSessionsCount=4; kampyleSessionPageCounter=1; kampyleUserPercentile=6.960052747211587; tfpsi=02a1ca5a-4445-43ab-88fe-a16d46fda69b; _clck=lm1p7v%7C2%7Cfpv%7C0%7C1742; qb_permanent=3wpmldbl1ta-0m20eyu8i-kok0lt4:5:1:2:2:0::0:1:0:BnBSSs:BnBhG1:A::::27.109.10.106:ahmedabad:148387:india:IN:23.01:72.51:surat%20metropolitan%20region:356008:gujarat:10002:migrated|1728450998679:::ZJvtTGX:ZJvtSvL:0:0:0::0:0:.menswearhouse.com:0; qb_session=1:1:5::0:ZJvtSvL:0:0:0:0:.menswearhouse.com; bounceClientVisit4803v=N4IgNgDiBcIBYBcEQM4FIDMBBNAmAYnvgO6kB0AtgKYB2KxVAhgE5wD2ArilWQMZsUiKBG2ZUAtGDa9GI5iAA0IebEUgAligD6AczZbuKFOrY0YAM0ZhuSzbogGqRk2eiXrVAL5A; _clsk=xbdt2l%7C1728451000806%7C1%7C1%7Cx.clarity.ms%2Fcollect; shq=638640478143966161%5E01926fb5-6e21-44c3-8c01-113c56e39666%5E01926fb5-6e21-4823-bb1b-fbac15fe4d87%5E0%5E27.109.10.106; bm_mi=9E87A983D9CE77A93D48033E2812C637~YAAQFtYsF8RvgGiSAQAAtnjNbxkh/+wncV3qv20fEuebCLibs1HigN1XC660N8tmey5bMHNuGJSCg8rRztaWiSUC7v/DtfwgZGeHCVSVWUyVcEN45BR86lT2F3Liil6TbtnSa4YYIdZLOLfPJimIS/q537ygfSkTrlrFfMmDGaoktmTestCS9tlqMsRjawLanu63vOmxekFXkQqHXV/ancZNdr3yq4pDy6PjaVM3VVoJwHjmD6qsEbGI0GdyNfaBYZbIi3TPLT7sU/YDCd0DNLq4Q1HPD9jPXbp880DYkCUh9PZst2hgnIfzSDEcXxdLry3Ypw3lDZqL6pjEX2bB05ZczeFd~1; bm_sz=03986F941D896018ED7030CB11DAA8F5~YAAQFtYsF8ZvgGiSAQAAtnjNbxmxJzeYfHWzXNIp7F8EwKcC61vyH0RNdAGFJp+q6u71TmsgdfigmGNA+GBXW90RWws3C+uh6gSW0wADavx4156NbzntDSfdrQXWEL/rmvIwLgyhrEe6LwELioeeCi7i2zbpdvAtRGf5ndo+IE0cQuHoYPti5r9ne0nRbfKZ9Zrd/X0SQQPii9kPBY0NaYcEED0SzuR1sT+kjQqpTs3Zr5sw04uRZiPHzjL0oX7yQecTJyd/TwDSm+UVP+dpipWKZo4+WsFGfyu1yB+00nyrvMj/iBt1H7Ra335T+JL3Ir1LeWgHItMAC3teRc37hGHKnyfSKUgN62fDXGuGuVMShbhXUAgrzARjcg/OhbnvJv/9aYW/2CiVtUNxWv/F8Gxw0tNiZT/xwRS4Ceddoh8=~3293748~3748409; _ga_K1TVK44QHB=GS1.1.1728450988.5.1.1728452588.60.0.2080093365; ADRUM_BT=R:46|i:3413674|g:57ab5fc0-b9e0-48a1-af27-ed70cf387418386793|e:2|n:tailored_7ab4fb5a-bc42-4886-9014-fa91e5a32512; ak_bmsc=356B56C875B5DCF1C9BE3065057A5511~000000000000000000000000000000~YAAQFtYsFxp4gGiSAQAAf6zNbxm2pbHEivX/53QjktPjEsCumRE8ECH3kQKBfnfx0SSmF3Ym4Ydd1mZQqDDv0psync+jIiNKC8WVSs6dcSKnoTgezX85v3mRfKjmW5bpeODGnLI9HuKZYfr1Wel+/ClMVBiOJUH7CX/DhD9inoe0mLgny+dBuc3c+Sihr/OZVze1zYhkCN3cFjArL4FX3QpXNlJ1DgcjXI+p9zC9iOVEDmWeqMo7qpI0U9fGFqfXJVhNKNI0xLxgKvBno+nvUJOnuUdxqAjUqGj2r/V9MMCpdp8p/Dt8UvVP8mSJGx2YSLO7QWlChFJNM3xU9xqH3+iKNRRN/PJNhOYEOVNr8vq3N3Mhegd02AsIlty4f1q374rhbCafX7Jwkw6S17YEfCjHs145i375db43zRsFo87n+8y7pegHu9qXnHv9Ldaaix6P6Alhf19579Du7DWQchXwXPm8x73hlpUtnX3OkzaFODEfi0Cc2OgigMIDDJiSBg0yS8DmXPJ3KkkQ; bm_sv=3E20456AE02D6BF9C1FA5A6B5287175B~YAAQFtYsFxt4gGiSAQAAf6zNbxl6KFkq2zhVoXBWCMUBdg7MJ5bIDoZfr+bCLGBlWAj3hW0ZmoZcn87xPidnmvLrokA2aY6nnv8FV9DI4JOOxj+4xomwxl6Mt6zHZyjlK36IznYALdO54/c0K/d1iHltw5zF2kYxhE9zDCMUAmVp9p8Yo+sB2Yf58HoWh+GIKXO7sOPB+jq3MSyZ5pWDllx9683Ygg+or4d49l4EhZmVBbJpSm8JFETxxkqED1qUZxgtu0SPgw==~1; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Oct+09+2024+11%3A13%3A10+GMT%2B0530+(India+Standard+Time)&version=202401.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=30c68090-a4e0-4f6e-93da-42a317c43dba&interactionCount=1&landingPath=NotLandingPage&GPPCookiesCount=1&groups=TB01%3A1%2CTB02%3A1%2CTB04%3A1%2CTB05%3A0%2CTB03%3A1&geolocation=IN%3BGJ&AwaitingReconsent=false; RT="z=1&dm=www.menswearhouse.com&si=1c698350-fc62-40d2-9471-4cbf9b58f96b&ss=m21f36z7&sl=1&tt=iea&bcn=%2F%2F684d0d45.akstat.io%2F&ld=yhlz"; _ga_MHWJKZ4QP6=GS1.1.1728450988.3.1.1728452593.55.0.0; _ga=GA1.2.978142608.1728390304; _gat_UA-30527783-1=1; _scid_r=YZkfJFTIUV3EwFQCGUDVdJ4u267NHcBa1Djitw; _uetsid=5d874b30857011efb74857f67b80356b; _uetvid=5d883170857011ef80804163968d23f0',
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

            yield scrapy.Request(url=cat_link,
                                 # cookies=cookies,
                                 # headers=headers,
                                 callback=self.parse,
                                 meta={'state':state})




    def parse(self, response):
        item=Store_subcat_links_Item()
        state=response.meta['state']
        subcat_links=response.xpath('//div[@class="sb-directory"]/ul/li')
        for each_subcat_links in subcat_links:
            subcat_link=each_subcat_links.xpath('./a/@href').extract_first()
            subcat_name=each_subcat_links.xpath('./a/text()').extract_first()
            item['state']=state
            item['subcat_name']=subcat_name
            item['subcat_link']=f'https://www.menswearhouse.com{subcat_link}'
            yield item

        try:

            update_query = "UPDATE store_cat_links SET status = 'Done' WHERE state = %s"
            self.cursor.execute(update_query,state)
            self.conn.commit()

        except Exception as e:
            print(f"Error updating store_cat_links: {e}")






if __name__ == '__main__':
    execute('scrapy crawl subcategory_links'.split())

