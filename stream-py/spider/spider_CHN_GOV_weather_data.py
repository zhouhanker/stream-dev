from DrissionPage import WebPage
from lxml import html

web_page = WebPage()
web_page.get("https://weather.cma.cn/web/weather/58377.html")
boot_root = html.fromstring(web_page.html)
print(web_page.html)

elements = boot_root.xpath("/html/body/div[1]/div[2]/div[1]")
print(boot_root)

