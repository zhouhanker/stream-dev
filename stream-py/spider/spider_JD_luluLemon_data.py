from lxml import html
from lxml import etree
from utils import ChromeDebugControllerUtils
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch


luluLemon_base_url = 'https://mall.jd.com/view_search-2286219.html'
local_debug_dir = '/Users/zhouhan/dev_env/chrome_debug_dir'
local_chrome_debug_port = 9222

chrome_debug_controller = ChromeDebugControllerUtils.ChromeDebugController(
    port=local_chrome_debug_port,
    user_data_dir=local_debug_dir)


def start_chrome_driver():
    chrome_debug_controller.start()
    return chrome_debug_controller.connect_drissionpage()


def write_to_postgres(insert_list):
    conn_params = {
        "host": "10.160.60.14",
        "port": "5432",
        "database": "spider_db",
        "user": "postgres",
        "password": "Zh1028,./"
    }
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    try:
        # noinspection SqlResolve
        insert_query = sql.SQL("""
            INSERT INTO spider_db.public.spider_lululemon_jd_product_info
            (product_link, image, sale_amount, product_desc, product_comment_link, insert_user, page_num)
            VALUES (%(product_link)s, %(image)s, %(sale_amount)s, %(product_desc)s, %(product_comment_link)s, %(insert_user)s, %(page_num)s)
        """)

        execute_batch(cursor, insert_query, insert_list)
        conn.commit()
        print(f"成功插入 {len(insert_list)} 条记录到数据库")

    except (Exception, psycopg2.Error) as error:
        print(f"数据库操作错误: {error}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def spider_product_list_info(max_pages=57):
    product_list = []
    drissionpage = start_chrome_driver()
    print(drissionpage.browser_version)

    try:
        # 访问第一页
        drissionpage.get(luluLemon_base_url, retry=3, interval=3)

        for page_num in range(1, max_pages + 1):
            print(f"正在爬取第 {page_num} 页...")

            # 页面加载和处理
            drissionpage.wait(2, 5)
            drissionpage.run_js("window.scrollBy(0,1400)")
            drissionpage.wait(2, 4)
            drissionpage.scroll.to_top()
            drissionpage.wait(1, 3)

            # 解析页面内容
            tree = etree.HTML(drissionpage.html)
            items = tree.xpath('//li[@class="jSubObject gl-item"]')

            # 如果没有找到商品，可能是最后一页，退出循环
            if not items:
                print(f"第 {page_num} 页未找到商品，可能已到最后一页")
                break

            # 处理当前页的商品
            page_products = []
            for item in items:
                link = item.xpath('.//div[@class="jPic"]/a/@href')
                link = link[0] if link else None
                image = item.xpath('.//div[@class="jPic"]/a/img/@src')
                image = image[0] if image else None

                # 获取商品价格
                price = item.xpath('.//div[@class="jPrice"]//span[@class="jdNum"]/text()')
                price = price[0] if price else None

                # 获取商品描述
                desc = item.xpath('.//div[@class="jDesc"]/a/@title')
                desc = desc[0] if desc else None

                # 获取商品评价链接
                comment_link = item.xpath('.//div[@class="jExtra"]/a/@href')
                comment_link = comment_link[0] if comment_link else None

                product_info = {
                    'product_link': "https:" + link if link else None,
                    'image': "https:" + image if image else None,
                    'sale_amount': float(price) if price else None,
                    'product_desc': desc,
                    'product_comment_link': "https:" + comment_link if comment_link else None,
                    'insert_user': 'Eric',
                    'page_num': str(page_num)
                }
                page_products.append(product_info)

            # 将当前页数据添加到总列表
            product_list.extend(page_products)

            # 将当前页数据写入数据库
            write_to_postgres(page_products)
            print(f"第 {page_num} 页数据已成功写入数据库")

            # 如果不是最后一页，点击下一页按钮
            if page_num < max_pages:
                try:
                    next_page_btn = drissionpage.ele('x://*[@id="J_topPage"]/a[2]')
                    if next_page_btn:
                        next_page_btn.click()
                        print(f"已成功翻到第 {page_num + 1} 页")
                    else:
                        print("未找到下一页按钮，可能已到最后一页")
                        break
                except Exception as e:
                    print(f"翻页失败: {e}")
                    break

    finally:
        pass
        if drissionpage:
            chrome_debug_controller.close()

    return product_list, drissionpage


def spider_product_detail_info():
    product_list_info, drissionpage = spider_product_list_info()
    for product in product_list_info:
        product__detail_url = product.get("product_link")
        if not product__detail_url:
            product["detail_error"] = "Missing product_link"
            continue

        try:
            drissionpage.get(product__detail_url)
            drissionpage.wait(2, 4)
            drissionpage.scroll(1500)
            drissionpage.wait(1, 2)

            tree = html.fromstring(drissionpage.html)

            color_list = tree.xpath('//div[@id="choose-attr-1"]/div[@class="dd"]/div[contains(@class, "item")]/@data-value')
            size_list = tree.xpath('//div[@id="choose-attr-2"]/div[@class="dd"]/div[contains(@class, "item")]/@data-value')

            comment_count_elem = tree.xpath('//div[@id="comment-count"]/a[@class="count J-comm-10147050006359"]')
            comment_count = comment_count_elem[0].text.strip() if comment_count_elem else "数据缺失"

            product_id_elem = tree.xpath('//div[@class="goods-base"]//div[contains(., "商品编号")]/following-sibling::div[1]/div')
            product_id = product_id_elem[0].text.strip() if product_id_elem else "数据缺失"

            product_code_elem = tree.xpath('//div[@class="goods-base"]//div[contains(., "货号")]/following-sibling::div[1]/div')
            product_code = product_code_elem[0].text.strip() if product_code_elem else "数据缺失"

            comments = [c.strip() for c in tree.xpath('//div[@class="info text-ellipsis-2"]/text()') if c.strip()]
            recommendations = [r.strip() for r in tree.xpath('//div[@id="tying-sale-recommend"]//div[@class="name"]/@title') if r.strip()]

            product.update({
                "color_list": color_list,
                "size_list": size_list if size_list else ["数据缺失"],
                "comment_count": comment_count,
                "product_id": product_id,
                "product_code": product_code,
                "comments": comments,
                "recommendations": recommendations
            })

        except Exception as e:
            product["detail_error"] = str(e)

    return product_list_info


def main():
    product_list_info, dis = spider_product_list_info()
    for i in product_list_info:
        print(i)


if __name__ == '__main__':
    main()
