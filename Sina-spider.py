import csv
import hashlib

import pymysql
import re
from redis import Redis
import requests
import time
from tqdm import tqdm

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def sum_total(result_dict):
    for key, value in result_dict.items():
        count_num = len(value)
        print(f"{key} 一共有{count_num}个")


class Sina_weibo:
    # https://weibo.com/p/100101B2094451DB64A7FB499E?page=
    def __init__(self):
        self.n = 0
        self.session = requests.Session()  # 创建 Session 对象
        self.setup_retries()  # 配置重试策略
        self.processed_urls = set()  # 添加此行以初始化 processed_urls
        self.failed_urls = set()

        self.base_url = 'https://weibo.com/p/aj/v6/mblog/mbloglist'
        self.headers = {
            'Cookie':'XSRF-TOKEN=nyRoewRJ7cUUPGeJJlxBZcdU; SCF=Amr75f_MAiIe5w668d4jdTkQdZ8V9XGguTDhflVpodpl8ZXxOKItCgrhzi0wY2TqEItjMXv6XVMfSZdCQlBm-E0.; _s_tentry=-; Apache=6360121289264.941.1732046371201; SINAGLOBAL=6360121289264.941.1732046371201; ULV=1732046371217:1:1:1:6360121289264.941.1732046371201:; PC_TOKEN=68521d43e3; ALF=1736590536; SUB=_2A25KXseYDeRhGeFH71QR9irEzz-IHXVpEkVQrDV8PUJbkNANLRmhkW1Nez7RU2c_eW9dlAC-MFuz8VnIohZy0BC_; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W52FphS4L7Jgn3DDnZEmLUf5JpX5KMhUgL.FoM4Shq7SoBRShe2dJLoIp7LxKML1KBLBKnLxKqL1hnLBoMN1KBcehqX1hB0; wb_view_log_7946064843=1152*7202; _dd_s=rum=0&expire=1733999402487',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0',
            'Referer': 'https://weibo.com'
        }
        self.params = {
            'ajwvr': '6',
            'domain': '100101',
            'page': '1',
            'pagebar': {},  # 需要更改 空 0-78
            'tab': 'home',
            'current_page': '1',
            'pl_name': 'Pl_Core_MixedFeed__4',
            'id': {},  # 6个机场，不同id
            # 'script_uri': '/p/100101B2094451DB64A7FB499E',
            'feed_type': '1',
            'pre_page': '1',  # 根据需要更改,等于page 1-27
            'domain_op': '100101',
            '__rnd': '1726335180',  # 随机数，bu需要更改
        }

        self.keywords = "广州白云国际 上海浦东国际 上海虹桥国际 北京首都国际 北京大兴国际 深圳宝安"
        self.repeat_times = 0

        # 初始化数据库连接和游标
        self.db = pymysql.connect(host='localhost', port=3306, user='root', password='Kid@1412', db='spiders16')
        self.cursor = self.db.cursor()
        self.red = Redis()

    #

    def create_table(self):
        # 使用预处理语句创建表
        # airport_name, usernames, mid_list, dates, corrected_content_list, location_list
        sql = '''
        CREATE TABLE IF NOT EXISTS 微博评论0916(
            id INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
            airport_name VARCHAR(255),
            user_name VARCHAR(255),
            mid VARCHAR(18),
            date_published VARCHAR(50),
            review_body TEXT,
            location VARCHAR(255),
            post_url VARCHAR(255)
        )
        '''
        try:
            self.cursor.execute(sql)
            self.db.commit()
            print("CREATE TABLE SUCCESS.")
        except Exception as ex:
            print(f"CREATE TABLE FAILED, CASE: {ex}")
            self.db.rollback()

    def url_lists(self):
        airport_id_list = {
            'beijing-capital-airport': '100101B2094451DB64A7FB499E',
            'beijing-daxing-airport': '100101B2094557DA69A5FC459F',
            'guangzhou-baiyun-airport': '100101B2094757D06EABF4479F',
            'shanghai-pudong-airport': '100101B2094757D068A1FD4098',
            'shanghai-hongqiao-airport': '100101B2094654DB65A6F84598',
            "shenzhen-bao'an-airport": '100101B2094757D66EA1FD4099'
        }

        url_list_with_airport = []  # 用来存储生成的 URL 和 airport_name
        for airport_name, airport_id in airport_id_list.items():
            # 生成 pagebar 为空的 URL
            params = self.params.copy()
            params['id'] = airport_id
            params['pagebar'] = ''  # pagebar 为空
            full_url_no_pagebar = self.base_url + '?' + '&'.join([f'{key}={value}' for key, value in params.items()])
            url_list_with_airport.append((full_url_no_pagebar, airport_name))  # 保存 URL 和 airport_name 的元组

            # 生成 pagebar 从 0 到 78 的 URL,range(0,79)
            for pagebar in range(0, 10):
                params['pagebar'] = pagebar
                full_url_with_pagebar = self.base_url + '?' + '&'.join(
                    [f'{key}={value}' for key, value in params.items()])
                url_list_with_airport.append((full_url_with_pagebar, airport_name))  # 保存 URL 和 airport_name 的元组

        return url_list_with_airport

    def get_data(self, url):
        all_texts = ""
        if url in self.processed_urls:
            # print(f"URL 已处理，跳过：{url}")
            print(f"重复网址，已处理~~~~~~")
            return "重复数据"
        else:
            self.n += 1  # 每次评论数+1
            print(f"\n~~~第{self.n}组动态~~~")
            print("\n正在爬取网页：", url)
            time.sleep(2)

        self.processed_urls.add(url)
        response = self.get_data_with_retries(url=url, headers=self.headers)
        time.sleep(2)
        if response:
            response.encoding = 'utf-8'
            # print(response.text)
            data = response.json()
            # print(data)
            html_content = str(data)
            all_texts += html_content + "\n"  # 收集所有script的内容

        return all_texts

    def setup_retries(self):
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def get_data_with_retries(self, url, headers):
        try:
            response = self.session.get(url=url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")  # 请求发生错误
            return None

    def parse_data(self, text):
        soup = BeautifulSoup(text, 'lxml')

        username_list = []
        timestamp_list = []
        content_list = []
        location_list = []
        mid_list = []
        post_urls_list = []
        data_to_insert = []

        ### 需要提取的信息为： 用户名 user_name   动态号 feed_id     时间日期 timestamp  (datetime)  动态文本内容 feed_text   网址 url     定位信息 location

        ##获取用户名 user_name
        def find_user_name(x):
            # 找到所有含有 nick-name 属性的标签
            usernames = soup.find_all(class_='W_f14 W_fb S_txt1', attrs={"nick-name": True})
            usernames = [tag.get('nick-name') for tag in usernames]
            for username in usernames:
                username_list.append(username)
            # print("所有提取的用户名:", username_list)
            # print("总计提取到的用户名数量:",len(username_list))
            # print("*" * 30)

        ##获取动态号 feed_id(mid)
        def find_feed_id(x):
            # class="WB_cardwrap WB_feed_type S_bg2 WB_feed_like" mid
            feed_ids = soup.find_all(class_='WB_cardwrap WB_feed_type S_bg2 WB_feed_like', attrs={"mid": True})
            feed_ids = [tag.get('mid') for tag in feed_ids]
            for feed_id in feed_ids:
                mid_list.append(feed_id)
            # print("所有提取的动态号:", mid_list)
            # print("总计提取到的动态号数量:",len(mid_list))
            # print("*" * 30)

        def find_timestamp(x):
            date_pattern = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}')
            # 找到包含日期时间的 a 标签
            timestamps = soup.find_all('a', class_='S_txt2', title=True)
            for tag in timestamps:
                if date_pattern.match(tag['title']):
                    timestamp_list.append(tag['title'])
            # print("所有提取的日期时间:", timestamp_list)
            # print("总计提取到的日期时间数量:",len(timestamp_list))
            # print("*" * 30)

        def clear(x):
            clear_text = re.sub(r'', '', x)
            clear_text = re.sub(r'\\u200b', '', clear_text)  # 去掉零宽空格字符
            clear_text = re.sub(r'2\s+[\u4e00-\u9fff]+·[\u4e00-\u9fff]+', ' ', clear_text)  # 删除地点相关内容
            clear_text = re.sub(r'2[\u4e00-\u9fff]+·[\u4e00-\u9fff]+', '', clear_text)  # 删除地点相关内容
            clear_text = re.sub('\w\s+\w*的微博视频', '', clear_text)  # 删除类似“的微博视频”的内容
            clear_text = clear_text.strip()
            return clear_text

        def find_feed_text(x):
            # 提取包含指定 class 和 attrs 的 div 元素
            text_list = soup.find_all('div', class_='WB_text W_f14', attrs={'node-type': 'feed_list_content'})
            # print(text_list)        ##一条plog告别八月#<
            # 提取并清理文本内容
            sum_longpost = 0
            i = 0
            for text in text_list:
                # print(text)
                content = text.get_text(separator=' ', strip=True)
                # print(content)
                i += 1
                # 处理长微博'展开全文'
                if '展开全文' in content:
                    url = f'https://weibo.com/p/aj/mblog/getlongtext?mid={mid_list[i - 1]}'
                    long_content = fetch_long_weibo_content(mid_list[i - 1])
                    if long_content is None:
                        print(f"Failed to fetch long weibo content for post ID {mid_list[i - 1]}")
                        print(f"出错网址：{url}")
                        content = "无法获取长微博内容"
                        self.failed_urls.add(url)
                    else:
                        sum_longpost += 1
                        content = long_content  # 用长微博内容替换原有短内容
                # 清理文本内容
                clear_text = clear(content)
                # print(f"第{i}条",clear_text)
                content_list.append(clear_text)
            # print("所有提取的评论列表：", content_list)
            # print("长微博总数：", sum_longpost)
            # print("总计提取到的微博数量:",len(content_list))

        def fetch_long_weibo_content(x):
            url = f'https://weibo.com/p/aj/mblog/getlongtext?mid={x}'
            try:
                # 发起请求，并确保状态码为 200
                response = self.get_data_with_retries(url=url, headers=self.headers)
                if response.status_code == 200:
                    # print(url)
                    # 尝试解析 JSON 数据
                    try:
                        json_data = response.json()
                    except ValueError as e:
                        print(f"Error parsing JSON: {e}")
                        return None

                    # 检查 'data' 是否存在且是字典
                    if 'data' in json_data and isinstance(json_data['data'], dict):
                        long_post = json_data['data'].get('html', 'No content found')
                        long_post = clear(long_post)
                        long_post = BeautifulSoup(long_post, 'lxml')
                        long_post = long_post.get_text(strip=True)
                        long_post = clear(long_post)

                        return long_post
                    else:
                        print("No 'data' field or 'data' is not a dictionary.")
                        return None
                else:
                    print(f"Failed to fetch data, status code: {response.status_code}")
                    return None
            except requests.RequestException as e:
                print(f"Request failed: {e}")
                return None

        def find_feed_url(x):
            href_list = soup.find_all('a', class_='S_txt2', href=True, target='_blank')
            url_pattern = re.compile(r'^https://weibo\.com/\d+/[\w-]+$')
            weibo_urls = [a['href'] for a in href_list if url_pattern.match(a['href'])]
            # 打印结果
            for url in weibo_urls:
                # print(url)
                post_urls_list.append(url)
            # print("所有提取的评论urls:", post_urls_list)
            # print("*" * 30)
            # print("总计提取到的url数量:",len(post_urls_list))

        def find_location(x):
            div_list = soup.find_all('div', class_='WB_text W_f14', attrs={'node-type': 'feed_list_content'})
            # 定义匹配地点格式的正则表达式模式
            location_pattern = re.compile('[\u4e00-\u9fff]+·[\u4e00-\u9fff]+')
            for div in div_list:
                location_match = location_pattern.search(str(div))  # 查找匹配
                # print(location_match)
                # print('~~')
                if location_match:
                    location = location_match.group()
                    location = self.find_matching_location(location)
                    # print(location)
                    location_list.append(location)
                else:
                    location_list.append("空")

            # print("所有提取的位置信息:", location_list)
            # print("总计提取到的地址数量:", len(location_list))
            # print("*" * 30)

        find_user_name(text)
        find_feed_id(text)
        find_timestamp(text)
        find_feed_text(text)
        find_feed_url(text)
        find_location(text)

        for username, mid, timestamp, content, location, post_url in zip(username_list, mid_list, timestamp_list,
                                                                         content_list, location_list, post_urls_list):
            # 每次循环创建一个新的字典，并添加到列表中
            data_to_insert.append({
                'username': username,
                'mid': mid,
                'timestamp': timestamp,
                'content': content,
                'location': location,
                'post_urls': post_url
            })
        print('\n本次提取到的数据量:',len(data_to_insert))
        return data_to_insert, username_list, mid_list, timestamp_list, content_list, post_urls_list, location_list

    def find_matching_location(self, location):
        # 检查 location 是否包含 keywords 中的任意一个关键字
        for keyword in self.keywords.split():
            if keyword in location:
                return location
        return '空'

    def get_md5(self, res):
        md5 = hashlib.md5()
        md5.update(str(res).encode())
        return md5.hexdigest()

    def save_data(self,airport_name, data_to_insert):
        # 确保数据库连接未关闭
        if self.db.open != 1:
            self.db.ping(reconnect=True)

        if self.cursor is None:
            self.cursor = self.db.cursor()

        try:
            valid_data = []
            for data in data_to_insert:
                # print(data)
                # 生成唯一的 MD5 哈希值
                unique_data = f"{data['username']}_{data['mid']}_{data['content']}_{data['location']}_{data['post_urls']}"
                md5_value = self.get_md5(unique_data)
                # print(unique_data)
                # 检查 Redis 中是否存在相同的哈希值，去重
                res = self.red.sadd('sina-0916', md5_value)

                if res > 0:  # 如果哈希值不存在，插入数据
                    valid_data.append((
                        airport_name,
                        data['username'],
                        data['mid'],
                        data['timestamp'],
                        data['content'],
                        data['location'],
                        data['post_urls']
                    ))
                else:
                    self.repeat_times += 1
                    print('^'*30)
                    print(f"数据重复，跳过插入: {md5_value}")
                    print(data)
                    print('^'*30)

            # 如果有有效数据待插入，进行批量插入操作
            if valid_data:
                print(valid_data)
                sql = """
                    INSERT INTO 微博评论0916 (airport_name, user_name, mid, date_published, review_body, location, post_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                self.cursor.executemany(sql, valid_data)

                # 提交事务
                self.db.commit()
                print(f"\nSuccessfully saved {len(valid_data)} reviews.")
            else:
                print("No new data to insert.")

        except Exception as e:
            print(f"Insertion failed, reason: {e}")
            self.db.rollback()

    def close_connection(self):
        # 关闭数据库连接
        if self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()

    def get_all_data(self):
        # 确保数据库连接未关闭
        if self.db.open != 1:
            self.db.ping(reconnect=True)

        if self.cursor is None or self.cursor.close:
            self.cursor = self.db.cursor()

        try:
            # 执行查询以获取所有数据
            sql = "SELECT airport_name, user_name, mid, date_published, review_body, location, post_url FROM 微博评论0916"
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()

            # 将查询结果转换为适合写入 CSV 的格式
            all_data = [row for row in rows]
            return all_data
        except Exception as e:
            print(f"Failed to fetch data: {e}")
            return []

    def sum_time(self, t1):
        total_time = time.time() - t1
        minutes = int(total_time // 60)  # 计算分钟数
        seconds = int(total_time % 60)  # 计算剩余秒数
        print(f"微博爬虫累计用时：{minutes}分{seconds}秒")

    def main(self, t1):
        self.create_table()
        airports_list = self.url_lists()
        total_airports = len(airports_list)
        # print(total_airports)

        # 初始化最终结果字典
        final_result_dict = {'username': [],'mid': [],'timestamp': [],'content': [],'post_urls': [],'location': []}

        # 初始化进度条
        with tqdm(total=total_airports, desc="Processing Airports", ncols=100) as airport_pbar:
            for url,airport_name,  in airports_list:  # 每次处理一个机场
                print('\n','*'*30)
                print(f"\n开始处理: {airport_name}")

                try:
                    # 获取数据
                    texts = self.get_data(url)
                    if not texts:
                        print(f"未从 URL 获取到数据：{url}")
                        continue

                    # 解析数据
                    data_to_insert, username_list, mid_list, timestamp_list, content_list, post_urls_list, location_list = self.parse_data(
                        texts)
                    # print(data_to_insert)

                    # 更新最终结果字典
                    final_result_dict['username'].extend(username_list)
                    final_result_dict['mid'].extend(mid_list)
                    final_result_dict['timestamp'].extend(timestamp_list)
                    final_result_dict['content'].extend(content_list)
                    final_result_dict['post_urls'].extend(post_urls_list)
                    final_result_dict['location'].extend(location_list)

                    self.save_data(airport_name,data_to_insert)

                except Exception as e:
                    print(f"Error processing {url} for {airport_name}: {e}")
                finally:
                    airport_pbar.update(1)

            # 打开 CSV 文件以写入数据
            with open('weibo_spider0916.csv', mode='w+', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                # 写入 CSV 文件的表头
                writer.writerow(
                    ['airport_name', 'user_name', 'mid', 'date_published', 'review_body', 'location', 'post_url'])
                all_data = self.get_all_data()  # 这个方法需要你在类中实现
                for data in all_data:
                    writer.writerow(data)  # 每行数据写入 CSV 文件
            print("数据已保存到 weibo_spider0916.csv")




            # 关闭数据库连接
            self.close_connection()

            print("------All of data-----")
            sum_total(final_result_dict)
            self.sum_time(t1)
            print("---------End---------")




if __name__ == '__main__':
    t1 = time.time()
    sina = Sina_weibo()
    sina.main(t1)

    # # 打开 CSV 文件以写入数据
    # with open('weibo_spider01214.csv', mode='a', encoding='utf-8', newline='') as f:
    #     writer = csv.writer(f)
    #
    #     # 写入 CSV 文件的表头
    #     writer.writerow(['airport_name', 'user_name', 'mid', 'date_published', 'review_body', 'location', 'post_url'])
    #     all_data = sina.get_all_data()  # 这个方法需要你在类中实现
    #     for data in all_data:
    #         writer.writerow(data)  # 每行数据写入 CSV 文件
    # print("数据已保存到 weibo_spider01214.csv")