import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import json
import csv
import time
import threading
import logging
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

class crawller:

    def __init__(self, key="", max_thread=10):
        self.key = key
        self.max_thread = max_thread

    def __get_new_proxy(self):
        rq = requests.get(
            f"https://wwproxy.com/api/client/proxy/available?key={self.key}&provinceId=-1"
        )
        response = json.loads(rq.text)
        if response["status"] == "BAD_REQUEST":
            return ""
        return response["data"]["proxy"]
    def __get_proxy(self):
        rq = requests.get(f"https://wwproxy.com/api/client/proxy/current?key={self.key}")
        response = json.loads(rq.text)
        if response["status"] == "BAD_REQUEST":
            return ""
        return response["data"]["proxy"]

    def __get_links_in_page(self, page, proxies, outputFile):
        response = requests.get(
            "https://bonbanh.com/oto/page," + str(page),
            proxies={"http": proxies, "https": proxies},
        )

        soup = BeautifulSoup(response.content, "html.parser")
        contents = soup.find_all("li", class_=["car-item row1", "car-item row2"])

        links = [link.find("a").attrs["href"] for link in contents]
        # open file in write mode
        with open(outputFile, "a") as fp:
            for link in links:
                # write each item on a new line
                fp.write("%s\n" % link)

        print(f"page: {page}, records: {len(links)}" , "\n")

    def crawl_carID(self, outputFile, firstPage=1, lastPage=2):
        proxies = self.__get_new_proxy()
        threads = []
        for page in range(firstPage, lastPage + 1):
            while threading.active_count() > self.max_thread:
                time.sleep(1)
            thread = threading.Thread(
                target=self.__get_links_in_page, args=(page, proxies, outputFile)
            )
            thread.start()
            threads.append(thread)
            if page % 80 == 0:
                while True:
                    proxies = self.__get_new_proxy()
                    if proxies == "":
                        print("Dang doi proxy")
                        for s in range(15):
                            print("Lay proxy sau: ", 15 - s, "s")
                            time.sleep(1)
                    else:
                        break
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        print("Hoàn thành")

    def __get_details(self, id, proxy, outputFile):
        logging.basicConfig(level=logging.INFO, filename="error.log", filemode="w")
        try:
            response = requests.get(
                "https://bonbanh.com/" + id, proxies={"http": proxy, "https": proxy}
            )
            soup = BeautifulSoup(response.content, "html.parser")
            content_details = soup.select("#mail_parent span.inp")

            date_release = soup.find("div", class_="notes").text.strip()
            date_release = date_release.replace("\n", "").replace("\t", "")
            dot_index = date_release.index(".")
            space_index = date_release.rfind(" ", 0, dot_index)
            date_release = date_release[space_index + 1 : dot_index].strip()

            span_tags = soup.find_all("span", itemprop="name")
            car_name = span_tags[3].text.strip()

            # loc quynh
            address_element = soup.select_one("div.contact-txt br:nth-of-type(2)")
            if address_element:
                address = address_element.next_sibling.strip()
                address = address.split()
                if "Vũng" in address:
                    result = address[-4:]
                elif "Huế" in address:
                    result = address[-3:]
                else:
                    result = address[-2:]
                address = " ".join(result)
            else:
                address = ""

            #
            record = {}
            try:
                record["ID"] = id
                record["Hãng xe"] = car_name
                if len(content_details) == 12:
                    record["Năm sản xuất"] = content_details[0].text.strip()
                    record["Tình trạng"] = content_details[1].text.strip()
                    record["Số Km đã đi"] = content_details[2].text.strip()
                    record["Xuất xứ"] = content_details[3].text.strip()
                    record["Kiểu dáng"] = content_details[4].text.strip()
                    record["Hộp số"] = content_details[5].text.strip()
                    record["Động cơ"] = content_details[6].text.strip()
                    record["Số chỗ ngồi"] = content_details[9].text.strip()
                    record["Dẫn động"] = content_details[11].text.strip()
                else:
                    record["Năm sản xuất"] = content_details[0].text.strip()
                    record["Tình trạng"] = content_details[1].text.strip()
                    record["Số Km đã đi"] = 0
                    record["Xuất xứ"] = content_details[2].text.strip()
                    record["Kiểu dáng"] = content_details[3].text.strip()
                    record["Hộp số"] = content_details[4].text.strip()
                    record["Động cơ"] = content_details[5].text.strip()
                    record["Số chỗ ngồi"] = content_details[8].text.strip()
                    record["Dẫn động"] = content_details[10].text.strip()
                record["Ngày đăng"] = date_release
                record["Địa điểm"] = address
            except Exception as ex:
                logging.error(id + " " + type(ex) + "\n")

            title = soup.find("div", class_=["title"]).find("h1").text.split()
            price = 0
            try:
                if "Tỷ" in title and "Triệu" in title:
                    title = title[-4:]
                    price = int(title[-4]) * 1e9 + int(title[-2]) * 1e6
                elif "Tỷ" in title:
                    title = title[-2:]
                    price = int(title[-2]) * 1e9
                else:
                    title = title[-2:]
                    price = int(title[-2]) * 1e6
            except:
                logging.error("Gia: " + id + "\n")

            record["Giá"] = price

            # write record
            with open(outputFile, "a", encoding="utf-8", newline="") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=record.keys())
                writer.writerow(record)
        except:
            logging.error("ID: " + id + "\n")

    def crawl_carDetails(self, inputFile, outputFile):
        proxies = self.__get_proxy()
        
        cnt = 0
        threads = []
        with open(inputFile, "r") as f:
            lines = f.readlines()

            for line in lines:
                cnt += 1
                while threading.active_count() > self.max_thread:
                    time.sleep(1)
                print("crawling: ", cnt)
                thread = threading.Thread(
                    target=self.__get_details, args=(line.strip(), proxies, outputFile)
                )
                thread.start()
                threads.append(thread)
                if cnt % 90 == 0:
                    while True:
                        proxies = self.__get_new_proxy()
                        if proxies == "":
                            print("Dang doi proxy ")
                            for s in range(15):
                                print("Lay proxy sau: ", 15 - s, "s")
                                time.sleep(1)
                        else:
                            break
        for thread in threads:
            thread.join()
        print("Dữ liệu đã được lưu vào file CSV thành công!")

class utilities:
    def find_km_range(x, arr):
        for i in range(len(arr)):
            if x < arr[i]:
                return f"{int(arr[i-1])}-{int(arr[i])}"

    def get_km_range_order_array(arr):
        order = []
        for i in range(len(arr)-1):
            order.append(f"{int(arr[i-1])}-{int(arr[i])}")
        return order
    def bootstrapmean(data, n_iterations=1000,confidence_interval=95):
        data = np.array(data)

        sample_size = len(data)

        bootstrap_means = []
        for _ in range(n_iterations):
            bootstrap_sample = np.random.choice(data, size=sample_size, replace=True)
            bootstrap_mean = np.mean(bootstrap_sample)
            bootstrap_means.append(bootstrap_mean)

        bootstrap_means = np.array(bootstrap_means)
        lower_bound = np.percentile(bootstrap_means, (100-confidence_interval)/2)
        upper_bound = np.percentile(bootstrap_means, confidence_interval+(100-confidence_interval)/2)
        return bootstrap_means.mean(),np.std(np.array(bootstrap_means)),lower_bound,upper_bound # mean, std, lower, upper

    def loc_barlot(data_clean, col, count):
        counts = data_clean[col].value_counts()

        counts_sorted = counts.sort_values(ascending=False)

        data_top = counts_sorted.head(count).reset_index()

        plt.figure(figsize=(15, 10))
        sns.barplot(data=data_top, y=col, x='count')
        plt.title(f'Top {count} {col} có số lượng xe lớn nhất',size=25)
        plt.xlabel('Số lượng',size=20)
        plt.ylabel(col,size=20)
        plt.xticks(rotation=90)

        for index, value in enumerate(data_top['count']):
            plt.text(value, index, str(value))
        plt.tight_layout()

    def sokmdadi(data):
        bins = np.arange(0, 100001, 1000)
        bins = np.append(bins, np.inf)
        histvalues, _ = np.histogram(data['Số Km đã đi'], bins=bins)
        bin_centers = (bins[:-1] + bins[1:]) / 2

        plt.figure(figsize=(20, 8))
        sns.lineplot(x=bin_centers, y=histvalues)
        plt.xticks(ticks=np.arange(0, 100001, 10000), labels=np.arange(0, 100001, 10000))
        # plt.yticks(ticks=np.arange(0, 850, 100), labels=np.arange(0, 850, 100))
        plt.xlabel('Số Km đã đi')
        plt.ylabel('Số lượng')
        plt.xlim(0, 100000)
        plt.title('Sự phân bố của Số Km đã đi')
        plt.show()
