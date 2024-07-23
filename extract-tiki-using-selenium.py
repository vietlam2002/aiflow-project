from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from webdriver_manager.chrome import ChromeDriverManager

import time
import pickle
from urllib import request

QUERY_PATH = './data-query'
IMGS_PATH = './tiki-imgs'

service = Service(executable_path=ChromeDriverManager().install())
options = Options()

driver = webdriver.Chrome(service=service, options=options)

my_urls = ["https://tiki.vn/dien-tu-dien-lanh/c4221?page=" + str(i) for i in range(1,21)]


_20_pages_info = dict()
list_item = []
# len(my_urls)+1
for i in range(1, 3):
    driver.get(my_urls[i-1])
    time.sleep(2)


    link_info = driver.find_elements(By.XPATH, "//div[@class='CatalogProducts__Wrapper-sc-1r8ct7c-0 jOZPiC']//a[@class='style__ProductLink-sc-139nb47-2 cKoUly product-item']")
    img_info = driver.find_elements(By.XPATH, "//div[@class='CatalogProducts__Wrapper-sc-1r8ct7c-0 jOZPiC']//picture[@class='webpimg-container']/img[@class='styles__StyledImg-sc-p9s3t3-0 hbqSye']")
    title_info = driver.find_elements(By.XPATH, "//div[@class='CatalogProducts__Wrapper-sc-1r8ct7c-0 jOZPiC']//a[@class='style__ProductLink-sc-139nb47-2 cKoUly product-item']//h3")
    discounted_info = driver.find_elements(By.XPATH, "//div[@class='CatalogProducts__Wrapper-sc-1r8ct7c-0 jOZPiC']//a[@class='style__ProductLink-sc-139nb47-2 cKoUly product-item']//div[@class='price-discount__price']")

    # item = dict()
    # for j in range(len(link_info)):
    #     item['href'] = link_info[j].get_attribute('href')
    #     #images
    #     srcset = img_info[j].get_attribute('srcset')
    #     item['img'] = srcset.split(' ')[0].split()[0]
    #     #title
    #     item['title'] = title_info[j].text
    #     #price
    #     item['discount_price'] = discounted_info[j].text
    #
    # list_item[f'page_{i}'] = item
    for j in range(len(img_info)):
        srcset = img_info[j].get_attribute('srcset')
        image_url = srcset.split(' ')[0].split()[0]
        list_item.append(image_url)
# _20_pages_info = list_item

# with open(QUERY_PATH + '/' + 'quey_tiki', 'wb') as f:
#     pickle.dump(_20_pages_info, f)
#     f.close()
#
# f =  open(QUERY_PATH + '/' + 'quey_tiki', 'rb')
# data = pickle.load(f)
# f.close()

# imgs_list = [data[f'page_{i}']['img'] for i in range (1,10)]
imgs_list = list_item
for i in range(len(imgs_list)):
    request.urlretrieve(imgs_list[i], IMGS_PATH + '/' + imgs_list[i].split('/')[-1])

driver.close()

    # for j in range(len(img_info)):
    #     srcset = img_info[j].get_attribute('srcset')
    #     image_url = srcset.split(' ')[0].split()[0]
    #     print(image_url)






# //div[@class='styles__ProductItemContainerStyled-sc-bszvl7-0 elOGIo']//picture[@class='webpimg-container']/img[@class='styles__StyledImg-sc-p9s3t3-0 hbqSye']