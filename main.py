import csv
import time as tm
from datetime import datetime as dt
from datetime import timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup

headerss = []
likes = []  # No of likes on the review
p_names = []  # product name
c_names = []  # customer name
ratings = []  # customer rating
dates = []  # product ratings
reviews = []  # total reviews
uids = []  # ASIN numbers of the prodcuts
asin = []
url = []
# Reading ASIN Unique codes from CSV file
with open('./headers.csv') as csv_file:
    reader2 = csv.reader(csv_file, delimiter='\n')
    for col in reader2:
        header = col[0]
        headerss.append(header)

with open('./asin.csv') as csv_file:
    reader = csv.reader(csv_file, delimiter=',')
    for col in reader:
        asin_t = col[1]
        asin.append(asin_t)
    time_prd = asin[len(asin) - 1]

curr_dat = dt.today()  # Current system date
day_diff = timedelta(int(time_prd))
print("Fetch questions from after :", (curr_dat - day_diff).strftime("%d %b %Y"))

asin.pop(0)
asin.pop(1)
print(asin)
print(asin[1])

print('Get ratings from : ', str(len(asin)) + ' products')


def get_data(uid):
    no_pages = 1
    flag = False
    while flag is False:

        hedr = random.choice(headerss)
        print(hedr)
        headers = {"User-Agent": hedr,
                   "Accept-Encoding": "gzip, deflate",
                   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT": "1",
                   "Connection": "close", "Upgrade-Insecure-Requests": "1"}
        tm.sleep(2)
        session = requests.Session()
        session.verify = False
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        r = session.get('https://www.amazon.in/product-reviews/' + str(uid) + '?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=' + str(no_pages),
                        headers=headers)  # , proxies=proxies) : ASIN example - B01L7C4IU2
        content = r.content
        soup = BeautifulSoup(content)

        try:
            name_p = soup.find('a',
                               attrs={'class': 'a-link-normal',
                                      'data-hook': 'product-link'}, href=True)
            if name_p is not None:
                name = name_p.text.strip()  # fetch name of product
                p_names.append(name)
                uids.append(uid)
                ratings.append(' ')
                reviews.append(' ')
                dates.append(' ')
                c_names.append(' ')
                url.append(' ')
                likes.append(' ')

                tm.sleep(2)
                container = soup.find('div', attrs={'id': 'cm_cr-review_list',
                                                    'class': 'a-section a-spacing-none review-views celwidget'})    # Whole review container
                if container is not None:
                    for r in container.findAll('div',
                                               attrs={'data-hook': 'review', 'class': 'a-section review aok-relative'}):
                        dte_t = r.find('span', attrs={'class': 'a-size-base a-color-secondary review-date',
                                                      'data-hook': 'review-date'}).text.strip()  # extract text and strip of whitespaces
                        # ex : Reviewed in India on 28 December 2020
                        temp = dte_t.split(' ')
                        dte = str(temp[4]) + ' ' + str(temp[5]) + ' ' + str(temp[6])
                        # print(formatted)
                        q_date = dt.strptime(dte, '%d %B %Y')  # Convert string to Date
                        if (curr_dat - day_diff) <= q_date:
                            cus = r.find('span', attrs={'class': 'a-profile-name'}).text.strip()
                            text_div = r.find('div', attrs={'class': 'a-row a-spacing-small review-data'})
                            review = text_div.find('span').text.strip()
                            like = r.find('span', attrs={'class': 'a-size-base a-color-tertiary cr-vote-text',
                                                          'data-hook': 'helpful-vote-statement'})
                            if like is not None:
                                likes.append(like.text.strip())
                            else:
                                likes.append(0)

                            que_lnk = r.find('a', attrs={
                                'class': 'a-size-base a-link-normal review-title a-color-base review-title-content a-text-bold',
                                'data-hook': 'review-title'}, href=True)
                            link = que_lnk['href']
                            rating = r.find('span', attrs={'class': 'a-icon-alt'}).text.strip()
                            clean = rating.split(' ')

                            print('================== Getting data for ' + cus + ' ==================')
                            ratings.append(clean[0])
                            reviews.append(review)
                            dates.append(q_date)
                            c_names.append(cus)
                            url.append('https://www.amazon.in' + link)
                            uids.append(' ')
                            p_names.append(' ')

                            print('Rating : ' + rating)

                        elif q_date < (curr_dat - day_diff):
                            print('Date Expired')
                            return uids, p_names, likes,  c_names, dates, ratings, reviews, url
                else:
                    print(container)
                    print(name)
                    print("================ NO REVIEWS ================")
                    return uids, p_names, 0, 'NA', 0, 0, 'NA', url
            else:
                print("************** PAGE DOES NOT EXIST **************")
                print('https://www.amazon.in/product-reviews/' + str(uid) + '?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=' + str(no_pages))
                print(name_p)
                return uids, str('ERROR'), 0, str('404'), 0, 0, str('NA'), url

            # --------------------------------------- PAGE CONCEPT HERE ---------------------------------------
            tm.sleep(2)
            if soup.find('li', attrs={'class': 'a-disabled a-last'}):
                flag = True
                print('//////END OF PAGES///////')
            tm.sleep(2)
            if soup.find('li', attrs={'class': 'a-last'}):  # Check if there are more pages?

                no_pages += 1
            else:
                print('SINGLE PAGE ONLY')
                return uids, p_names, likes, c_names, dates, ratings, reviews, url
            return uids, p_names, likes, c_names, dates, ratings, reviews, url

        except requests.exceptions.ConnectionError:
            print('Connection refused')


for t in range(1, 50):  # len(asin) - 1
    print('|||||||| PRODUCT NO : ' + str(t - 1) + ' ||||||||')
    unique, product_name, likess, cus_name, datess, ratingss, reviewss, urls = get_data(asin[t])
    dict = {'ASIN': unique, 'Product Name': product_name, 'Likes': likess, 'Customer Name': cus_name,
            'Question Date': datess, 'Rating': ratingss, 'Review': reviewss, 'URL': urls}

df = pd.DataFrame(dict)

df.to_csv('./reviews.csv', index=False, encoding='utf-8')
