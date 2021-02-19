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
titles = []  # Title of the review
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
        if no_pages % 3 is 0:  # To sleep for avoiding timeouts
            print('SLEEPING FOR : ' + str((no_pages/3)*20) + ' SECONDS. ||| PAGE NUMBER - ' + str(no_pages) + ' |||')
            tm.sleep(20*(no_pages/3))
        print('PAGE NUMBER: ' + str(no_pages))
        hedr = random.choice(headerss)
        headers = {"User-Agent": hedr,
                   "Accept-Encoding": "gzip, deflate",
                   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT": "1",
                   "Connection": "close", "Upgrade-Insecure-Requests": "1"}
        tm.sleep(3)
        session = requests.Session()
        session.verify = False
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        r = session.get('https://www.amazon.in/product-reviews/' + str(
            uid) + '?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=' + str(no_pages),
                        headers=headers)  # , proxies=proxies) : ASIN example - B01L7C4IU2
        print('https://www.amazon.in/product-reviews/' + str(
            uid) + '?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=' + str(no_pages))
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
                titles.append(' ')
                ratings.append(' ')
                reviews.append(' ')
                dates.append(' ')
                c_names.append(' ')
                url.append(' ')
                likes.append(' ')

                tm.sleep(3)
                container = soup.find('div', attrs={'id': 'cm_cr-review_list',
                                                    'class': 'a-section a-spacing-none review-views celwidget'})  # Whole review container
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
                            t_title = r.find('a', attrs={'data-hook': "review-title"}, href=True)
                            title = t_title.find('span').text.strip()
                            cus = r.find('span', attrs={'class': 'a-profile-name'}).text.strip()
                            text_div = r.find('div', attrs={'class': 'a-row a-spacing-small review-data'})
                            review = text_div.find('span').text.strip()
                            like = r.find('span', attrs={'class': 'a-size-base a-color-tertiary cr-vote-text',
                                                         'data-hook': 'helpful-vote-statement'})
                            if like is not None:
                                temp1 = like.text.strip()
                                temp = temp1.split(' ')
                                if temp[0] == ('One'):
                                    likes.append(1)
                                else:
                                    likes.append(temp[0])
                            else:
                                likes.append(0)

                            que_lnk = r.find('a', attrs={
                                'class': 'a-size-base a-link-normal review-title a-color-base review-title-content a-text-bold',
                                'data-hook': 'review-title'}, href=True)
                            link = que_lnk['href']
                            rating = r.find('span', attrs={'class': 'a-icon-alt'}).text.strip()
                            clean = rating.split(' ')

                            print('================== Getting data for ' + cus + ' ==================')
                            titles.append(title)
                            ratings.append(clean[0])
                            reviews.append(review)
                            dates.append(q_date.strftime("%d %b %Y"))
                            c_names.append(cus)
                            url.append('https://www.amazon.in' + link)
                            uids.append(' ')
                            p_names.append(' ')

                            print('Rating : ' + rating)

                        elif q_date < (curr_dat - day_diff):
                            print('Date Expired')
                            return uids, p_names, c_names, dates, titles, ratings, reviews, likes, url
                else:
                    print(name)
                    c_names.append('100')
                    print("================ NO REVIEWS ================")
                    return uids, p_names, c_names, dates, titles, ratings, reviews, likes, url
            else:
                print("************** PAGE DOES NOT EXIST **************")
                print('https://www.amazon.in/product-reviews/' + str(
                    uid) + '?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=' + str(no_pages))
                print(name_p)
                likes.append('0')
                ratings.append('0')
                reviews.append('PAGE DOES NOT EXIST')
                dates.append('NA')
                c_names.append('= ERROR - 404 =')
                url.append('https://www.amazon.in/dp' + uid)
                uids.append(uid)
                p_names.append(' ')
                return uids, p_names, c_names, dates, titles, ratings, reviews, likes, url

            # --------------------------------------- PAGE CONCEPT HERE ---------------------------------------
            tm.sleep(3)
            if soup.find('li', attrs={'class': 'a-disabled a-last'}):
                flag = True
                print('//////END OF PAGES///////')
                return uids, p_names, likes, c_names, dates, ratings, reviews, url
            tm.sleep(5)
            if soup.find('li', attrs={'class': 'a-last'}):  # Check if there are more pages?
                no_pages += 1
            else:
                print('SINGLE PAGE ONLY')
                return uids, p_names, likes, c_names, dates, ratings, reviews, url

        except requests.exceptions.ConnectionError:
            print('Connection refused')


for t in range(1, len(asin) - 1):  # len(asin) - 1
    if t % 100 is 0:
        print('SLEEPING FOR : ' + str(1800/60) + 'MINUTES')
        tm.sleep(1800)
    print('|||||||| PRODUCT NO : ' + str(t) + ' ||||||||')
    unique, product_name, cus_name, datess, titless, ratingss, reviewss, likess, urls = get_data(asin[t])
    if cus_name != '100':
        dict = {'ASIN': unique, 'Product Name': product_name, 'Customer Name': cus_name,
                'Question Date': datess, 'Title': titless, 'Rating': ratingss,
                'Review': reviewss,'Likes': likess, 'URL': urls}

df = pd.DataFrame(dict)

df.to_csv('./reviews.csv', index=False, encoding='utf-8')
