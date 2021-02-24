import csv
import random
import smtplib
import ssl
import time as tm
from datetime import datetime as dt
from datetime import timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.headerregistry import Address
from email.header import Header
from email.utils import formataddr

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Email headers

receiver_email = []
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

with open('C:/Users/33669/PycharmProjects/txt_rev_scrapper/headers.csv') as csv_file:  # Read headers for avoiding IP timeout
    reader2 = csv.reader(csv_file, delimiter='\n')
    for col in reader2:
        header = col[0]
        headerss.append(header)

with open('C:/Users/33669/PycharmProjects/txt_rev_scrapper/asin.csv') as csv_file:  # Read Asin values from the csv
    reader = csv.reader(csv_file, delimiter=',')
    for col in reader:
        asin_t = col[1]
        asin.append(asin_t)
    time_prd = asin[len(asin) - 1]

with open('C:/Users/33669/PycharmProjects/txt_rev_scrapper/contacts_file.csv') as file:  # Read emailIDs for the automated mail response
    reader = csv.reader(file, delimiter='\n')
    next(reader)  # Skip header row
    for email in reader:
        receiver_email.append(email[0])

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
        if no_pages % 2 == 0:  # To sleep for avoiding timeouts
            print(
                'SLEEPING FOR : ' + str((no_pages / 2) * 20) + ' SECONDS.')
            tm.sleep(20 * (no_pages / 2))
        print(' ||| PAGE NUMBER - ' + str(no_pages) + ' |||')
        hedr = random.choice(headerss)
        headers = {"User-Agent": hedr,
                   "Accept-Encoding": "gzip, deflate",
                   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT": "1",
                   "Connection": "close", "Upgrade-Insecure-Requests": "1"}
        tm.sleep(3)
        session = requests.Session()
        session.verify = False
        retry = Retry(connect=500, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        r = session.get('https://www.amazon.in/product-reviews/' + str(
            uid) + '?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=' + str(no_pages),
                        #verify='./amzn_certi.cer',
                        headers=headers)  # , proxies=proxies) : ASIN example - B01L7C4IU2
        print('https://www.amazon.in/product-reviews/' + str(
            uid) + '?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=' + str(no_pages))
        content = r.content
        soup = BeautifulSoup(content)

        name_p = soup.find('a',
                           attrs={'class': 'a-link-normal',
                                  'data-hook': 'product-link'}, href=True)
        container = soup.find('div', attrs={'id': 'cm_cr-review_list',
                                            'class': 'a-section a-spacing-none review-views celwidget'})  # Whole review container
        if name_p or container is not None:
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
            print('================== Getting data for ' + name + ' ==================')
            tm.sleep(3)

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
                        print('------------------ Date Expired ------------------')
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
            return uids, p_names, c_names, dates, titles, ratings, reviews, likes, url
        tm.sleep(5)
        if soup.find('li', attrs={'class': 'a-last'}):  # Check if there are more pages?
            no_pages += 1
        else:
            print('SINGLE PAGE ONLY')
            return uids, p_names, c_names, dates, titles, ratings, reviews, likes, url




def send_mail(receiver):

    sender_email = 'utkarsh.kharayat@havells.com'
    subject = "*AUTOMATED* Daily Amazon report *TEST*"
    body = """
    <html>
      <body style="text-align: center; color: blue;">
        <p>************ Automated Mail for daily Amazon feedback ************<br>
           Find attached CSV files.<br><br>
           *****    DO     -   NOT   -    REPLY   *****
        </p>
      </body>
    </html>
    """
    message = MIMEMultipart()
    message["From"] = 'Utkarsh Kharayat <utkarsh.kharayat@havells.com>'
    message["To"] = sender_email
    message["Subject"] = subject
    # message["BCC"] = receiver

    message.attach(MIMEText(body, "html"))
    filename = []
    filename.append("C:/Users/33669/PycharmProjects/txt_rev_scrapper/reviews.csv")
    filename.append("C:/Users/33669/PycharmProjects/rev_scraper/ratings.csv")
    filename.append("C:/Users/33669/PycharmProjects/amzn_scraper/questions.csv")
    for f in range(0, len(filename)):
        with open(filename[f], "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)
        splits = filename[f].split('/')
        name = splits[len(splits) - 1]
        # Add header as key/value pair to attachment part
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {name}",
        )

        # Add attachment to message and convert message to string
        message.attach(part)
        text = message.as_string()
    context = ssl.create_default_context()

    for destination in receiver:
        with smtplib.SMTP('smtp.havells.com', 2521) as server:
            server.ehlo()  # Can be omitted
            server.starttls(context=context)
            server.ehlo()  # Can be omitted
            server.sendmail(sender_email, destination, text)  # server.sendmail(text)

def exception_mail(receiver):
    sender_email = 'utkarsh.kharayat@havells.com'
    subject = "*AUTOMATED* Exception occurred *TEST*"
    body = """
    <html>
      <body style="text-align: center; color: red;">
        <p>************ Automated Mail for Exception ************<br>
           Exception occurred while running Review scraping.<br><br>
           *****    DO     -   NOT   -    REPLY   *****
        </p>
      </body>
    </html>
    """
    message = MIMEMultipart()
    message["From"] = 'Utkarsh Kharayat <utkarsh.kharayat@havells.com>'
    message["To"] = sender_email
    message["Subject"] = subject
    # message["BCC"] = receiver

    message.attach(MIMEText(body, "html"))
    filename = []
    filename.append("C:/Users/33669/PycharmProjects/txt_rev_scrapper/reviews.csv")
    filename.append("C:/Users/33669/PycharmProjects/rev_scraper/ratings.csv")
    filename.append("C:/Users/33669/PycharmProjects/amzn_scraper/questions.csv")
    for f in range(0, len(filename)):
        with open(filename[f], "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)
        splits = filename[f].split('/')
        name = splits[len(splits) - 1]
        # Add header as key/value pair to attachment part
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {name}",
        )

        # Add attachment to message and convert message to string
        message.attach(part)
        text = message.as_string()
    # Create a secure SSL context
    context = ssl.create_default_context()

    for destination in receiver:
        with smtplib.SMTP('smtp.havells.com', 2521) as server:
            server.ehlo()  # Can be omitted
            server.starttls(context=context)
            server.ehlo()  # Can be omitted
            server.sendmail(sender_email, destination, text)  # server.sendmail(text)

try:
    for t in range(1, 3):  # len(asin) - 1
        if t % 2 == 0:
            print('SLEEPING FOR : ' + str(4) + ' Seconds')
            tm.sleep(4)
        print('==== REVIEWS ====|||||||| PRODUCT NO : ' + str(t) + ' ||||||||')

        unique, product_name, cus_name, datess, titless, ratingss, reviewss, likess, urls = get_data(asin[t])
        if cus_name != '100':
            dict = {'ASIN': unique, 'Product Name': product_name, 'Customer Name': cus_name,
                    'Question Date': datess, 'Title': titless, 'Rating': ratingss,
                    'Review': reviewss, 'Likes': likess, 'URL': urls}

    for t in range(1, len(receiver_email)):  # len(asin) - 1
        print('|||||||| SENDING MAIL TO : ' + str(receiver_email[t]) + ' ||||||||')
    send_mail(receiver_email)

except requests.exceptions.ConnectionError:
    df_temp = pd.DataFrame(dict)
    df_temp.to_csv('./reviews.csv', index=False, encoding='utf-8')
    print('////////////////////////////////// Connection refused - ' + str(
        dt.today()) + ' //////////////////////////////////')
    mailid = ['utkarsh.kharayat@havells.com',
              'arush.agarwal@havells.com']
    exception_mail(mailid)  # Send Exception mail
except ssl.SSLCertVerificationError:
    print('////////////////////////////////// SSL Cerificate ISSUE - ' + str(
        dt.today()) + ' //////////////////////////////////')
df = pd.DataFrame(dict)
df.to_csv('./questions.csv', index=False, encoding='utf-8')

