import urllib.request
import time
import schedule
import pandas as pd
import sqlite3
import os

KEY = 'c6329c918853f9964713ca759691e42775cc16ebac6c3b9e18c943ce481a4478'
PHISHTANK_URL = 'http://data.phishtank.com/data/' + KEY + '/online-valid.csv'
PHISHTANK_DIR = './phishtank_csv/'


def download_phishtank(url=PHISHTANK_URL, dir=PHISHTANK_DIR):

    curr_time = str(int(time.time()))
    filename = curr_time + '-online-valid.csv'
    urllib.request.urlretrieve(url, dir+filename)

    update_db(dir+filename)


def update_db(file_path):

    db = sqlite3.connect('./phishtank.db')
    cursor = db.cursor()

    df = pd.read_csv(file_path, low_memory=False)
    for index, row in df.iterrows():
        if len(cursor.execute("SELECT * FROM phishtank WHERE phish_id=" + str(row['phish_id'])).fetchall()) == 0:
            print(row['phish_id'])
            sql = ''' INSERT INTO phishtank (phish_id,url,phish_detail_url,submission_time,verified,verification_time,online,target)
                          VALUES(?,?,?,?,?,?,?,?) ''' % (row['phish_id'], row['url'], row['phish_detail_url'], row['submission_time'], row['verified'], row['verification_time'], row['online'], row['target'])
            cursor.execute(sql)
            db.commit()
        else:
            print("Existed")


if __name__ == '__main__':

    # run()
    # download_phishtank()
    schedule.every().hour.do(download_phishtank)

    while True:
        schedule.run_pending()
        time.sleep(60)
