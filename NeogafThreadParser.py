"""
 HaloGAF Thread Parser
 Author: Rob White
 Created: October 4, 2015
"""

from bs4 import BeautifulSoup
import urllib2
import re
import csv
from collections import defaultdict, Counter
from datetime import date, timedelta


class ThreadParser:

    def __init__(self, thread_base_url, output_filename='results.csv', lexicon_filename='sentiment-lexicon.txt'):
        self.thread_base_url = thread_base_url
        self.lexicon_dict = self._lexicon_file_to_dict(lexicon_filename)
        self.output_filename = 'Files/' + output_filename

    def run(self):

        thread_results = []
        current_page = 1
        max_page = self._get_max_thread_page()

        # For each page, append the dictionary on to an array.
        while current_page <= max_page:
            print('Page ' + str(current_page) + '/' + str(max_page))
            paged_url = self._get_next_url(current_page)
            page_results = self._get_page_sentiment_results(paged_url)
            thread_results += page_results
            current_page += 1

        grouped_results = self._group_and_sum_thread_results(thread_results, 'date', ['score', 'posts'])

        self._write_csv_file(grouped_results)

    def _lexicon_file_to_dict(self, filename):
        lexicon_dict = {}
        with open(filename, 'rb') as source:
            for line in source:
                fields = line.split('\t')
                lexicon_dict[fields[0]] = float(fields[1].split()[0])
        return lexicon_dict

    def _write_csv_file(self, thread_results):
        csv_file = open(self.output_filename, 'w')
        fieldnames = ['date', 'score', 'posts']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for result in thread_results:
            writer.writerow(result)

    def _get_page_sentiment_results(self, url):

        html = urllib2.urlopen(url)
        soup = BeautifulSoup(html, 'lxml')

        post_results = []

        posts = soup.find_all('div', class_='postbit alt2 clearfix')
        for post in posts:
            post_data = {'score': 0}

            post_text = post.find('div', class_='post').text
            post_words = post_text.split()

            for word in post_words:
                if word in self.lexicon_dict:
                    post_data['score'] += self.lexicon_dict[word]

            post_data['posts'] = 1
            post_data['score'] = post_data['score']
            post_data['date'] = self._get_post_date(post)
            post_results.append(post_data)

        return post_results

    def _group_and_sum_thread_results(self, results, group_by_key, sum_value_keys):
        container = defaultdict(Counter)

        for item in results:
            key = item[group_by_key]
            values = {k: item[k] for k in sum_value_keys}
            container[key].update(values)

        new_dataset = [
            dict([(group_by_key, item[0])] + item[1].items())
            for item in container.items()
        ]
        new_dataset.sort(key=lambda item: item[group_by_key])

        return new_dataset

    def _get_post_date(self, post):
        post_details = post.find('div', class_='postbit-details')
        match = re.search(r'(\d{2}-\d{2}-\d{4},\s\d{2}:\d{2}\s(AM|PM))', post_details.text)
        today_match = re.search(r'(Today,\s\d{2}:\d{2}\s(AM|PM))', post_details.text)
        yesterday_match = re.search(r'(Yesterday,\s\d{2}:\d{2}\s(AM|PM))', post_details.text)
        if not match:
            if today_match:
                today = date.today()
                the_date = today.strftime('%m-%d-%Y')
            if yesterday_match:
                yesterday = date.today() - timedelta(1)
                the_date = yesterday.strftime('%m-%d-%Y')
        else:
            the_date = match.group(1)[:10]
        return the_date

    def _get_max_thread_page(self):
        html = urllib2.urlopen(self.thread_base_url + '&page=100000')
        soup = BeautifulSoup(html)
        li = soup.find('li', {'class': 'current'})
        return int(li.text)

    def _get_next_url(self, page_num):
        return self.thread_base_url + '&page=' + str(page_num)


def main():

    """
     -- Some example URLs --
     http://www.neogaf.com/forum/showthread.php?t=154012 (Halo 3 Beta Thread)
     http://www.neogaf.com/forum/showthread.php?t=187715 (Halo 3 Official Thread)
     http://www.neogaf.com/forum/showthread.php?t=497808 (Halo 4 OT1)
     http://www.neogaf.com/forum/showthread.php?t=580093 (Halo 4 OT2)
     http://www.neogaf.com/forum/showthread.php?t=929710 (MCC OT1)
     http://www.neogaf.com/forum/showthread.php?t=950368 (MCC OT2)
     http://www.neogaf.com/forum/showthread.php?t=701461 (Battlefield 4 OT1)
     http://www.neogaf.com/forum/showthread.php?t=1120872 (Metal Gear Insurance)
     http://www.neogaf.com/forum/showthread.php?t=581271 (Microsoft E3 2013)
    """

    parser = ThreadParser('http://www.neogaf.com/forum/showthread.php?t=1120872', 'MetalGearInsurance.csv')
    parser.run()

if __name__ == "__main__":
    main()
