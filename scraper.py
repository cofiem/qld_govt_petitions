import os
import re
import sqlite3
import string
from datetime import datetime
from itertools import groupby
from typing import Optional, Any, Dict, List

import requests
from lxml import html


class QldGovtPetitions:
    """
    Gather the details of the petitions.
    """

    petition_list = 'https://www.parliament.qld.gov.au/work-of-assembly/petitions/current-epetitions'
    petition_item = 'https://www.parliament.qld.gov.au/work-of-assembly/petitions/petition-details?id={}'
    petition_sign = 'https://www.parliament.qld.gov.au/apps/E-Petitions/home/TermsAndConditions/{}'
    sqlite_db_file = 'data.sqlite'
    iso_datetime_format = '%Y-%m-%dT%H:%M:%S+10:00'
    regex_collapse_newline = re.compile(r'(\n|\r)+')
    regex_collapse_whitespace = re.compile(r'\s{2,}')
    regex_signatures = re.compile('signatures.*', re.DOTALL)

    allowed_chars = string.digits + string.ascii_letters + string.punctuation

    cache_chars = string.digits + string.ascii_letters
    local_cache_dir = 'cache'
    use_cache = False

    def run(self):
        current_time = datetime.today()

        db_conn = None
        try:
            db_conn = self.get_sqlite_db()
            self.create_sqlite_database(db_conn)

            print('Reading petition list')
            petition_list_page = self.download_html(self.petition_list)
            petition_items = self.parse_petition_list_page(petition_list_page)

            count_added = 0
            count_skipped = 0

            print('Reading petitions')
            for petition_item in petition_items:
                reference_num = petition_item['reference_num']
                url = self.petition_item.format(reference_num)
                petition_item_page = self.download_html(url)
                petition_detail = self.parse_petition_item_page(reference_num, url, current_time, petition_item_page)

                db_data = self.build_rows(petition_item, petition_detail)

                if not self.sqlite_petition_row_exists(db_conn, db_data['reference_num'], db_data['signatures']):
                    print('Adding {} - "{}"'.format(db_data['reference_num'], db_data['subject']))
                    self.sqlite_petition_row_insert(db_conn, db_data)
                    count_added += 1
                else:
                    print('Already exists {} - "{}"'.format(db_data['reference_num'], db_data['subject']))
                    count_skipped += 1

                db_conn.commit()

            print('Added {}, skipped {}, total {}'.format(count_added, count_skipped, count_added + count_skipped))
            print('Completed successfully.')

        finally:
            if db_conn:
                db_conn.close()

    def parse_petition_list_page(self, tree) -> List[Dict[str, Any]]:
        result = []

        if tree is None:
            return result

        rows = tree.xpath('//div[@class="petitions-item"]')
        for row in rows:
            a = row.xpath('div/div/a')[0]
            reference_num = self.custom_split(a.get('href'), '/=?')[-1].strip()
            item = {
                'reference_name': str.join(' ', row.xpath('./div/div/text()')).strip().strip('- '),
                'reference_num': reference_num,
                'title': a.text.strip(),
                'url': self.petition_item.format(reference_num),
                'signatures': row.xpath('div/div/p/span')[1].text.replace('Signatures', '').strip(),
                'closed_at': datetime.strptime(str.join(' ', row.xpath('div/div/p/text()')).strip(), '%d/%m/%Y'),
            }
            result.append(item)

        return result

    def parse_petition_item_page(self, reference_num, url, current_time, tree) -> Dict[str, Any]:
        content = tree.xpath('//div[@class="standard-box standard-box-content"]')[0]

        subject = content.xpath('//div/h2')[0].text_content().strip()
        principal = self.regex_collapse_whitespace.sub(' ', content.xpath('./div/div/div/blockquote')[0].text_content()).strip()
        sponsor = content.xpath('(./div/div/div/p)[3]/text()')[0].strip()
        posted_at = datetime.strptime(content.xpath('(./div/div/div/p)[4]/text()')[0].strip(), '%d/%m/%Y')
        closed_at = datetime.strptime(content.xpath('(./div/div/div/p)[5]/text()')[0].strip(), '%d/%m/%Y')
        signatures = content.xpath('./div/div/div/p/span/text()')[0].strip()
        body = content.xpath('./div[@class="petitionBody"]')[0].text_content().strip()
        eligibility = content.xpath('./h3[@class="eligibility"]')[0].text.replace('Eligibility -', '').strip()
        addressed_to = content.xpath('./div[@class="petitionHeading"]/strong')[0].text.replace('TO:', '').strip()
        item = {
            'retrieved_at': current_time,
            'url': url,
            'reference_num': reference_num,
            'subject': subject,
            'signatures': signatures,
            'closed_at': closed_at,
            'body': self.regex_collapse_whitespace.sub(' ', self.regex_collapse_newline.sub('\n', body)).strip(),
            'principal': principal,
            'eligibility': eligibility,
            'sponsor': sponsor,
            'posted_at': posted_at,
            'addressed_to': addressed_to,
        }

        return item

    def build_rows(self, petition_item: Dict[str, Any], petition_detail: Dict[str, Any]) -> Dict[str, Any]:
        """Create a row to be inserted into sqlite db."""

        for k, v in petition_item.items():
            if k in petition_detail and petition_detail[k] != v and k != 'principal':
                # raise 'List page info did not match details page info: {} --- {}'.format(petition_item, petition_detail)
                pass

        data = {
            'retrieved_at': petition_detail['retrieved_at'].strftime(self.iso_datetime_format),
            'url': petition_detail['url'],
            'reference_name': petition_item['reference_name'],
            'reference_num': petition_detail['reference_num'],
            'subject': petition_detail['subject'],
            'signatures': petition_detail['signatures'],
            'closed_at': petition_detail['closed_at'].strftime(self.iso_datetime_format),
            'body': petition_detail['body'],
            'principal': petition_detail['principal'],
            'eligibility': petition_detail['eligibility'],
            'sponsor': petition_detail['sponsor'],
            'posted_at': petition_detail['posted_at'].strftime(self.iso_datetime_format),
            'addressed_to': petition_detail['addressed_to'],
        }

        return data

    def normalise_string(self, value):
        if not value:
            return ''

        value = value.replace('’', "'")
        remove_newlines = value.replace('\n', ' ').replace('\r', ' ').strip()
        result = ''.join(c if c in self.allowed_chars else ' ' for c in remove_newlines).strip()
        return result

    def custom_split(self, value, chars):
        return [''.join(gp) for _, gp in groupby(value, lambda char: char in chars)]

    # ---------- SQLite Database -------------------------

    def sqlite_petition_row_exists(self, db_conn, reference_num, signatures):
        c = db_conn.execute(
            'SELECT COUNT() FROM data WHERE reference_num = ? AND signatures = ?',
            (reference_num, signatures))

        row = list(c.fetchone())
        match_count = int(row[0])

        return match_count > 0

    def sqlite_petition_row_insert(self, db_conn, row: Dict[str, Any]) -> int:
        c = db_conn.execute(
            'INSERT INTO data '
            '(retrieved_at, url, reference_name, reference_num, '
            'subject, signatures, closed_at, body, principal, '
            'eligibility, sponsor, posted_at, addressed_to) '
            'VALUES (?, ?, ?, ?, '
            '?, ?, ?, ?, ?, '
            '?, ?, ?, ?)',
            (row['retrieved_at'], row['url'], row['reference_name'], row['reference_num'],
            row['subject'], row['signatures'], row['closed_at'], row['body'], row['principal'],
            row['eligibility'],row['sponsor'], row['posted_at'], row['addressed_to'],))

        row_id = c.lastrowid

        return row_id

    def get_sqlite_db(self):
        conn = sqlite3.connect(self.sqlite_db_file)
        return conn

    def create_sqlite_database(self, db_conn):
        db_conn.execute(
            'CREATE TABLE '
            'IF NOT EXISTS '
            'data '
            '('
            'retrieved_at TEXT,'
            'url TEXT,'
            'reference_name TEXT,'
            'reference_num TEXT,'
            'subject TEXT,'
            'signatures TEXT,'
            'closed_at TEXT,'
            'body TEXT,'
            'principal TEXT,'
            'eligibility TEXT,'
            'sponsor TEXT,'
            'posted_at TEXT,'
            'addressed_to TEXT,'
            'UNIQUE (reference_num, signatures)'
            ')')

        db_conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS reference_num_signatures '
            'ON data (reference_num, signatures)')

    # ---------- Downloading -----------------------------

    def download_html(self, url: str):
        content = self.load_page(url)

        if not content:
            page = requests.get(url)
            if page.is_redirect or page.is_permanent_redirect or page.status_code != 200:
                content = None
            else:
                content = page.content
                self.save_page(url, content)

        if not content:
            return None

        tree = html.fromstring(content)
        return tree

    # ---------- Local Cache -----------------------------

    def cache_item_id(self, url):
        item_id = ''.join(c if c in self.cache_chars else '' for c in url).strip()
        return item_id

    def save_page(self, url, content) -> None:
        if not self.use_cache:
            return

        os.makedirs(self.local_cache_dir, exist_ok=True)
        item_id = self.cache_item_id(url)
        file_path = os.path.join(self.local_cache_dir, item_id + '.txt')

        with open(file_path, 'wb') as f:
            f.write(content)

    def load_page(self, url) -> Optional[bytes]:
        if not self.use_cache:
            return None

        os.makedirs(self.local_cache_dir, exist_ok=True)
        item_id = self.cache_item_id(url)
        file_path = os.path.join(self.local_cache_dir, item_id + '.txt')

        if not os.path.isfile(file_path):
            return None

        with open(file_path, 'rb') as f:
            return f.read()


petitions = QldGovtPetitions()
petitions.run()
