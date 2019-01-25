import os
import uuid
import requests
from multiprocessing.dummy import Pool
from functools import partial
import logging
import time


class Logger():
    def __init__(self, filename='download.log'):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filename=filename,
            filemode='a')

    def print_log(self):
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(name)-12s: %(levelname)-8s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)


class Path():
    def __init__(self):
        self._save_folder = False

    def url_file(self):
        return dict(
            drawings=os.path.join('raw_data', 'drawings', 'urls_drawings.txt'),
            hentai=os.path.join('raw_data', 'hentai', 'urls_hentai.txt'),
            neutral=os.path.join('raw_data', 'neutral', 'urls_neutral.txt'),
            porn=os.path.join('raw_data', 'porn', 'urls_porn.txt'),
            sexy=os.path.join('raw_data', 'sexy', 'urls_sexy.txt'))

    def save_folder(self):
        _save = dict(
            drawings=os.path.join('raw', 'drawings'),
            hentai=os.path.join('raw', 'hentai'),
            neutral=os.path.join('raw', 'neutral'),
            porn=os.path.join('raw', 'porn'),
            sexy=os.path.join('raw', 'sexy'))
        if not self._save_folder:
            [
                os.makedirs(name=v) for k, v in _save.items()
                if not os.path.exists(path=v)
            ]
        self._save_folder = True
        return _save


class Downloader():
    def __init__(self):
        self.headers = {
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
        }
        self.path = Path()
        self.url_file = self.path.url_file()
        self.save_folder = self.path.save_folder()
        self.pool = Pool(2000)
        self.max_retry = 5
        logger = Logger()
        logger.print_log()

    def spider(self, url, retry=0):
        url = url.strip()
        try:
            r = requests.get(url=url, headers=self.headers, timeout=5)
            r.raise_for_status()
            if sum(len(chunk) for chunk in r.iter_content(512)) < 1024:
                logging.warning('{url} filtered'.format(url=url))
                return None
            return {'content': r.content, 'url': url}
        except Exception as e:
            logging.warning('{url} error, retry: {retry_times}'.format(
                url=url, retry_times=retry))
            if retry < self.max_retry:
                retry += 1
                time.sleep(1)
                self.spider(url=url, retry=retry)
            else:
                return None

        # logging.info('{url} ok'.format(url=url))

    def save(self, content, classification):
        if content:
            with open(
                    file=os.path.abspath(
                        os.path.join(
                            self.save_folder.get(classification),
                            str(uuid.uuid4()))),
                    mode='wb') as f:

                f.write(content.get('content'))
            logging.info('{url} ok'.format(url=content.get('url')))

    def parse(self):
        urls = dict()
        for k, v in self.url_file.items():
            with open(file=os.path.abspath(v), mode='r', encoding='utf8') as f:
                urls.setdefault(k, f.readlines())
        return urls

    def download(self, url, classification):
        self.save(content=self.spider(url=url), classification=classification)

    def run(self):
        urls = self.parse()
        for k, v in urls.items():
            fake = partial(self.download, classification=k)
            f = self.pool.map_async(fake, v)
            f.wait()


def test():
    downloader = Downloader()
    downloader.run()


if __name__ == '__main__':
    test()
