import os
import uuid
import requests
from multiprocessing.dummy import Pool
from functools import partial


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
        self.pool = Pool(500)

    def spider(self, url):
        r = requests.get(url=url, headers=self.headers)
        r.raise_for_status()
        return r.content

    def save(self, content, classification):
        with open(
                file=os.path.abspath(
                    os.path.join(
                        self.save_folder.get(classification),
                        str(uuid.uuid4()))),
                mode='wb') as f:
            f.write(content)

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
