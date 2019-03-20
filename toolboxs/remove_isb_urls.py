from indexclient.client import IndexClient
from scripts.settings import INDEXD


def main():
    """
    Remove all isb bucket urls from indexd
    """
    indexclient = IndexClient(INDEXD['host'], INDEXD['version'], INDEXD['auth'])
    it = indexclient.list(page_size=1000)
    n = 0
    for doc in it:
        n = n + 1
        need_update = False
        urls = doc.urls
        urls_metadata = doc.urls_metadata
        for url in doc.urls:
            if "gs://isb" in url or "gs://5aa919de" in url or "gs://62f2c827" in url or "gs://7008814a" in url or "gs://t358dcaa" in url:
                need_update = True
                urls.remove(url)
                urls_metadata.pop(url, None)
        if need_update:
            doc.urls = urls
            doc.urls_metadata = urls_metadata
            try:
                doc.patch()
            except Exception as e:
                print e
        if n % 1000 == 0:
            print("Finish {}".format(n))


if __name__ == '__main__':
    main()
