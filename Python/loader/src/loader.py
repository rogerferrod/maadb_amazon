import json
import gzip
import sys
import time
from optparse import OptionParser

from pymongo import MongoClient

"""
Prerequisiti:
database: "amazon"
collection: "products"
collection: "reviews"
in sharding
e replica

use amazon
sh.enableSharding("amazon")
sh.shardCollection("amazon.products",{"_id":"hashed"})

db.products.deleteMany({})

"""


def parse(path):
    g = gzip.open(path, 'r')
    for l in g:
        yield json.loads(json.dumps(eval(l)))


def main():
    global options

    client = MongoClient(options.hostname + ':' + options.port)
    db = client.amazon

    with open(options.input + '/products.txt', 'r') as prod_file:
        products = [line.replace('\n', '') for line in prod_file]

    count = 0
    start_time = time.time()
    for prod in parse(options.input + "\\metadata.json.gz"):
        asin = prod['asin']
        if asin in products:
            db.products.insert_one(prod)

        count += 1
        if options.limit is not None and count >= options.limit:
            break

    db.products.create_index('asin')
    end_time = time.time()
    print("insert products completed in {0} sec".format(end_time - start_time))

    count = 0
    start_time = time.time()
    for rev in parse(options.input + "\\reviews.json.gz"):
        asin = rev['asin']
        if asin in products:
            db.reviews.insert_one(rev)

        count += 1
        if options.limit is not None and count >= options.limit:
            break

    db.reviews.create_index('asin')
    end_time = time.time()
    print("insert reviews completed in {0} sec".format(end_time - start_time))


if __name__ == "__main__":
    print("Loader")

    argv = sys.argv[1:]
    parser = OptionParser()
    parser.add_option("-i", "--input", help='input folder', action="store", type="string", dest="input",
                      default="D:\Amazon_dataset\Cellphone")
    parser.add_option("-n", "--host", help='hostname mongoDB', action="store", type="string", dest="hostname",
                      default="127.0.0.1")
    parser.add_option("-p", "--port", help='port server mongoDB', action="store", type="string", dest="port",
                      default="27017")
    parser.add_option("-l", "--limit", help='input limit', action="store", type="int", dest="limit")

    (options, args) = parser.parse_args()

    if options.input is None or options.hostname is None or options.port is None:
        print("mandatory arguments missing")
        sys.exit(2)

    main()
