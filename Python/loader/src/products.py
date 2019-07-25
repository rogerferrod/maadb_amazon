from optparse import OptionParser
import json
import sys
import time
import html
import gzip


def parse(path):
    g = gzip.open(path, 'r')
    for l in g:
        yield json.loads(json.dumps(eval(l)))


def main():
    global options

    group = ['Cell Phones', 'Phones', 'Batteries', 'Chargers']
    results = set()

    count = 0

    start_time = time.time()

    for product in parse(options.input + "\\metadata.json.gz"):
        asin = product['asin']

        if 'categories' in product.keys():
            for cat in product['categories']:
                for c in cat:
                    category = html.unescape(c)
                    if category in group:
                        results.add(product['asin'])

        count += 1
        # if count >= 1000:  # temporaneoooo
        # break

    end_time = time.time()
    print("{0} lines".format(count))
    print("{0} sec".format(end_time - start_time))

    with open(options.output + '/rev_cell_all.txt', 'w', encoding='utf-8') as out:
        out.write(str(group) + '\n')
        for p in results:
            out.write(p + '\n')


if __name__ == "__main__":
    print("Products")

    argv = sys.argv[1:]
    parser = OptionParser()
    parser.add_option("-o", "--output", help='output folder', action="store", type="string", dest="output",
                      default="../output")
    parser.add_option("-i", "--input", help='input folder', action="store", type="string", dest="input",
                      default="D:\Amazon_dataset\Cellphone")

    (options, args) = parser.parse_args()

    if options.input is None:
        print("input/output path missing")
        sys.exit(2)

    main()
