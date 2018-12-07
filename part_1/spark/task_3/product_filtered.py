import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('ProductFiltered')
sc = SparkContext(conf=conf)

rdd_products = sc.textFile(sys.argv[1])
rdd_products.persist()

filter_word = sys.argv[3]

def filter_by_title(title):
    return filter_word.lower() in title.lower()

(
    rdd_products
        .filter(lambda id_title_rating_csv_line: filter_by_title(id_title_rating_csv_line.split(',')[1]))
        .saveAsTextFile(sys.argv[2])
)

