import json
import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('ProductAvgRating')
sc = SparkContext(conf=conf)

rdd_products = sc.textFile(sys.argv[1])

def get_product_rating(product_json):
    product = json.loads(product_json)
    return (product.get('asin'), product.get('overall'))

rdd_product_rating = rdd_products.map(lambda product_json: get_product_rating(product_json))
rdd_product_avg_rating = (
    rdd_product_rating
        .aggregateByKey(
                (0, 0),
                lambda sum_count, rating: (sum_count[0] + rating, sum_count[1] + 1),
                lambda sum_count_x, sum_count_y: (sum_count_x[0] + sum_count_y[0],
                                                  sum_count_x[1] + sum_count_y[1])
            )
        .mapValues(lambda sum_count: sum_count[0] / sum_count[1])
)

def to_csv_line(data):
    delimiter = ','
    fields = []
    for item in data:
        field = str(item)
        if delimiter in field:
            field = '"{0}"'.format(field)
        fields.append(field)
    return delimiter.join(fields)

rdd_product_avg_rating.map(to_csv_line).saveAsTextFile(sys.argv[2])

