import ast
import json
import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('ProductNameRating')
sc = SparkContext(conf=conf)

rdd_products = sc.textFile(sys.argv[1])
rdd_products_meta = sc.textFile(sys.argv[2])

rdd_product_avg_rating = rdd_products.map(lambda csv_line: csv_line.split(','))
rdd_product_avg_rating.persist()

def get_product_name(product_json):
    # Hack for "JSONDecodeError: Expecting property name enclosed in double quotes"
    product = ast.literal_eval(product_json)
    return (product.get('asin'), product.get('title'))

rdd_product_name = (
    rdd_products_meta
        .map(lambda product_json: get_product_name(product_json))
        .filter(lambda product_data: all(product_data))
)
rdd_product_name.persist()

def to_csv_line(data):
    delimiter = ','
    fields = []
    for item in data:
        field = str(item)
        if delimiter in field:
            field = '"{0}"'.format(field)
        fields.append(field)
    return delimiter.join(fields)

rdd_product_name_rating = rdd_product_avg_rating.join(rdd_product_name)
(
    rdd_product_name_rating
        .map(lambda id__rating_name: (id__rating_name[0], id__rating_name[1][1], id__rating_name[1][0]))
	.map(to_csv_line)
        .saveAsTextFile(sys.argv[3])
)

