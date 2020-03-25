import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    customer = fields[0]
    spend = float(fields[2])
    return customer, spend


data = sc.textFile("../data/customer-orders.csv")
lines = data.map(parse_line)

sales_by_customer = lines.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1])
result = sales_by_customer.collect()

for customer, sales in result:
    print("customer {cx} spends {sales}".format(cx=customer, sales=sales))
