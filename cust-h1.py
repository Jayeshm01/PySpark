from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmountSpent")
sc = SparkContext(conf = conf)

def lineParse(line):
    cols = line.split(',')
    cust_id = float(cols[0])
    amount = float(cols[2])
    return (cust_id,amount)


lines = sc.textFile('customer-orders.csv')
rdd = lines.map(lineParse)
totalOrderValue = rdd.reduceByKey(lambda x,y: x + y)
totalOrderValueSorted = totalOrderValue.map(lambda x: (x[1], x[0])).sortByKey()
results = totalOrderValueSorted.collect()

for result in results:
    print(result)
