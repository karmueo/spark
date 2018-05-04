# @File  : rdd_test.py
# @Author: 沈昌力
# @Date  : 2018/4/20
# @Desc  :
from pyspark import SparkConf, SparkContext

sc = SparkContext(master="local[8]", appName="rdd_test")


lines = sc.textFile("file:///D:/ProgramData/spark-2.3.0-bin-hadoop2.7/README.md")
print(lines.first())



def _fun(line):
    s = line.split(" ")
    new_ = {}
    for i in range(len(s)):
        new_[i] = s[i]
    return new_


lineLengths = lines.map(_fun)
print(lineLengths.collect())

print(lineLengths.take(4))

lineLengths.foreach(print)

def _fun2(line):
    s = line.split(" ")
    return len(s)

def _fun3(line1_words, line2_words):
    if line1_words > line2_words:
        return line1_words
    else:
        return line2_words

rdd2 = lines.map(_fun2)
res = rdd2.reduce(_fun3)
print(res)





# nums = [1, 2, 3, 4, 5]
# rdd = sc.parallelize(nums)
