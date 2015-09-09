
import os
import sys

# use brew install pyspark, open pyspark from terminal, import sys, print sys.path, copy py4j part and paste below
# os.environ['SPARK_HOME']="/usr/local/Cellar/apache-spark/1.4.1"
# sys.path.append("/usr/local/Cellar/apache-spark/1.4.1/libexec/python/")
# sys.path.append("/usr/local/Cellar/apache-spark/1.4.1/libexec/python/lib/py4j-0.8.2.1-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)

import datetime


def separateBlocks(iterator):
    error = 0
    prev = ''
    for item in iterator:
        cur = item
        if cur < prev:
            error += 1

        prev = cur

    return [error]


def main(arglist):

    with open("log_file_v.txt", "a") as f:
        f.write("Start time of validation...... %s\n" % datetime.datetime.now())

    print("Start time of validation...... %s" % datetime.datetime.now())

    # mapreduce params
    output = arglist[0]
    minPartitions = int(arglist[1])

    # initialize
    sc = SparkContext(appName="PythonValidate")

    # rdd = sc.textFile(output_file_name, minPartitions=minPartitions)
    rdd = sc.wholeTextFiles(output, minPartitions=minPartitions)
    print('partitions', rdd.getNumPartitions())
    error_count = rdd.mapPartitions(separateBlocks).sum()

    sc.stop()

    print("End time of validation...... %s" % datetime.datetime.now())
    with open("log_file_v.txt", "a") as f:
        f.write("End time of validation...... %s\n" % datetime.datetime.now())
        f.write("Error count of sorted file...... %s" % error_count)

    f.close()


if __name__ == "__main__":

    main(sys.argv[1:])
