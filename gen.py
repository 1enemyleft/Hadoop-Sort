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

import random
import string
import datetime

WORDS = open('words.txt').read().split()


def randomGenerator(x, str_max_length=20):

    random.seed(x)
    char_pool = string.ascii_lowercase + string.digits
    len = random.randint(1, str_max_length)

    return ''.join(random.choice(char_pool) for _ in range(len))


def randomWord(x):
    random.seed(x)
    return random.choice(WORDS)


def main(arglist):

    with open("log_file.txt", "w") as f:
        f.write("Start time of gen...... %s\n" % datetime.datetime.now())

    print("Start time...... %s" % datetime.datetime.now())

    # mapreduce params
    size = int(arglist[0])
    output_dir = arglist[1]
    numSlices = int(arglist[2])

    # initialize
    sc = SparkContext(appName="PythonGen")

    sc.parallelize(xrange(0, int(50*1024*1024*size)), numSlices=numSlices).map(randomWord).saveAsTextFile(output_dir)

    sc.stop()

    print("End time...... %s" % datetime.datetime.now())
    with open("log_file.txt", "a") as f:
        f.write("End time of gen...... %s\n" % datetime.datetime.now())


if __name__ == "__main__":

    main(sys.argv[1:])
