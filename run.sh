#!/bin/bash

# a dictionary txt must be named 'word.txt' and put in the same folder as python files
# there must be no folder named 'input files folder' below

#rm -rf input_sample
# generate text data from dictionary, file size = 100mb, input files foler = input_sample, numSlices = 24
#spark-submit --driver-memory 10g gen.py 20 input_sample 24
#spark-submit --master spark://SRA2012:7077 --driver-memory 10g gen.py 20 input_sample 30

# sort the file pieces, input files folder = input_sample, sorted file name = output_sample, partition = 24
spark-submit --master spark://SRA2012:7077 --driver-memory 10g final/test_wc.py input_sample output_sample 30

# validate the sorted data, sorted file name = output_sample, partition = 24
spark-submit --master spark://SRA2012:7077 --driver-memory 10g validate.py output_sample 30

# see log_file.txt in the same folder for running results
