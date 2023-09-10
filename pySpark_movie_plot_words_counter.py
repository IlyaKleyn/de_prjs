

# a small program to count the number of words in the plot of the movie and returns three columns as a result
# film_id","plot", 'cnt_plot_words'

import pandas as pd
import pyspark 
import pymongo
import numpy as np
from pyspark.sql import SparkSession
from os.path import join

# Import SparkSession
from pyspark.sql import SparkSession 

# Create SparkSession 
spark = SparkSession.builder.appName("my spark app").master("local[*]").getOrCreate()

sc = spark.sparkContext



# Read JSON file into dataframe (only columns that we need for word counts)
movies_df = spark.read.json("movies.json")['_id', 'plot']
# Prints out the schema in the tree format
movies_df.printSchema()
# show() is used to display the contents of the DataFrame 
movies_df.show()

# returns the complete dataset as an Array
# collected_plots=movies_df.collect()
# convert the dataframe to rdd because map operation is not available in DataFrame 
# split each plot by space in an RDD 
splt_plots_rdd=movies_df.rdd.map(lambda x: (x[0],x[-1],x[-1].split(" ")))
# apply map to get id, not splitted plot and the number of words in splitted plot
splt_plots_rdd2=splt_plots_rdd.map(lambda x: (x[0], x[-2], len(x[-1])))

# Take the first elements of the RDD and display to verity that the result is correct
splt_plots_rdd2.take(1)

#  Convert PySpark RDD to DataFrame
Columns = ["film_id","plot", 'cnt_plot_words']
cnt_df = splt_plots_rdd2.toDF(Columns)
# show() is used to display the contents of the DataFrame 
cnt_df.show()
