#This assignment was dedicated to practise on analyzing the data via Spark RDD, DataFrame and Spark SQL. 
#the task assumed working on daily statistics for trending YouTube videos collected from Youtube via an open source tool called Trending-YouTube-Scraper.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       

#YouTube (the world-famous video sharing website) maintains a list of the top trending videos on the platform. According to Variety magazine, 
#“To determine the year’s top-trending videos, 
#YouTube uses a combination of factors including measuring users interactions (number of views, shares, comments and likes).

#Note that they are not the most-viewed videos overall for the calendar year”. Top performers on the YouTube trending list are music videos 
#(such as the famously virile “Gangnam Style”), celebrity and/or reality TV performances, and the random dude-with-a-camera viral videos that YouTube is well-known for. 
#This dataset is a daily record of the top trending YouTube videos.






# import necessary libraries
import findspark
import pyspark

# findspark ads pyspark to sys.path at runtime
findspark.init()
# import a class SparkSession. It is the entry point to programming Spark with the Dataset and DataFrame API
from pyspark.sql import SparkSession
# using a  Builder attribute to construct SparkSession instances.
# The entry point to the Spark application for creating a DataFrame is SparkSession, 
# which defines the configuration parameters: the name of the application, the cluster manager 
# (i.e. how to connect - locally, to Kubernates or YARN, etc.), 
# the number of allocated cores and memory. An example of initialization may look like this:
spark = (SparkSession.builder
.master("local[6]")
.appName('PySpark_Tutorial')
.config("spark.driver.host", "127.0.0.1")
.getOrCreate()) # get Or Create() returns an already existing Spark Session; if it does not exist, a new Spark Session is created

# Spark Session internally creates Spark Config and SparkContext

sc = spark.sparkContext


### Spark RDD part ###

#1. [3 points] Extract category titles from the JSON file and store it as a dictionary of 
#key value pairs where the key is the category id and the value is the category title. 
#Use this shared variable to map category ids to category titles
# completed by: Kleyn IS

# Read JSON file into dataframe (only columns that we need for word counts)
movies_df = spark.read.option("multiline", "true").json("MX_category_id.json")
# Prints out the schema in the tree format
movies_df.printSchema()
# show() is used to display the contents of the DataFrame 
movies_df.show()

# //explode items array field using explode
from pyspark.sql.functions import explode
items_df = movies_df.select(explode(movies_df["items"])).toDF("items")

# create the dataframe with category id and title of films

title_and_cat_id_df=items_df.select("items.id", "items.snippet.title")

# create the dataframe for which we will use for test mapping based on the broadcast variable
cat_and_titles_df=items_df.select("items.id", "items.snippet.title")


# create a list of Rows to create a dictionary for mapping
collected_title_and_cat_id_df=title_and_cat_id_df.collect()

# turn list into dictionary
bv = dict(collected_title_and_cat_id_df)
# create the broadcast variable for mapping
# create Broadcast variable using the broadcast(v) method of the SparkContext class
broadcastVar = sc.broadcast(bv)
print(broadcastVar.value)

# Using broadcast variables 
# create the udf that allows us
# to use the broadcast variable to cache the category id - category title lookup 
# info on each machine and tasks use this cached info while executing the transformations
def cat_title_mapper(cat_id):

    return broadcastVar.value[cat_id]

# here we convert the dataframe to rdd to apply map
# After that we use map to apply the broadcast mapping using shared variable
mapped_cat_title_rdd = cat_and_titles_df.rdd.map(lambda x: (x[0],x[-1],cat_title_mapper(x[0])))
# see 10 rows in mapped rdd
# the 3-rd column with mapped category title was added 
mapped_cat_title_rdd.take(10)

# 2. [3 points] Read csv file and extract only video titles from it. 
# For simplicity, you can read it as Spark DataFrame and convert into rdd after extracting.

# completed by: Kleyn IS

MXvideos_df = spark.read.option("header",True).csv("MXvideos.csv")

# convert the dataframe to rdd using rdd 
# convert the dataframe to rdd to aplly actions and transformations
# Since PySpark 1.3, it provides a property .rdd 
# on DataFrame which returns the PySpark RDD class object of DataFrame (converts DataFrame to RDD).
MXvideos_rdd=MXvideos_df.rdd

# then apply map() transformation to extract title column

video_title_rdd=MXvideos_rdd.map(lambda line : line['title'])

# check the output 
# The action take(n) returns n number of elements from RDD
video_title_rdd.take(5)


# 3. [2 points] Read csv file again and extract category ids. 
# For simplicity, you can read it as Spark DataFrame and convert into rdd after extracting

# completed by: Kleyn IS

MXvideos_df = spark.read.option("header",True).csv("MXvideos.csv")

# convert the dataframe to rdd using rdd 
# convert the dataframe to rdd to aplly actions and transformations
# Since PySpark 1.3, it provides a property .rdd 
# on DataFrame which returns the PySpark RDD class object of DataFrame (converts DataFrame to RDD).
MXvideos_rdd=MXvideos_df.rdd

# then apply map() transformation to extract category id column

video_cat_id_rdd=MXvideos_rdd.map(lambda line : line['category_id'])

# check the output 
# The action take(n) returns n number of elements from RDD
video_cat_id_rdd.take(5)


# 4. [2 points] Read csv file again and extract number of views. 
# For simplicity, you can read it as Spark DataFrame and convert into rdd after extracting
# views

# completed by: Kleyn IS

MXvideos_df = spark.read.option("header",True).csv("MXvideos.csv")

# convert the dataframe to rdd using rdd 
# convert the dataframe to rdd to aplly actions and transformations
# Since PySpark 1.3, it provides a property .rdd 
# on DataFrame which returns the PySpark RDD class object of DataFrame (converts DataFrame to RDD).
MXvideos_rdd=MXvideos_df.rdd

# extract only views column using map() transformation 
video_num_views_rdd=MXvideos_rdd.map(lambda line : line['views'])

# check the output 
# The action take(n) returns n number of elements from RDD
video_num_views_rdd.take(5)


# 5. [3 points] Display the first 5 video titles which have more than 1 million views.
# completed by: Kleyn IS


# import StructType class to define the structure of the DataFrame
# StructType is a collection or list of StructField objects
# import StructField class to define the columns which include column name(String), 
# column type (DataType), nullable column (Boolean) and metadata (MetaData)
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, BooleanType

# specify the structure using StructType and StructField classes
schema = StructType([ \
    StructField("video_id",StringType(),True), \
    StructField("trending_date",DateType(),True), \
    StructField("title",StringType(),True), \
    StructField("channel_title", StringType(), True), \
    StructField("category_id", IntegerType(), True), \
    StructField("publish_time", TimestampType(), True), \
    StructField("tags", StringType(), True), \
    StructField("views", IntegerType(), True), \
    StructField("likes", IntegerType(), True),
    StructField("dislikes", IntegerType(), True), \
    StructField("comment_count", IntegerType(), True), \
    StructField("thumbnail_link", StringType(), True), \
    StructField("comments_disabled", StringType(), True), \
    StructField("ratings_disabled", StringType(), True), \
    StructField("video_error_or_removed", StringType(), True), \
    StructField("description", StringType(), True)               
  ])

# Applying custom schema to data frame
# here we applied the customized schema to that CSV file by changing the names 

MXvideos_df = spark.read.format(
    "csv").schema(schema).option(
    "header", True).load("MXvideos.csv")

# extract only title and views column
video_titles_df=MXvideos_df.select('title', 'views')

# drop rows with null values in title and views columns
video_titles_df=video_titles_df.na.drop(subset=['title', 'views'])

# convert the dataframe to rdd to aplly actions and transformations
# Since PySpark 1.3, it provides a property .rdd 
# on DataFrame which returns the PySpark RDD class object of DataFrame (converts DataFrame to RDD).
video_titles_rdd=video_titles_df.rdd

# we leave only those lines in which the number of views is more than one million
# for filtering we use filter()  narrow transformation
filt_video_titles_rdd=video_titles_rdd.filter(lambda x: int(x[-1])>1000000)
# here we use map narrow transformation to change the order of columns in rdd pairs
# We have to do it since sortByKey sorts by key
# here we use sortByKey() transformation is used to sort RDD elements by number of views
srt_filt_video_titles_rdd=filt_video_titles_rdd.map(lambda x: (x[-1] , x[0])).\
sortByKey(False)

# here we get top 5 by numer of views videos using take
# The action take(n) returns n number of elements from RDD
srt_filt_video_titles_rdd.take(5)


# 6. [3 points] Display the category titles of the first 5 video titles which have more than 1 million views

# completed by: Kleyn IS

# read videos csv
MXvideos_df = spark.read.option("header",True).csv("MXvideos.csv")

# leave only 'title', 'category_id', 'int_views' , 'views' columns
# use withColumn to convert views to int
views_df=MXvideos_df.withColumn('int_views', MXvideos_df.views.cast('int'))['title', 'category_id', 'int_views', 'views']

# leave only records with greater the 1 mln num of views using filter 
f_views_df=views_df.filter(views_df.int_views>1000000)


# sort the dataframe by num of view using sort 
# and desc; 
sorted_by_views_df=f_views_df.sort(f_views_df.int_views.desc())

# apply broadcast variable (from task 1) to get category title by category id
# to apply the broadcast variable, we first convert 
# the dataframe to rdd to aplly actions and transformation susing a property .rdd 

## next we use map. In it, we extract the category_id with the last column and pass it as an argument to a 
## function that performs mapping and allows you to get the category title by its category_id
map_result_rdd =sorted_by_views_df.rdd.map(lambda x: (x[0],x[1],x[2], cat_title_mapper(x[-3]) ) )\
# here we use map to leave only number of views and category titles
.map(lambda x: (int(x[-2]), (x[-1]) ))
# .toDF(cols)

# show top 5 records, sorted by num of view in desc order 

map_result_rdd.take(5)



# 7. [2 points] Read csv file again and extract channel title. 
# For simplicity, you can read it as Spark DataFrame and convert it into rdd after extracting.

# completed by: Kleyn IS

# read videos csv
MXvideos_df = spark.read.option("header",True).csv("MXvideos.csv")
# extract only channel_title 

# convert the dataframe to rdd using rdd 
# convert the dataframe to rdd to aplly actions and transformations
# Since PySpark 1.3, it provides a property .rdd 
# on DataFrame which returns the PySpark RDD class object of DataFrame (converts DataFrame to RDD).

MXvideos_rdd=MXvideos_df.rdd

# extract only views column using map() transformation 

channel_title_rdd=MXvideos_rdd.map(lambda line : line['channel_title'])

# check the output 
# The action take(n) returns n number of elements from RDD
channel_title_rdd.take(5)



# 8. [7 points] What are the top-5 channels by number of videos that contain more videos 
# than the average number of videos in all channels? For instance, a channel will satisfy this constraint 
# if its number of videos is more than the average number of videos in all channels.

# completed by: Kleyn IS

# read videos csv
MXvideos_df = spark.read.option("header",True).csv("MXvideos.csv")
# extract only channel_title 

# convert the dataframe to rdd using rdd 
# convert the dataframe to rdd to aplly actions and transformations
# Since PySpark 1.3, it provides a property .rdd 
# on DataFrame which returns the PySpark RDD class object of DataFrame (converts DataFrame to RDD).

MXvideos_rdd=MXvideos_df.rdd

# here we use map transformation twice
# firstly we are adding a new column with value 1 for each video_id
# then we use map to get channel_title, value pairs
# this output will be used to count number of videos by each channel
MXvideos_rdd_with_v_cnt=MXvideos_rdd.map(lambda line: (line['channel_title'], line['video_id'], 1) ).\
map(lambda elem: (elem[0], elem[-1]) )

# PySpark RDD's reduceByKey(~) method aggregates the RDD data by key, and perform a reduction operation. 

# Grouping by key in pair RDD and performing a reduction operation (count video id by each channel)

num_of_video_by_chann_rdd=MXvideos_rdd_with_v_cnt.reduceByKey(lambda a, b: a+b)

# create udf that returns pairs where the key is a channel and the value is a pair of (num_of_videos, 1)
# To use reduceByKey() for finding averages, we need to find the (sum_of_videos, total_number_of_videos)
def create_combined_pair(record):
    tokens = record
    channel= tokens[0]
    num_of_videos = int(tokens[1])
    return (channel, (num_of_videos, 1))

chann_rdd = num_of_video_by_chann_rdd.map(lambda rec : create_combined_pair(rec))
# Once we’ve created the (channel, (num_of_videos, 1)) pairs we can apply the reduceByKey() transformation to sum up all the num of videos and the 
# number of videos for a given channel. The output of this step will be tuples of (channel, (sum_of_videos, number_of_videos))

sum_and_count = chann_rdd.reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1]))

# extract sum of vieos, num of videos pairs
sum_and_count_pairs=sum_and_count.map(lambda x: x[-1])

# apply map transformation to extract 1 from each pair (sum_of_videos, number_of_videos)
# then apply reduce action to calculate total numer of video
cnt=sum_and_count_pairs.map(lambda t: t[1]).reduce(lambda t1, t2: t1+t2)

# the same we do to calculate the summ of video
summa=sum_and_count_pairs.map(lambda t: int(t[0])).reduce(lambda t1, t2: t1+t2)

# calculate average
avg_num_of_v=float(summa)/cnt

# leave only channels with number of videos more than total avg videos
# use filter twice: to leave channels with number of video nore then avg and after that use filter to exclude 
# records with not defined channel title
# use map to get (number of channel video, channel title) pairs 
# sort in desc order 
# use take method to limit the get top 5 channels with number of video more than avg
num_of_video_by_chann_rdd.filter(lambda x: x[-1]>avg_num_of_v).filter(lambda x: x[0]!=None)\
.map(lambda line: (line[-1], line[0])).sortByKey(False).take(5)


### Spark DataFrame part ###



# 1 [5 points] Build a schema for the dataset in csv file and use the schema to read the dataset.

# completed by: Kleyn IS

# import StructType class to define the structure of the DataFrame
# StructType is a collection or list of StructField objects
# import StructField class to define the columns which include column name(String), 
# column type (DataType), nullable column (Boolean) and metadata (MetaData)
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, BooleanType

# specify the structure using StructType and StructField classes
schema = StructType([ \
    StructField("video_id",StringType(),True), \
    StructField("trending_date",DateType(),True), \
    StructField("title",StringType(),True), \
    StructField("channel_title", StringType(), True), \
    StructField("category_id", IntegerType(), True), \
    StructField("publish_time", TimestampType(), True), \
    StructField("tags", StringType(), True), \
    StructField("views", IntegerType(), True), \
    StructField("likes", IntegerType(), True),
    StructField("dislikes", IntegerType(), True), \
    StructField("comment_count", IntegerType(), True), \
    StructField("thumbnail_link", StringType(), True), \
    StructField("comments_disabled", StringType(), True), \
    StructField("ratings_disabled", StringType(), True), \
    StructField("video_error_or_removed", StringType(), True), \
    StructField("description", StringType(), True)               
  ])

  
# Applying custom schema to data frame
# here we applied the customized schema to that CSV file by changing the names 

MXvideos_df = spark.read.format(
    "csv").schema(schema).option(
    "header", True).load("MXvideos.csv")

# displaying the updated schema of the data frame
MXvideos_df.printSchema()
# display first 5 rows of df
MXvideos_df.show(15)



# 2. [4 points] Calculate the difference in likes and dislikes for videos published 
# before 2010 and show the top-10 videos in descending order.
# avg() function returns the average of values in the input column.

# completed by: Kleyn IS

# import StructType class to define the structure of the DataFrame
# StructType is a collection or list of StructField objects
# import StructField class to define the columns which include column name(String), 
# column type (DataType), nullable column (Boolean) and metadata (MetaData)
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, BooleanType, DateType

# import List of built-in functions available for DataFrame
# # sql functions import
import pyspark.sql.functions as F

# specify the structure using StructType and StructField classes
schema = StructType([ \
    StructField("video_id",StringType(),True), \
    StructField("trending_date",DateType(),True), \
    StructField("title",StringType(),True), \
    StructField("channel_title", StringType(), True), \
    StructField("category_id", IntegerType(), True), \
    StructField("publish_time", TimestampType(), True), \
    StructField("tags", StringType(), True), \
    StructField("views", IntegerType(), True), \
    StructField("likes", IntegerType(), True),
    StructField("dislikes", IntegerType(), True), \
    StructField("comment_count", IntegerType(), True), \
    StructField("thumbnail_link", StringType(), True), \
    StructField("comments_disabled", StringType(), True), \
    StructField("ratings_disabled", StringType(), True), \
    StructField("video_error_or_removed", StringType(), True), \
    StructField("description", StringType(), True)               
  ])

  
# Applying custom schema to data frame
# here we applied the customized schema to that CSV file by changing the names 

MXvideos_df = spark.read.format(
    "csv").schema(schema).option(
    "header", True).load("MXvideos.csv")

# displaying the updated schema of the data frame
MXvideos_df.printSchema()

# use PySpark filter() function is used to filter the rows from DataFrame 
# in this example, we leave only records for those videos that were published earlier than 2018
# then we apply groupBy() to group records based on video title
# Next we use agg to function to find thee sum of likes and dislikes 
# and set aliases 

likes_dislikes_summs=MXvideos_df.filter(MXvideos_df.publish_time.astype('date')<'2018-01-01')\
.groupBy('title').agg(F.sum("likes").alias("like_sum"), \
                      F.sum("dislikes").alias("dislike_sum"))

# here we use  cast() function of Column class to convert sum of likes and dislikes to int
likes_dislikes_summs=likes_dislikes_summs.withColumn('like_sum',likes_dislikes_summs.like_sum.cast('int'))\
.withColumn('dislike_sum',likes_dislikes_summs.dislike_sum.cast('int'))

# here we calculat the difference between likes and dislikes
# and then apply toDF() to set column name for calculated column
diff_df=likes_dislikes_summs.select(likes_dislikes_summs.title,
                                        likes_dislikes_summs.like_sum-likes_dislikes_summs.dislike_sum).\
                                        toDF('title', 'diff')

# here we convert the columnt with difference of likes and dislikes to int 
# and apply sort function to sort 
# to sort in desc order we use desc function of column
# funally we use head function to see only top 10 rows
diff_df.select(diff_df.title, diff_df.diff.cast(IntegerType()))\
                                        .sort(diff_df.diff.desc()).show(10)



# 3. [4 points] Calculate the average likes for each channel and display the top 5 channels by likes

# completed by: Kleyn IS

# import StructType class to define the structure of the DataFrame
# StructType is a collection or list of StructField objects
# import StructField class to define the columns which include column name(String), 
# column type (DataType), nullable column (Boolean) and metadata (MetaData)
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, BooleanType, DateType

# import List of built-in functions available for DataFrame
# # sql functions import
import pyspark.sql.functions as F

# specify the structure using StructType and StructField classes
schema = StructType([ \
    StructField("video_id",StringType(),True), \
    StructField("trending_date",DateType(),True), \
    StructField("title",StringType(),True), \
    StructField("channel_title", StringType(), True), \
    StructField("category_id", IntegerType(), True), \
    StructField("publish_time", TimestampType(), True), \
    StructField("tags", StringType(), True), \
    StructField("views", IntegerType(), True), \
    StructField("likes", IntegerType(), True),
    StructField("dislikes", IntegerType(), True), \
    StructField("comment_count", IntegerType(), True), \
    StructField("thumbnail_link", StringType(), True), \
    StructField("comments_disabled", StringType(), True), \
    StructField("ratings_disabled", StringType(), True), \
    StructField("video_error_or_removed", StringType(), True), \
    StructField("description", StringType(), True)               
  ])

  
# Applying custom schema to data frame
# here we applied the customized schema to that CSV file by changing the names 

MXvideos_df = spark.read.format(
    "csv").schema(schema).option(
    "header", True).load("MXvideos.csv")

# displaying the updated schema of the data frame
MXvideos_df.printSchema()


# here we first extract the columns with the channel title and the column with the number of likes. 
# Next, we change the data type of the column with the number of likes, using the with Column function for this. 
# Next, we use the group By dataframe function to group entries by 
# channel header and use the ag function to calculate the average number of likes for each channel.
# use alias() of Column type to rename a DataFrame column

chann_df=MXvideos_df.select('channel_title', 'likes')\
.withColumn('likes', MXvideos_df.likes.cast(IntegerType())).\
groupBy('channel_title').\
        agg(F.avg("likes").alias("avg_likes_num"))

# here we sort our records using sort method based on the avg value of channel likes
# in order to sort in desc order we use desc function of column
# to get only top 5 records we use head function of df
chann_df.sort(chann_df.avg_likes_num.desc()).head(5)



# 4. [5 points] Calculate the total views for each channel and retrieve 
# the channels which have more than 1 million views in total

# completed by: Kleyn IS

# import StructType class to define the structure of the DataFrame
# StructType is a collection or list of StructField objects
# import StructField class to define the columns which include column name(String), 
# column type (DataType), nullable column (Boolean) and metadata (MetaData)
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, BooleanType, DateType

# import List of built-in functions available for DataFrame
# # sql functions import
import pyspark.sql.functions as F

# specify the structure using StructType and StructField classes
schema = StructType([ \
    StructField("video_id",StringType(),True), \
    StructField("trending_date",DateType(),True), \
    StructField("title",StringType(),True), \
    StructField("channel_title", StringType(), True), \
    StructField("category_id", IntegerType(), True), \
    StructField("publish_time", TimestampType(), True), \
    StructField("tags", StringType(), True), \
    StructField("views", IntegerType(), True), \
    StructField("likes", IntegerType(), True),
    StructField("dislikes", IntegerType(), True), \
    StructField("comment_count", IntegerType(), True), \
    StructField("thumbnail_link", StringType(), True), \
    StructField("comments_disabled", StringType(), True), \
    StructField("ratings_disabled", StringType(), True), \
    StructField("video_error_or_removed", StringType(), True), \
    StructField("description", StringType(), True)               
  ])

  
# Applying custom schema to data frame
# here we applied the customized schema to that CSV file by changing the names 

MXvideos_df = spark.read.format(
    "csv").schema(schema).option(
    "header", True).load("MXvideos.csv")

# displaying the updated schema of the data frame
MXvideos_df.printSchema()

# first, we use the select function to extract the columns we need: the column with the channel title and the column 
# with the number of views. Then we convert the data type of the column with the number of views into an integer 
# so that the total number of views can be counted. Then we use the groupBY and ag functions to calculate the 
# total number of likes for each channel. Then we set an alias for the column with the total number of views using the 
# alias function

total_chann_views=MXvideos_df.select('channel_title', 'views').\
withColumn('views', MXvideos_df.views.cast(IntegerType())).\
groupby("channel_title") \
    .agg(
    F.sum("views").alias("views_summa")
)

# Here, using the where function, we leave only those channels for which the number of views is more 
# than 1 million and use the show() 
# function to see the first 30 entries

total_chann_views.where(total_chann_views.views_summa>1000000).show(30)



# 5. [6 points] Analyse the linear correlation between views and likes for all 
# videos and explain whether the correlation is significant or not

# completed by: Kleyn IS

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, BooleanType, DateType, ArrayType
# from pyspark.sql.functions import split, col, sum, count, avg
import pyspark.sql.functions as F
# specify the structure using StructType and StructField classes
schema = StructType([ \
    StructField("video_id",StringType(),True), \
    StructField("trending_date",DateType(),True), \
    StructField("title",StringType(),True), \
    StructField("channel_title", StringType(), True), \
    StructField("category_id", IntegerType(), True), \
    StructField("publish_time", TimestampType(), True), \
    StructField("tags", StringType(), True), \
    StructField("views", IntegerType(), True), \
    StructField("likes", IntegerType(), True),
    StructField("dislikes", IntegerType(), True), \
    StructField("comment_count", IntegerType(), True), \
    StructField("thumbnail_link", StringType(), True), \
    StructField("comments_disabled", StringType(), True), \
    StructField("ratings_disabled", StringType(), True), \
    StructField("video_error_or_removed", StringType(), True), \
    StructField("description", StringType(), True)               
  ])

  
# Applying custom schema to data frame
# here we applied the customized schema to that CSV file by changing the names 

MXvideos_df2 = spark.read.format(
    "csv").schema(schema).option(
    "header", True).load("MXvideos.csv")

# first, we select the columns we need from the dataframe using the select dataframe function. 
# then we change the data type to int for the views and likes columns so that the sum can be calculated. 
# Then we group by column with the title of the video using group By and calculate the amount of 
# views and likes for each video
stat_df=MXvideos_df2.select('title', 'likes', 'views')\
.withColumn('likes', MXvideos_df2.likes.cast(IntegerType())).\
withColumn('views', MXvideos_df2.views.cast(IntegerType())).\
groupBy('title').\
        agg(F.sum("likes").alias("likes_num"), F.sum("views").alias("views_num"))

# here we filter records with nan values in both views_num and likes_num using isNotNull()
stat_df=stat_df.filter((stat_df.views_num.isNotNull() & stat_df.likes_num.isNotNull()))

# import all from stat module
from pyspark.ml.stat import *

# calculate Pearson correlation coefficient to check  the correlation between the number of views and number of likes
# pySpark DataFrame's corr(~) method returns the correlation of the specified numeric columns as a float
# Here, we see that the number of views and number of likes are positively correlated with a 
# Pearson correlation coefficient of around 0.83.
stat_df.corr("views_num", "likes_num")

! pip install scipy

# use scipy since the is no functions to calc p-value for Pearson correlation significance testing in pyspark.ml.stat
from scipy.stats import pearsonr
import numpy as np

# convert column with num of views to np.array to use in pearsonr() function

views_num=stat_df.select('views_num')

views_num_arr=np.array(views_num.collect()).reshape(-1)

# convert column with num of likes to np.array to use in pearsonr() function

likes_num=stat_df.select('likes_num')

likes_num_arr=np.array(likes_num.collect()).reshape(-1)


# compute Pearson correlation coefficient and p-value for testing non-correlation using pearsonr from scipy.stats
# it return The Pearson product-moment correlation coefficient and p-value associated with the chosen alternative 
# by default alpha is 0.05
r = round(pearsonr(views_num_arr, likes_num_arr)[0], 4)

print(r)


#p-value 
# p = round(pearsonr( likes_num_arr, views_num_arr)[1], 25)
p=pearsonr(views_num_arr, likes_num_arr)[1]

pearsonr(views_num_arr, likes_num_arr)

# sifnificance test for Pearson corr coef
# Let's compare the p-value with alpha=0.05
# if p-value<alpha we should reject the null hypothesis (which is pearson corr is 0) 
# else we should not reject hypothesis
# since the p-value less then alpha we sholud accept the H1 hypotesis (which is pearson corr is not null)
# Hence, the result of correlation is significant
print(f'p-value {p} < alpha {0.05}: ? {p<0.05}')



# 6. [5 points] Calculate the average number of tags for each channel. Return the top-10 channels by number of tags.

# completed by: Kleyn IS

# import StructType class to define the structure of the DataFrame
# StructType is a collection or list of StructField objects
# import StructField class to define the columns which include column name(String), 
# column type (DataType), nullable column (Boolean) and metadata (MetaData)
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, BooleanType, DateType

# import List of built-in functions available for DataFrame
# # sql functions import
import pyspark.sql.functions as F

# specify the structure using StructType and StructField classes
schema = StructType([ \
    StructField("video_id",StringType(),True), \
    StructField("trending_date",DateType(),True), \
    StructField("title",StringType(),True), \
    StructField("channel_title", StringType(), True), \
    StructField("category_id", IntegerType(), True), \
    StructField("publish_time", TimestampType(), True), \
    StructField("tags", StringType(), True), \
    StructField("views", IntegerType(), True), \
    StructField("likes", IntegerType(), True),
    StructField("dislikes", IntegerType(), True), \
    StructField("comment_count", IntegerType(), True), \
    StructField("thumbnail_link", StringType(), True), \
    StructField("comments_disabled", StringType(), True), \
    StructField("ratings_disabled", StringType(), True), \
    StructField("video_error_or_removed", StringType(), True), \
    StructField("description", StringType(), True)               
  ])

  
# Applying custom schema to data frame
# here we applied the customized schema to that CSV file by changing the names 

MXvideos_df = spark.read.format(
    "csv").schema(schema).option(
    "header", True).load("MXvideos.csv")

# displaying the updated schema of the data frame
MXvideos_df.printSchema()

# PySpark SQL function regexp_replace() you can replace a column value with a string for another string/substring
# here we use regexp_replace function to replace double quotes in tags column to an empty string
# we use square brackets to indicate the set of valid characters in a regular expression
tags_df=MXvideos_df.withColumn('tags', F.regexp_replace('tags','[""]',''))

# check the result
tags_df.select('channel_title', 'tags').show(5)

# here we create the dataframe based on previous one (with tags column wihout double quotes)
# here we first use the split function to split the string by the 
# | character and get an array. Then we use the explode function to get each element of the array in a separate line
# we use \ since | is a reserved regexp symbol
# Next we group by channel name and count the number of tags for each (we use explodede column to count tags)
tags_df2=tags_df.select('channel_title',F.explode(F.split(tags_df.tags, '\|')).alias('tags_arr')).\
groupBy('channel_title').agg(F.count("tags_arr").alias("tagsCount"))

# check the intermediate result
tags_df2.show(5)

# here we create dataframe based on the above df with number of tags for each channel
avg_tags_df=tags_df2.groupBy('channel_title').agg(F.avg("tagsCount").alias("avg_tags_num"))

# here we use sort function of dataframe to sort channels in desc order based on avg number of tags
# we use desc method of the Column avg_tags_num to sort in desc
# finally we use show(10) to see top 10 channels by average number of tags
avg_tags_df.sort(avg_tags_df.avg_tags_num.desc()).show(10)



# 7. [5 points] Calculate the frequency of tags in all videos and display the top-10 tags in 
# all videos with its frequency (number of videos).

# completed by: Kleyn IS

# to solve this task, we slightly modify the task in which the average number of tags by channels was calculated
# we will perform grouping not by channel title, but by tag, and for each tag we will count the number of video titles

# import StructType class to define the structure of the DataFrame
# StructType is a collection or list of StructField objects
# import StructField class to define the columns which include column name(String), 
# column type (DataType), nullable column (Boolean) and metadata (MetaData)
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, BooleanType, DateType

# import List of built-in functions available for DataFrame
# # sql functions import
import pyspark.sql.functions as F

# specify the structure using StructType and StructField classes
schema = StructType([ \
    StructField("video_id",StringType(),True), \
    StructField("trending_date",DateType(),True), \
    StructField("title",StringType(),True), \
    StructField("channel_title", StringType(), True), \
    StructField("category_id", IntegerType(), True), \
    StructField("publish_time", TimestampType(), True), \
    StructField("tags", StringType(), True), \
    StructField("views", IntegerType(), True), \
    StructField("likes", IntegerType(), True),
    StructField("dislikes", IntegerType(), True), \
    StructField("comment_count", IntegerType(), True), \
    StructField("thumbnail_link", StringType(), True), \
    StructField("comments_disabled", StringType(), True), \
    StructField("ratings_disabled", StringType(), True), \
    StructField("video_error_or_removed", StringType(), True), \
    StructField("description", StringType(), True)               
  ])

  
# Applying custom schema to data frame
# here we applied the customized schema to that CSV file by changing the names 

MXvideos_df = spark.read.format(
    "csv").schema(schema).option(
    "header", True).load("MXvideos.csv")

# displaying the updated schema of the data frame
MXvideos_df.printSchema()

# PySpark SQL function regexp_replace() you can replace a column value with a string for another string/substring
# here we use regexp_replace function to replace double quotes in tags column to an empty string
# we use square brackets to indicate the set of valid characters in a regular expression
tags_df=MXvideos_df.withColumn('tags', F.regexp_replace('tags','[""]',''))

# here we create the dataframe based on previous one (with tags column wihout double quotes)
# here we first use the split function to split the string by the 
# | character and get an array. Then we use the explode function to get each element of the array in a separate line
# we use \ since | is a reserved regexp symbol

# we will perform grouping not by channel title, but by tag, and for each tag we will count the number of video titles

# # here we create dataframe based on the above df with tags frequency for each video
tags_frequency_df=tags_df.select('title',F.explode(F.split(tags_df.tags, '\|')).alias('tags_exploded_arr')).\
groupBy('tags_exploded_arr').agg(F.count("title").alias("tags_frequency"))

# here we use sort function of dataframe to sort tags in desc order based on its frequency in video
# we use desc method of the Column tags_frequency to sort in desc
# finally we use show(10) to see top 10 tags by the frequency

# this is a case in which none values are allowed in the tags_exploded_arr column
tags_frequency_df.sort(tags_frequency_df.tags_frequency.desc()).show(10)

# # # this is a case in which none values are not allowed in the tags_explode_arr column

# Use regex expression with rlike() to find row with none and then use ~ to inverse the mask to find all records that 
# don't contain [none] in tags_explode_arr col
tags_frequency_df.filter(~tags_frequency_df['tags_arr'].rlike('^\[none\]')).\
sort(tags_frequency_df.tags_frequency.desc()).show(10)

