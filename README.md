# PySpark


# Databricks notebook source
# MAGIC %md
# MAGIC The dbutils.fs.ls() command is used in Databricks to list files and directories at a given location in the Databricks file system (DBFS). Here's how you can use it:

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# To display all the files locations present in dataricks 
dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

# MAGIC %md
# MAGIC # OR

# COMMAND ----------

files = dbutils.fs.ls('/FileStore/tables')

for file in files:
    print(f"Name: {file.name}, Path: {file.path}, Size: {file.size} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) Data Reading

# COMMAND ----------

# spark.read() is a dataframe reader API within PySaprk 
df= spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Display Data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Triggering Actions 
# MAGIC   1 - show()
# MAGIC   2 - display() 

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Another file format

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading JSON file

# COMMAND ----------

df_json = spark.read.format('json').option('inforSchema',True)\
                    .option('header',True)\
                    .option('multiline',False)\
                    .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2) SCHEMA - DDL and StructType()
# MAGIC - A schema is the structure of a table, which defines the column names and their data types.
# MAGIC - if we mention inferSchema=True bydefault it will use StructType schema while creation of Data Frame.
# MAGIC

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC in df Item Weight i want to convert as string by using "**DDL Schema**"

# COMMAND ----------

my_ddl_schema = '''
                Item_Identifier string,
                Item_Weight string,              
                Item_Fat_Content string,
                Item_Visibility double,
                Item_Type string,
                Item_MRP double,
                Outlet_Identifier string,
                Outlet_Establishment_Year integer,
                Outlet_Size string,
                Outlet_Location_Type string,
                Outlet_Type string,
                Item_Outlet_Sales double
                '''

# COMMAND ----------

# instead of using option('inferSchema',True), we can use schema(schema name)
df=spark.read.format('csv')\
    .schema(my_ddl_schema)\
    .option('header',True)\
    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC in df Item Weight i want to convert as string by using "**StructType Schema**"

# COMMAND ----------

# here we want to use 2 functions struct type() and struct field()
from pyspark.sql.types import * 
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([
    StructField("Item_Identifier", StringType(), True),
    StructField("Item_Weight", StringType(), True),
    StructField("Item_Fat_Content", StringType(), True),
    StructField("Item_Visibility", DoubleType(), True),
    StructField("Item_Type", StringType(), True),
    StructField("Item_MRP", DoubleType(), True),
    StructField("Outlet_Identifier", StringType(), True),
    StructField("Outlet_Establishment_Year", IntegerType(), True),
    StructField("Outlet_Size", StringType(), True),
    StructField("Outlet_Location_Type", StringType(), True),
    StructField("Outlet_Type", StringType(), True),
    StructField("Item_Outlet_Sales", DoubleType(), True)
])

# COMMAND ----------

df = spark.read.format('csv')\
    .schema(my_struct_schema)\
    .option('header',True)\
    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3) Data Transformation Techniques
# MAGIC   I) Select Transformation                         
# MAGIC   II) ALIAS                                         
# MAGIC   III) Where/ Filter                                                          
# MAGIC   IV) withColumnRenamed                                                                      
# MAGIC   V) withColumn     
# MAGIC   VI) Type Casting                                                                        
# MAGIC   VII) Sort/ OrderBy                                                             
# MAGIC   VIII) Limit                                                                     
# MAGIC   XI) Drop                                                        
# MAGIC   X) Drop Duplicates                                      
# MAGIC   XI) Union and Union ByName                                                                   
# MAGIC   XII) String Functions                                                 
# MAGIC   XIII) Date Functions                                                      
# MAGIC   XIV) HANDLING NULL VALUES                                       
# MAGIC   XV) SPLIT and INDEXING                                                      
# MAGIC   XVI) Explode          
# MAGIC   XVII) ARRAY_CONTAINS                                                          
# MAGIC   XVIII) Group_BY      
# MAGIC   XIX) Collect_List                   
# MAGIC   XX) PIVOT                            
# MAGIC   XXI) When_OtherWise                  
# MAGIC   XXII) JOINS                    
# MAGIC   XXIII) Window Functions              
# MAGIC   XXIV) User Defined Functions                                                                                      

# COMMAND ----------

# MAGIC %md
# MAGIC - 4 - Data writing with PySpark
# MAGIC - 5 - Data Writing Modes with PySpark
# MAGIC - 6 - Perquet file format
# MAGIC - 7 - Managed vs External Tables in spark 
# MAGIC - 8 - Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1 Select 

# COMMAND ----------

# I)
# selecting only 1st 3 columns
df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()
#or by using col(), both will give same results  "recomended as COL()"
df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2 Alias

# COMMAND ----------

# II)
# using ALIAS 
df.select(col('Item_Identifier').alias('Item_ID')).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.3 Filter/ Where 

# COMMAND ----------

# III)
# filter/ where performing slicing the data 
# Questions 1) filter the data with fat content = Regular
#           2) slice the data with item type = Soft Drinks and weight < 10
#           3) Fetch the data with Tier in (Tier1 or Tier2) and Outlet Size is Null
# 1Q) Ans 
df.select("*").where(col('Item_Fat_Content')=='Regular').limit(5).display()
#OR
df.filter(col('Item_Fat_Content')=='Regular').limit(5).display()

# COMMAND ----------

# III)
# filter/ where performing slicing the data 
# Questions 1) filter the data with fat content = Regular
#           2) slice the data with item type = Soft Drinks and weight < 10
#           3) Fetch the data with Tier in (Tier1 or Tier2) and Outlet Size is Null
# 2Q) Ans 
df.select("*").where((col('Item_Type')=='Soft Drinks') & (col('Item_Weight') < 10)).limit(5).display()
#OR
df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight') < 10)).limit(5).display()

# COMMAND ----------

# III)
# filter/ where performing slicing the data 
# Questions 1) filter the data with fat content = Regular
#           2) slice the data with item type = Soft Drinks and weight < 10
#           3) Fetch the data with Tier in (Tier1 or Tier2) and Outlet Size is Null
# 2Q) Ans 
df.select("*").where((col('Outlet_Location_Type').isin('Tier 1','Tier 2')) & (col('Outlet_Size').isNull())).display()
#OR
df.filter((col('Outlet_Location_Type').isin('Tier 1', 'Tier 2')) & (col('Outlet_Size').isNull())).display()
                     

# COMMAND ----------

df.display()

# COMMAND ----------

df.select(col('Outlet_location_Type')).distinct().display()

# COMMAND ----------

df.filter((col('Outlet_location_Type').isin('Tier 1','Tier 2'))&(col('Outlet_Size').isNull())).display()

# COMMAND ----------

df.filter((col('Outlet_Location_Type')=='Tier 1')&(col('Outlet_Size').isNull())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.4) withColumnRenamed

# COMMAND ----------

# Change Column Name in Data Frame level, and it won't affet Original DataFrame 
df.withColumnRenamed("Item_Weight",'Item_WT').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5) withColumn
# MAGIC Senario 1 : Adding new Column                                  
# MAGIC Senario 2 : Modifying values with an existing column

# COMMAND ----------

# 1st Senario Ex :- 1
# it is used for creating/ adding new column and also mofifying values with an existing table we will use withColumn method
# i want to add new column
df=df.withColumn("flag",lit("New"))    # Lit() is used for storing the constant value in a column

# COMMAND ----------

df.display()

# COMMAND ----------

# senario 1 ex :- 2
# adding a column in that values presnt that by product of 2 existing column
df=df.withColumn("Multiply",col('Item_Weight')*col('Item_MRP'))

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# Senario 2 
# modifying the values in a existing column ex:- Item_Fat_Content Low_Fat as LF and Regular as REG 
df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
    .withColumn("Item_Fat_Content",regexp_replace(col('Item_Fat_Content'),"Low Fat",'LF')).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.6 Type Casting

# COMMAND ----------

# Type casting the data type into another
df= df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# spark.read() is a dataframe reader API within PySaprk 
df= spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')  

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 3.7 Sort/ OrderBy

# COMMAND ----------

# Senario 1 :-
df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# Senario :- 2
df.sort(col("Item_Visibility").asc()).display()

# COMMAND ----------

# Senario :- 3
df.sort(['Item_weight','Item_Visibility'],ascending=[False,False]).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.8 Limit

# COMMAND ----------

df.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.9 Drop

# COMMAND ----------

df.drop(col('Item_Visibility')).display()

# COMMAND ----------

df.drop(col('Item_Visibility'),col('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.10 Drop Duplicates

# COMMAND ----------

# Senario 1 droping all duplicates
df.dropDuplicates().display()

# COMMAND ----------

# Senario 2 droping duplicates single column
df.dropDuplicates(subset=['Item_Type','Item_MRP']).display() 

# COMMAND ----------

# or another way of we can see
df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Intermdeiate Level Transformations 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.11 UNION and UNION BYNAME

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Preparing DataFrames

# COMMAND ----------

data1=[('1','kad'),('2','said')]
schema1="id STRING, name STRING"
df1=spark.createDataFrame(data1,schema1)

data2=[('3','rahul'),('4','jas')]
schema2='id STRING, name STRING'
df2=spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# Applying UNION in both df1 & df2
# UNION
df3=df1.union(df2)
df3.display()

# COMMAND ----------

# UNIONBY Name
data1=[('kad','1'),('said','2')]      # I'm giving df1 as reverse
schema1="name STRING, id STRING"
df1=spark.createDataFrame(data1,schema1)
df1.display()
data2=[('3','rahul'),('4','jas')]
schema2='id STRING, name STRING'
df2=spark.createDataFrame(data2,schema2)
df2.display()

# COMMAND ----------

# now if we perform union, will get wrong output
df1.union(df2).display() 

# COMMAND ----------

# to overcome above wrong outcome will use "UNIONBY Name"
df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.12 String Functions
# MAGIC - INITCAP()
# MAGIC - UPPER()
# MAGIC - LOWER()

# COMMAND ----------

# INITCAP()
df.select(initcap(col('Item_Type'))).display()

# COMMAND ----------

# LOWER()
df.select(lower(col('Item_Type'))).display()

# COMMAND ----------

#UPPER()
df.select(upper('Item_Type').alias('Upper_Item_Type')).limit(8).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 3.13 Date Functions
# MAGIC - CURRENT_DATE()
# MAGIC - DATE_ADD()
# MAGIC - DATE_SUB()
# MAGIC - DATEDIFF()
# MAGIC - DATE_FORMAT()

# COMMAND ----------

# CURRENT_DATE()
df=df.withColumn('Curr_Date',current_date())

# COMMAND ----------

df.limit(3).display()

# COMMAND ----------

# Date Add()
df=df.withColumn("Week_After",date_add('Curr_Date',7))
df.limit(3).display()

# COMMAND ----------

# date Sub()
df.withColumn("Week_before",date_sub('Curr_Date',7)).display()

# COMMAND ----------

# DATEDIFF()
df.withColumn("day's_diff",datediff('Week_After','Curr_Date')).limit(3).display()


# COMMAND ----------

# Date_Format() for changing the format of date we will use this
df=df.withColumn('Week_After',date_format('Week_After','dd-MM-yyyy'))
df.limit(3).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 3.14 Handling Null Values

# COMMAND ----------

df.limit(3).display()

# COMMAND ----------

# drop/ fill null values
# Droping null in data frame level 
# df.filter(col('Outlet_Size').isNull()).display()
df.dropna('all').display()  # all :- it only drop the row when same row all columns present as null values then only it will drop the record 
# the output say's that none of the row is present with null values values in same row with all columns  that's the reason we can notice the count is same for this cell compare to previous ones 

# COMMAND ----------

df.dropna('any').display()   # it will work as if any cell null value present the entire row will be delated so that we can notice half of the records are delated

# NOte there is a chance that we can loss the DATA to overcome this issue we will use SUBSET Option

# COMMAND ----------

# Subset
df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# Filling the Null values there is a drawback it will replace all columns to overcome this we will use SUBSET
df.fillna('Not_Available').display()

# COMMAND ----------

df.fillna("Fill_0",subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.15 Split and Indexing

# COMMAND ----------

# Split
from pyspark.sql.functions import split
df.withColumn('Outlet_Type',split('Outlet_Type',' ')).limit(7).display()

# COMMAND ----------

# Indexing
df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).limit(7).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.16 EXPLODE
# MAGIC - this will convert the single column list values into separate each value in a  new columns 

# COMMAND ----------

# creating new Data Frame
df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_exp.limit(3).display()

# COMMAND ----------

# now performing Explode operation on new data frame
from pyspark.sql.functions import split, explode
df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.17 ARRAY_CONTAINS

# COMMAND ----------

# i want to know type1 is available in perticular list
from pyspark.sql.functions import array_contains
df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.18 GroupBY

# COMMAND ----------

# Senario 1 :- total MRP for all the Item Type
df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

# Senario 2 :- find the average MRP for all the Item Type
df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

# Senario 3 :- want to find  the sum of Item_Type & Outlet_Size and find total MRP for all
df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

# Senario 4 :- 
df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP'),avg('Item_MRP').alias('AVG_MRP')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.19 Collect_List

# COMMAND ----------

# this is a replacement/ Alternative function of group concat available in a MySQL
data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema="user string, book string" 
df_book = spark.createDataFrame(data,schema)
df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.20 PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.21 WHEN-OTHERWISE

# COMMAND ----------

# Senario 1
from pyspark.sql.functions import col, when
df.withColumn('Veg_flag',when(col('Item_Type')=='Meat','Non_Veg').otherwise('Veg')).display()

# COMMAND ----------

df = df.withColumn('Veg_flag',when(col('Item_Type')=='Meat','Non_Veg').otherwise('Veg'))

# COMMAND ----------

df.display()

# COMMAND ----------

# Senario 2 :
df.withColumn('veg_exp_flag',when(((col('veg_flag')=='Veg')&(col('Item_MRP')<100)),'Veg_InExcepnsive')\
    .when((col('veg_flag')=='Veg')&(col('Item_MRP')>100),'Veg_Expensive')\
        .otherwise('Non_Veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.22 JOINS
# MAGIC - Inner Join
# MAGIC - Left Join
# MAGIC - Right Join
# MAGIC - Full Join
# MAGIC - Anti Join

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Inner Join

# COMMAND ----------

dataj1=[('1','garu','d01'),
        ('2','kit','d02'),
        ('3','sam','d03'),
        ('4','tim','d03'),
        ('5','aman','d05'),
        ('6','nad','d06')]

schemaj1='emp_id STRING, emp_name STRING, dept_id STRING'

df1=spark.createDataFrame(dataj1,schemaj1)

dataj2=[('d01','HR'),
        ('d02','Marketing'),
        ('d03','Accounts'),
        ('d04','IT'),
        ('d05','Finance')]

schemaj2='dept_id STRING, dept_name STRING'
df2=spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# Inner join
df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Left Join

# COMMAND ----------

# Left join
df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Right Join

# COMMAND ----------

# Right Join
df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Anti Join

# COMMAND ----------

# Anti join
df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# Full Outer join
df1.join(df2,df1['dept_id']==df2['dept_id'],'full').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.23 Windows Functions
# MAGIC - Row Number()
# MAGIC - Rank()
# MAGIC - Dence_Rank()
# MAGIC - Cumulative Sum

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# Row Number()
df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# Rank
df.withColumn('rank',rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# Rank
# Same Senario with descending order
df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# Dence Rank
df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .withColumn('denceRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# Cumulative Sum
df.withColumn('CUMSUM',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

# Correct Way of doing Cumulative SUM
# Cumulative Sum
df.withColumn('CUMSUM',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# Total Sum
df.withColumn('TotalSUM',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.24 User Defined Functions

# COMMAND ----------

def my_func(x):
    return x*x
op=my_func(4)
print(op)

# COMMAND ----------

# Pyspark
my_udf=udf(my_func)
df.withColumn('mynewUDF',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4) Data Writing
# MAGIC

# COMMAND ----------


