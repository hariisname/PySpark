# Databricks notebook source
# MAGIC %md
# MAGIC # I) Data Reading                                                                  
# MAGIC PySpark supports various data sources like CSV, JSON, Parquet, etc. Here’s how to read data:

# COMMAND ----------

# To get all the files present in DBSF default databricks storage
files = dbutils.fs.ls('/FileStore/tables')
for file in files:
    print(f"Name: {file.name}, Path: {file.path}, Size: {file.size} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing CSV File

# COMMAND ----------

# Importing CSV File
df=spark.read.format('csv').option('inforSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')
df.limit(5).display() # Action

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing JSON File

# COMMAND ----------

# Reading JSON File
df_json = spark.read.format('json').option('inferSchema',True)\
    .option('header',True)\
    .option('multline',False).load('/FileStore/tables/drivers.json')  # single line Json file format
df_json.limit(5).display()    

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema - DDL & StructType()
# MAGIC A schema is nothing but the structure of a table, including column names, their data types, and whether each column can accept null values. This can be defined in two ways:
# MAGIC
# MAGIC - Using DDL (Data Definition Language).
# MAGIC - Using the StructType class.

# COMMAND ----------

# df.printSchema() is a PySpark method that prints the schema of a DataFrame, showing the column names, data types, and nullability in a tree-like format.
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DDL Schema 

# COMMAND ----------

# here I'm modifying Item Weight String into Double
my_ddl_schema = '''
    Item_Identifier STRING,
    Item_Weight DOUBLE,  -- Modified from STRING to DOUBLE
    Item_Fat_Content STRING,
    Item_Visibility DOUBLE,
    Item_Type STRING,
    Item_MRP DOUBLE,
    Outlet_Identifier STRING,
    Outlet_Establishment_Year INT,
    Outlet_Size STRING,
    Outlet_Location_Type STRING,
    Outlet_Type STRING,
    Item_Outlet_Sales DOUBLE
'''

# Here I'm using schema() option insterd inferSchem() 
df=spark.read.format('csv')\
    .schema(my_ddl_schema)\
    .option('header',True)\
    .load('/FileStore/tables/BigMart_Sales.csv')

df.printSchema()
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### structType() Schema
# MAGIC
# MAGIC - StructType: It is a collection of StructField objects, representing the overall schema of a DataFrame.
# MAGIC - StructField: It defines an individual column in a schema, specifying the column's name, data type, and whether it can be null.
# MAGIC

# COMMAND ----------

# from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([
    StructField("Item_Identifier", StringType(), True),
    StructField("Item_Weight", StringType(), True),  # You can later cast to DoubleType
    StructField("Item_Fat_Content", StringType(), True),
    StructField("Item_Visibility", StringType(), True),
    StructField("Item_Type", StringType(), True),
    StructField("Item_MRP", StringType(), True),
    StructField("Outlet_Identifier", StringType(), True),
    StructField("Outlet_Establishment_Year", StringType(), True),
    StructField("Outlet_Size", StringType(), True),
    StructField("Outlet_Location_Type", StringType(), True),
    StructField("Outlet_Type", StringType(), True),
    StructField("Item_Outlet_Sales", StringType(), True)
])

df = spark.read.format('csv')\
    .schema(my_struct_schema)\
    .option('header',True)\
    .load('/FileStore/tables/BigMart_Sales.csv')

df.printSchema()
df.limit(5).display()

# COMMAND ----------

# here I'm modifying Item Weight String into Double
my_ddl_schema = '''
    Item_Identifier STRING,
    Item_Weight DOUBLE,  -- Modified from STRING to DOUBLE
    Item_Fat_Content STRING,
    Item_Visibility DOUBLE,
    Item_Type STRING,
    Item_MRP DOUBLE,
    Outlet_Identifier STRING,
    Outlet_Establishment_Year INT,
    Outlet_Size STRING,
    Outlet_Location_Type STRING,
    Outlet_Type STRING,
    Item_Outlet_Sales DOUBLE
'''

# Here I'm using schema() option insterd inferSchem() 
df=spark.read.format('csv')\
    .schema(my_ddl_schema)\
    .option('header',True)\
    .load('/FileStore/tables/BigMart_Sales.csv')

df.printSchema()
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # II) Data Transform & Techniques                                                              
# MAGIC PySpark transformations are used to process and transform/ modification/ manipulate the data.                                  
# MAGIC
# MAGIC - Select                
# MAGIC - Alias                 
# MAGIC - Filter/ Where                                 
# MAGIC - withColumnRenamed                              
# MAGIC - withColumn                                   
# MAGIC - Type Casting                                                            
# MAGIC - Sort/ OrderBY                                  
# MAGIC - Limit                                                              
# MAGIC - Drop                                                  
# MAGIC - Drop Duplicates                                           
# MAGIC - Union and Union ByName                                                  
# MAGIC - Sting Functions                                                     
# MAGIC - Date Functions                                                                              
# MAGIC - Handling NULL                                                                 
# MAGIC - Split and Indexing                                                             
# MAGIC - Explode                                                     
# MAGIC - Group BY                                                               
# MAGIC - Collect LIST                                                                        
# MAGIC - PIVOT                                                      
# MAGIC - When Otherwise                                         
# MAGIC - Joins                                                                  
# MAGIC - Window Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Select 

# COMMAND ----------

# Use string-based column names for simplicity when selecting columns directly.
df.select('Item_Identifier','Item_Weight','Item_Fat_Content').limit(5).display()

# Use col() when you need additional transformations, aliases, or dynamic column handling.
df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID'),col('Item_Weight').alias('I_Weight')).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Filter/ Where
# MAGIC Both filter() and where() in PySpark serve the same purpose: to filter rows based on a condition. They are functionally equivalent, so we can use either.                                         
# MAGIC
# MAGIC Senarios :-                                         
# MAGIC ----------------                                   
# MAGIC I) Filter the data with fat content = Regular                              
# MAGIC II) Slice the data with item type = SOft Drinks and weight < 10                              
# MAGIC III) Fetch the data with Tier in(Tier 1 or Tier 2) and Outlet Size is Null
# MAGIC

# COMMAND ----------

# I) Filter the data with fat content = Regular
# by using Filter()
df.select('Item_Fat_Content').filter(col('Item_Fat_Content') == 'Regular').limit(6).display()
# by using Where()
df.select('Item_Fat_Content').where(col('Item_Fat_Content') == 'Regular').limit(6).display()

# COMMAND ----------

# II) Slice the data with item type = SOft Drinks and weight < 10
df.filter((col('Item_Type')=='Soft Drinks')&(col('Item_Weight')<10)).limit(4).display()
# OR
df.select('Item_Type','Item_Weight').filter((col('Item_Type')=='Soft Drinks')&(col('Item_Weight')<10)).limit(5).display()

# COMMAND ----------

# III) Fetch the data with Tier in(Tier 1 or Tier 2) and Outlet Size is Null
#df.select('Outlet_Location_Type','Outlet_Size').limit(5).display()
df.select('Outlet_Location_Type','Outlet_Size')\
    .filter((col('Outlet_Location_Type').isin(['Tier 1','Tier 2'])) & (col('Outlet_size').isNull())).limit(6).display()

# OR
df.filter((col('Outlet_Location_Type').isin(['Tier 1','Tier 2'])) & (col('Outlet_size').isNull())).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) WithColumnRenamed
# MAGIC withColumnRenamed(): Permanent renaming of a column in the DataFrame level.                 
# MAGIC alias(): Temporary renaming used in specific operations.

# COMMAND ----------

# Renaming column Item_Fat_Content into Item_Fat_Type
df.withColumnRenamed('Item_Fat_Content','Item_Fat_Type').limit(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) withColumn
# MAGIC Will use withColumn() when you need to add a new column or modify an existing column by applying transformations, operations, or conditional logic.

# COMMAND ----------

# senario 1 :- adding a new column with a constant value by usin lit()
# The lit() function is used to represent a constant value as a Column, which can then be added to your DataFrame.
df = df.withColumn('flag',lit('new'))  
df.limit(5).display()

# COMMAND ----------

# senario 2 :- adding a new column with some transformation wothan existing column
df.withColumn('Item_Outlet_Sales^2',col('Item_Outlet_Sales')*2).limit(5).display()

# COMMAND ----------

# senario 3 :- adding a new that product of 2 columns
df.withColumn('weight*mrp',col('Item_Weight')*col('Item_MRP')).limit(5).display()

# COMMAND ----------

# Senario 4 :- replacing values in existing column ex :- Low Fat into LF and Regular into Regby using regexp_replace()
# regexp_replace() is a powerful function for performing regular expression-based replacements in a column, enabling data cleaning and transformations using patterns.
df.withColumn('Item_Fat_Content', 
                  regexp_replace(col('Item_Fat_Content'), r"Low Fat", "LF")) \
       .withColumn('Item_Fat_Content', 
                  regexp_replace(col('Item_Fat_Content'), r"Regular", "Reg")).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6) Type Casting
# MAGIC Type casting is nothing but converting one datatype into another respective datatype of required column

# COMMAND ----------

# Converting Item_Weight from Double to String with help of STringType() function data type.
df.printSchema()

df=df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))
df.limit(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7) Sort/ OrderBY

# COMMAND ----------

# Scenario 1 :- sorting item_Weight column in Descending Order
df.sort(col('Item_Weight').desc()).limit(5).display() 

# Scenario 2 :- Item_Visibility ASC Order
df.sort(col('Item_Visibility').asc()).limit(5).display() 

# Soring multiple columns
# Scenario 3 :- both columns in ascending order
df.sort(['Item_Weight','Item_Visibility'],ascending=[0,0]).limit(5).display()

# Scenario 4 :- one column in desc and one is asc order
df.sort(['Item_Weight','Item_Visibility'],ascending=[0,1]).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8) Limit

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 9) Drop

# COMMAND ----------

# droping single required column
df.drop('Item_Visibility').limit(3).display()

# droping multiple required columns
df.drop('Item_Visibility','Item_Type').limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 10) Drop Duplicated

# COMMAND ----------

# If we use df.dropDuplicates().display(), and a row has the same values as another row across all columns (i.e., it is duplicated), then one of the rows will be deleted, and the other will be retained.
df.dropDuplicates().display()
# based on one column that will remove entire row
df.drop_duplicates(subset=['Item_Type']).display()
# or we can use Distinct
df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 11) Union & Union ByName

# COMMAND ----------

# Creating example data frames
data1=[('1','kad'),('2','said')]
schema1="id STRING, name STRING"
df1=spark.createDataFrame(data1,schema1)
data2=[('3','rahul'),('4','jas')]
schema2='id STRING, name STRING'
df2=spark.createDataFrame(data2,schema2)

df1.display()
df2.display()

# COMMAND ----------

# Union
df3=df1.union(df2)
df3.display()

# COMMAND ----------

# UNIONBY Name
data1=[('kad','1'),('said','2')] # I'm giving df1 as reverse
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
df1.unionByName(df2).display() # it will map the columns based on column names


# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 12) String Functions in PySpark
# MAGIC
# MAGIC ### 1. **INITCAP()**
# MAGIC Converts the first letter of each word to uppercase and the rest to lowercase.  
# MAGIC Example: `df.select(initcap(col('Item_Type'))).display()`  
# MAGIC Input: `"hello world"` → Output: `"Hello World"`
# MAGIC
# MAGIC ### 2. **UPPER()**
# MAGIC Converts all characters in a string to uppercase.  
# MAGIC Example: `df.select(upper('Item_Type').alias('Upper_Item_Type')).limit(8).display()`  
# MAGIC Input: `"hello world"` → Output: `"HELLO WORLD"`
# MAGIC
# MAGIC ### 3. **LOWER()**
# MAGIC Converts all characters in a string to lowercase.  
# MAGIC Example: `df.select(lower(col('Item_Type'))).display()`  
# MAGIC Input: `"HELLO WORLD"` → Output: `"hello world"`
# MAGIC
# MAGIC ### 4. **CONCAT()**
# MAGIC Concatenates two or more strings.  
# MAGIC Example: `df.select(concat(col('col1'), col('col2'))).display()`  
# MAGIC Input: `"Hello"`, `"World"` → Output: `"HelloWorld"`
# MAGIC
# MAGIC ### 5. **SUBSTRING()**
# MAGIC Extracts a substring from a string based on specified start and length.  
# MAGIC Example: `df.select(substring(col('col1'), 1, 5)).display()`  
# MAGIC Input: `"HelloWorld"` → Output: `"Hello"`
# MAGIC
# MAGIC ### 6. **LENGTH()**
# MAGIC Returns the length of a string.  
# MAGIC Example: `df.select(length(col('col1'))).display()`  
# MAGIC Input: `"Hello"` → Output: `5`
# MAGIC
# MAGIC ### 7. **TRIM(), LTRIM(), RTRIM()**
# MAGIC Removes spaces from strings:  
# MAGIC - `trim()`: Removes spaces from both ends.  
# MAGIC - `ltrim()`: Removes spaces from the left.  
# MAGIC - `rtrim()`: Removes spaces from the right.  
# MAGIC Example: `df.select(trim(col('col1'))).display()`  
# MAGIC Input: `" Hello "` → Output: `"Hello"`
# MAGIC
# MAGIC ### 8. **REVERSE()**
# MAGIC Reverses the characters in a string.  
# MAGIC Example: `df.select(reverse(col('col1'))).display()`  
# MAGIC Input: `"Hello"` → Output: `"olleH"`
# MAGIC
# MAGIC ### 9. **TRANSLATE()**
# MAGIC Replaces specific characters in a string with other characters.  
# MAGIC Example: `df.select(translate(col('col1'), 'aeiou', '12345')).display()`  
# MAGIC Input: `"Hello"` → Output: `"H2ll4"`
# MAGIC
# MAGIC ### 10. **REGEXP_REPLACE()**
# MAGIC Replaces substrings matching a regex pattern.  
# MAGIC Example: `df.select(regexp_replace(col('col1'), '[0-9]', '')).display()`  
# MAGIC Input: `"abc123"` → Output: `"abc"`
# MAGIC
# MAGIC ### 11. **REGEXP_EXTRACT()**
# MAGIC Extracts a substring matching a regex pattern.  
# MAGIC Example: `df.select(regexp_extract(col('col1'), '([0-9]+)', 1)).display()`  
# MAGIC Input: `"abc123"` → Output: `"123"`
# MAGIC
# MAGIC ### 12. **SPLIT()**
# MAGIC Splits a string into an array based on a delimiter.  
# MAGIC Example: `df.select(split(col('col1'), ',')).display()`  
# MAGIC Input: `"a,b,c"` → Output: `["a", "b", "c"]`
# MAGIC
# MAGIC ### 13. **FORMAT_NUMBER()**
# MAGIC Formats a number as a string with a specified number of decimal places.  
# MAGIC Example: `df.select(format_number(col('col1'), 2)).display()`  
# MAGIC Input: `1234.5678` → Output: `"1234.57"`
# MAGIC
# MAGIC ### 14. **ASCII()**
# MAGIC Returns the ASCII value of the first character in a string.  
# MAGIC Example: `df.select(ascii(col('col1'))).display()`  
# MAGIC Input: `"A"` → Output: `65`
# MAGIC
# MAGIC ### 15. **CONTAINS()**
# MAGIC Checks if a string contains a specific substring.  
# MAGIC Example: `df.filter(col('col1').contains('Hello')).display()`  
# MAGIC Input: `"Hello World"` → Output: `True`
# MAGIC
# MAGIC ### 16. **STARTSWITH()**
# MAGIC Checks if a string starts with a specific substring.  
# MAGIC Example: `df.filter(col('col1').startswith('Hel')).display()`  
# MAGIC Input: `"Hello World"` → Output: `True`
# MAGIC
# MAGIC ### 17. **ENDSWITH()**
# MAGIC Checks if a string ends with a specific substring.  
# MAGIC Example: `df.filter(col('col1').endswith('ld')).display()`  
# MAGIC Input: `"Hello World"` → Output: `True`
# MAGIC
# MAGIC ### 18. **REPEAT()**
# MAGIC Repeats a string a specified number of times.  
# MAGIC Example: `df.select(repeat(col('col1'), 3)).display()`  
# MAGIC Input: `"Hi"` → Output: `"HiHiHi"`
# MAGIC
# MAGIC ### 19. **LOCATE()**
# MAGIC Returns the position of the first occurrence of a substring.  
# MAGIC Example: `df.select(locate('o', col('col1'))).display()`  
# MAGIC Input: `"Hello World"` → Output: `5`
# MAGIC
# MAGIC ### 20. **LPAD() and RPAD()**
# MAGIC - **LPAD()**: Pads a string from the left with a specified character to a specified length.  
# MAGIC - **RPAD()**: Pads a string from the right with a specified character to a specified length.  
# MAGIC Example: `df.select(lpad(col('col1'), 10, '*')).display()`  
# MAGIC Input: `"Hi"` → Output: `"********Hi"`
# MAGIC
# MAGIC ### 21. **OVERLAY()**
# MAGIC Replaces a substring at a specific position with another substring.  
# MAGIC Example: `df.select(overlay(col('col1'), 'New', 1, 3)).display()`  
# MAGIC Input: `"Hello"` → Output: `"Newlo"`
# MAGIC
# MAGIC ### 22. **HEX() and UNHEX()**
# MAGIC - **HEX()**: Converts a string or number to its hexadecimal representation.  
# MAGIC - **UNHEX()**: Converts a hexadecimal string back to binary.  
# MAGIC Example: `df.select(hex(col('col1'))).display()`  
# MAGIC Input: `255` → Output: `"ff"`
# MAGIC
# MAGIC ### 23. **BASE64() and UNBASE64()**
# MAGIC - **BASE64()**: Encodes a string to Base64 format.  
# MAGIC - **UNBASE64()**: Decodes a Base64 string.  
# MAGIC Example: `df.select(base64(col('col1'))).display()`  
# MAGIC Input: `"Hello"` → Output: `"SGVsbG8="`
# MAGIC
# MAGIC ### 24. **SOUNDEX()**
# MAGIC Returns the Soundex code for a string (used for phonetic comparison).  
# MAGIC Example: `df.select(soundex(col('col1'))).display()`  
# MAGIC Input: `"Smith"` → Output: `"S530"`
# MAGIC
# MAGIC ### 25. **LEVENSHTEIN()**
# MAGIC Calculates the Levenshtein distance (edit distance) between two strings.  
# MAGIC Example: `df.select(levenshtein(col('col1'), col('col2'))).display()`  
# MAGIC Input: `"kitten"`, `"sitting"` → Output: `3`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 13) Date Functions
# MAGIC - CURRENT_DATE()                                           
# MAGIC - DATE_ADD()
# MAGIC - DATE_SUB()                                                        
# MAGIC - DATEDIFF()                                            
# MAGIC - DATE_FORMAT()
# MAGIC

# COMMAND ----------

# Current_Date()
df = df.withColumn('curr_date',current_date())
df.limit(5).display()

# date_add() :- adding new date as 1 week after
df=df.withColumn('week_after',date_add('curr_date',7))
df.limit(3).display()

# date_sub()
df=df.withColumn('week_before',date_sub('curr_date',10))  # df.withColumn('week_before',date_add('curr_date',-10))
df.limit(3).display()

# date_diff()
df=df.withColumn('difference',datediff('week_after','curr_date'))  # best practice use week after 1st & curr date 2nd
df.limit(3).display()

# date_format()
# modifying the week after column
df = df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))
df.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 14) Handling NULL
# MAGIC - Dropping Null's (any, all and subset)                                                              
# MAGIC - Handling Null's

# COMMAND ----------

#dropping nulls
df.dropna('all').display() # any :- any one cell drop entire row or all :- in a row all the cells null then only drop the row

df.dropna('any').display()

df.dropna(subset=['Outlet_Size']).display() # we are checking only in once column based on that the entire row delated

# COMMAND ----------

# Handling/ Filling Null values
df.fillna('NotAvailable').display()  # full dataframe filled with notAvailable value
df.fillna('NotAvailable',subset=['Outlet_Size']).display() # only respective column in a  dataframe filled with notAvailable value

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 15) Split and Indexing

# COMMAND ----------

# Splitting
df.withColumn('Outlet_Type',split('Outlet_Type'," ")).limit(4).display()

#Indexing
df.withColumn('Outlet_Type',split('Outlet_Type'," ")[1]).limit(4).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 16) Explode

# COMMAND ----------

# to convert a row into separate columns we will use EXPLODE
df_exp = df.withColumn('Outlet_Type',split('Outlet_Type'," "))
df_exp.display()

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 17) Array Contains

# COMMAND ----------

df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).limit(15).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 18) Group_BY

# COMMAND ----------

# senario 1 :- sum of mrp for each item type
df.groupBy('Item_Type').agg(sum('Item_MRP')).limit(3).display()

# senario 2 :- avg of mrp for each item type
df.groupBy('Item_Type').agg(avg('Item_MRP')).limit(3).display()

# senario 3 :- sum of item type and outlet size of each item type
df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).limit(5).display()

#senario 4 :- 
df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP'),avg('Item_MRP').alias('Average_MRP')).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 19) Collect LIST

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

df_book.groupBy('user').agg(collect_list('book')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 20) PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 21) When Otherwise

# COMMAND ----------

# senario 1 :-  Item type veg or non veg
df = df.withColumn('Veg_Flag',when(col('Item_Type')=='Meat','Non_Veg').otherwise('Veg'))
df.limit(6).display()

# senario 2 :- Item type is veg and item mrp < 100 then flag as expencive othervise in-expensive
df.withColumn('veg_exp_flag',when(((col('Veg_Flag')=='Veg') & (col("Item_MRP")<100)),'Veg_Inexpensive')\
                            .when((col('Veg_Flag')=='Veg') & (col("Item_MRP")>100),'Veg_Expensive')\
                            .otherwise('Non_Veg')).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 22) JOINS
# MAGIC - Inner Join                                                                              
# MAGIC - Left Join                                                             
# MAGIC - Right Join                                                               
# MAGIC - Full Join                                                                   
# MAGIC - Anti Join                                              

# COMMAND ----------

# Creting Data Frames
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

df1.display()
df2.display()

# COMMAND ----------

# Inner join
print('Inner Join')
df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()
print('--------------------------------------------------------------')
# Left join
print('Left Join')
df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()
print('--------------------------------------------------------------')
# Right Join
print('Right Join')
df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()
print('--------------------------------------------------------------')
# Full Outer join
print('Full Outer Join')
df1.join(df2,df1['dept_id']==df2['dept_id'],'full').display()
print('--------------------------------------------------------------')
# Anti join
print('Anti Join')
df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()
print('--------------------------------------------------------------')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 23) Window Functions                                                 
# MAGIC - Row Number() :- not a ranking function                                                                 
# MAGIC - Rank()       :- ranking function                                
# MAGIC - Dence Rank() :- ranking function                                                         
# MAGIC - Cumulative Sum
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# if we want to assign a unique number to each row then we will use row number function & it is not a ranking function
df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# Ranking functions
# Rank() scenario 1 :- 
df.withColumn('Rank',rank().over(Window.orderBy('Item_Identifier'))).display() # this is column ASC ordre

# Rank() Scenario 2 :- 
df.withColumn('Rank',rank().over(Window.orderBy(col('Item_Identifier').desc()))).display() # this is column desc 

# COMMAND ----------

# displaying 2 columns same time by using withColumn()
# dence Rank() Scenario 1 :- 
df.withColumn('Rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .withColumn('Dense Rank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cumulative Sum

# COMMAND ----------

# sum(), over() and frame clause() 
df.withColumn('CUMSUM',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# Correct Way of doing Cumulative SUM
# Cumulative SUM
df.withColumn('CUMSUM',sum('Item_MRP').over(Window.orderBy('Item_Type')\
    .rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()           # with frame clause()

# Total Sum
df.withColumn('TotalSUM',sum('Item_MRP').over(Window.orderBy('Item_Type')\
    .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 24) UDF (User Defined Function)

# COMMAND ----------

def my_func(x):
    return x*x
op=my_func(4)
print(op)

my_udf=udf(my_func)
df.withColumn('mynewUDF',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # III) Data Writing                                               
# MAGIC After processing or transforming the data, we can write or store it back in various formats to a storage layer, such as ADLS Gen2, S3, or GCS Data Lakes.
# MAGIC
# MAGIC - **NoSQL Databases**: Storage solutions like MongoDB, Cassandra, or DynamoDB, suitable for semi-structured or unstructured data.
# MAGIC - **Data Warehouses**: Platforms like Snowflake, BigQuery, Redshift, or Azure Synapse Analytics, optimized for analytical workloads.
# MAGIC
# MAGIC **Supported Formats**:  
# MAGIC - CSV  
# MAGIC - JSON  
# MAGIC - Parquet  
# MAGIC - ORC  
# MAGIC - Avro  
# MAGIC - Delta Lake  
# MAGIC - XML  
# MAGIC - Plain Text  
# MAGIC
# MAGIC **Data Writing Modes**:  
# MAGIC - **Append**: Add new data to existing files.  
# MAGIC - **Overwrite**: Replace existing data with new data.  
# MAGIC - **Error**: Throw an error if data already exists.  
# MAGIC - **Ignore**: Skip writing if data already exists.  
# MAGIC

# COMMAND ----------

# CSV
df.write.format('csv').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# Data writing with MODE Option
df.write.format('csv')\
    .mode('append')\
    .save('/FileStore/tables/CSV/data.csv')


# Another Way
df.write.format('csv')\
    .mode('append')\        # in this place we can write diff eriting options 
    .option('path','/FileStore/tables/CSV/data.csv')\
    .save()

# Parquet File Format
df.write.format('parquet')\
    .mode('overwride')\        # in this place we can write diff eriting options 
    .option('path','/FileStore/tables/CSV/data.csv')\
    .save()


# COMMAND ----------

# Instead of saving in another format, we can save this in a TABLE
df.write.format('parquet') \
    .mode('overwrite') \
    .saveAsTable('my_table')  # This saves the DataFrame as a table

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark SQL

# COMMAND ----------

# converting saprk object into sql object
# creating temp view (temporary view)
df.createTempView('sql_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sql_df
# MAGIC where Item_Fat_Content='Low Fat';

# COMMAND ----------

# After Performing transformstion now we wantr to writing the data how - just convert this temp view into Data Frame
df_transform = spark.sql('select * from sql_df')

# COMMAND ----------

df_transform.limit(5).display()

# COMMAND ----------


