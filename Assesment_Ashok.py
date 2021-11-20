#Databricks notebook source
#importing libraries
from pyspark.sql.functions import concat,col,lit,when

#Reading email data 
email_data_df = spark.read.option("header",True).option("inferSchema",True).csv('/FileStore/tables/EmailData_1_.csv')

#Reading website visiter data 
webiste_visitor_df = spark.read.option("header",True).option("inferSchema",True).csv('/FileStore/tables/webiste_visitor_data_1_.csv')


#1.Renaming Email to MailID and displaying the dataframe
#----------------------------------------------------------------------------------------------------------------------------------------------------
df1 = webiste_visitor_df.withColumnRenamed("email","MailID") 
df1.show(df1.count(),truncate = False)


#2.Full name with prefix(Mr./Mrs.)
#--------------------------------------------------------------------------------------------------------------------------------------------------
df2 = webiste_visitor_df.withColumn("full_name",concat(col('first_name'),lit(' '),col('last_name'))).\
                         withColumn("full_name",when(col('gender')=='Male',concat(lit('Mr.'),col('full_name'))).\
                                                when(col('gender')=='Female',concat(lit('Mrs.'),col('full_name'))).\
                                                otherwise(col('full_name'))) # concating first and last name using concat() and prefixing Mr./Mrs. based on the                                                                                #gender using when()
df2.show(df2.count(),truncate=False)


#3.Display records have inappropriate values for Gender
#--------------------------------------------------------------------------------------------------------------------------------------------------
df3 = webiste_visitor_df.filter((col('gender')!='Male') & (col('gender')!='Female'))
df3.show(df3.count(),truncate=False)
print("total number of records that have inappropriate values for Gender : ",df3.count())


#4.Records having Null values in Visitor ID,Email and Gender
#---------------------------------------------------------------------------------------------------------------------------------------------------
df4 = webiste_visitor_df.filter((col('visitor_id').isNull()) & (col('email').isNull()) & (col('gender').isNull()))
df4.show(df4.count(),truncate=False)
print("total records having Null values in Visitor ID,Email and Gender : ",df4.count())


#5.Records that do not have null values in any of the columns present in the dataframe
#--------------------------------------------------------------------------------------------------------------------------------------------------
df5 = webiste_visitor_df.filter((col('visitor_id').isNotNull()) & (col('first_name').isNotNull()) & (col('last_name').isNotNull()) & (col('email').isNotNull()) &                                  (col('gender').isNotNull()) & (col('ip_address').isNotNull()))
df5.show(df5.count(),truncate=False)
print("Records that do not have null values in any of the columns : ",df5.count())


#6.Records having null values in Gender column
#--------------------------------------------------------------------------------------------------------------------------------------------------
df6 = webiste_visitor_df.filter(col('gender').isNull())
df6.show(df6.count(),truncate=False)
print("Records having null values in Gender column : ",df6.count())


#7.Adding Flag Column which will have 0 for Valid Email and 1 for Invalid Emails
#--------------------------------------------------------------------------------------------------------------------------------------------------
regex1 = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'             #using regular exp to check the validity of mail
df7 = email_data_df.withColumn("Flag",when(col('mail_id').rlike(regex1), lit(0)).otherwise(lit(1)))
df7.show(df7.count(),truncate=False)
