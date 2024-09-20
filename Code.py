# Databricks notebook source
#Import necessary libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("customer_file","")
dbutils.widgets.text("loan_file","")
dbutils.widgets.text("loan_repayment_file","")

# COMMAND ----------

customer_file = dbutils.widgets.get("customer_file")
loan_file = dbutils.widgets.get("loan_file")
loan_repayment_file = dbutils.widgets.get("loan_repayment_file")

print(customer_file)
print(loan_file)
print(loan_repayment_file)

# COMMAND ----------

dbutils.fs.mount(
 source = "wasbs://pa186030@tdaastrgdls.blob.core.windows.net",
 mount_point = "/mnt/pa186030",
 extra_configs = {"fs.azure.account.key.tdaastrgdls.blob.core.windows.net": "hrt8qv1dBGlnto3uwz8Nmi1VBPKiPpOww8cWUFvT3N/BBSrpSUoR3yslI0LEC3ReeI2yxuWjWxdArj23evI5sA=="}
)

# COMMAND ----------

cust_file = f'/mnt/pa186030/Input/{customer_file}'
loan_file = f'/mnt/pa186030/Input/{loan_file}'
loan_rp_file = f'/mnt/pa186030/Input/{loan_repayment_file}'

print(cust_file)
print(loan_file)
print(loan_rp_file)

# COMMAND ----------

# Schema for Customer JSON file

locationElement = StructType(
	[
		StructField("City",StringType(),True),
		StructField("State",StringType(),True),
		StructField("Country",StringType(),True),
	]
)

customerElement = StructType(
	[
		StructField("CustomerID",StringType(),True),
		StructField("CustomerName",StringType(),True),
		StructField("CustomerLocation",StructType(locationElement)),
	]
)

custSchema = StructType(
	[
		StructField("CustomerData",ArrayType(customerElement))
	]
)


# COMMAND ----------

# Read JSON into Dataframe

try:
  cust_df = spark.read.option("multiline","true").schema(custSchema).json(cust_file).select(explode("CustomerData").alias("CustData")).select("CustData.CustomerID","CustData.CustomerName","CustData.CustomerLocation.City","CustData.CustomerLocation.State","CustData.CustomerLocation.Country").orderBy("CustomerID")
except Exception as e:
  print("Error while reading file"+str(e))
  raise e

# COMMAND ----------

# Display Dataframe
display(cust_df)

# COMMAND ----------

#Schema for the Loan JSON file

loanElement = StructType(
	[
		StructField("BorrowerID", StringType(), True),
    StructField("EMIAmount", DoubleType(),True),
    StructField("InterestRate", DoubleType(), True),
		StructField("LoanAccountNumber", StringType(), True),
    StructField("LoanAmount", LongType(),True),
    StructField("LoanID", StringType(), True),
    StructField("LoanStatus", StringType(), True),
    StructField("LoanTerm", LongType(),True),
		StructField("LoanType", StringType(), True),
    StructField("MonthlyPaymentDay", LongType(),True),
    StructField("OriginationDate", StringType(), True)
	]
)

loanSchema = StructType(
  [
    StructField("LoanData", ArrayType(loanElement))
  ]
)
  

# COMMAND ----------

# Read JSON into Dataframe

try:
  loan_df = spark.read.option("multiline","true").schema(loanSchema).json(loan_file).select(explode("LoanData").alias("LnData")).select("LnData.BorrowerID","LnData.EMIAmount","LnData.InterestRate","LnData.LoanAccountNumber","LnData.LoanAmount","LnData.LoanID","LnData.LoanStatus","LnData.LoanTerm","LnData.LoanType","LnData.MonthlyPaymentDay","LnData.OriginationDate").orderBy("LoanID")
except Exception as e:
  print("Error while reading file"+str(e))
  raise e

# COMMAND ----------

# Display Dataframe
display(loan_df)

# COMMAND ----------

# Schema for Loan Repayment JSON file

loanRpElement = StructType(
	[
	  StructField("LoanID", StringType(), True),
      StructField("PaymentDate", StringType(),True),
      StructField("PaymentAmount", DoubleType(), True),
	  StructField("PrincipalPayment", DoubleType(), True),
      StructField("InterestPayment", DoubleType(),True),
      StructField("PaymentReferenceNumber", StringType(), True)
	]
)

loanRpSchema = StructType(
	[
	  StructField("LoanRepaymentData",ArrayType(loanRpElement))
	]
)

# COMMAND ----------

# Read JSON into Dataframe

try:
  loan_rp_df = spark.read.option("multiline","true").schema(loanRpSchema).json(loan_rp_file).select(explode("LoanRepaymentData").alias("LnRpData")).select("LnRpData.LoanID","LnRpData.PaymentDate","LnRpData.PaymentAmount","LnRpData.PrincipalPayment","LnRpData.InterestPayment","LnRpData.PaymentReferenceNumber").orderBy("LoanID")
except Exception as e:
  print("Error while reading file"+str(e))
  raise e

# COMMAND ----------

# Display Dataframe
display(loan_rp_df)

# COMMAND ----------

# Drop duplicates from Dataframe

cust_df = cust_df.dropDuplicates()
loan_df = loan_df.dropDuplicates()
loan_rp_df = loan_rp_df.dropDuplicates()

# COMMAND ----------

# Capitalize First Letter in CustomerName
cust_df = cust_df.withColumn("CustomerName",initcap(col('CustomerName'))).orderBy("CustomerID")

# COMMAND ----------

# Display Dataframe
display(cust_df)

# COMMAND ----------

# Replace blank PaymentReferenceNumber with null
loan_rp_df = loan_rp_df.withColumn("PaymentReferenceNumber",when(col("PaymentReferenceNumber")=="" ,"null").otherwise(col("PaymentReferenceNumber"))).orderBy("LoanID")

# COMMAND ----------

# Display Dataframe
display(loan_rp_df)

# COMMAND ----------

# Create temporary table/view from Dataframe
cust_df.createOrReplaceTempView("customer")
loan_df.createOrReplaceTempView("loan")
loan_rp_df.createOrReplaceTempView("loan_repayment")

# COMMAND ----------

# Join columns from Customer & Loan Dataframes based on CustomerID

cust_rp_df = cust_df.join(loan_df, cust_df.CustomerID == loan_df.BorrowerID).select(loan_df['BorrowerID'], cust_df['CustomerName'],cust_df['City'].alias('CustomerCity'),cust_df['State'].alias('CustomerState'),cust_df['Country'].alias('CustomerCountry'), loan_df['LoanID'], loan_df['LoanAccountNumber'],loan_df['LoanAmount'],loan_df['InterestRate'],loan_df['LoanTerm'],loan_df['LoanType'],loan_df['LoanStatus'],loan_df['OriginationDate'],loan_df['EMIAmount'])

display(cust_rp_df)

# COMMAND ----------

# Calculate Principal Balance Payment Left

prin_bal_df = spark.sql("select lr.LoanID, lo.LoanAmount, sum(lr.PrincipalPayment) as PrincipalPaid, lo.LoanAmount-sum(lr.PrincipalPayment) as PrincipalBalPymntLeft from loan lo inner join loan_repayment lr on lo.LoanID = lr.LoanID group by lr.LoanID,lo.LoanAmount order by lr.LoanID")

display(prin_bal_df)

# COMMAND ----------

# Calculate LateEMIPaymntDaysCnt & Total Penalty to be paid

penalty_df = spark.sql("select lo.BorrowerID, lr.LoanID, EXTRACT(DAY FROM to_date(lr.PaymentDate,'yyyy-MM-dd'))-lo.MonthlyPaymentDay as PaymentLateDays, lo.EMIAmount, (2.5*lo.EMIAmount)/100 as Penalty from loan lo inner join loan_repayment lr on lo.LoanID = lr.LoanID and EXTRACT(DAY FROM to_date(lr.PaymentDate,'yyyy-MM-dd'))-lo.MonthlyPaymentDay>0")

penalty_df = penalty_df.groupBy("LoanID").agg(first("EMIAmount").alias("EMIAmount"), first("Penalty").alias("Penalty"), sum("PaymentLateDays").alias("LateEMIPaymntDaysCnt"))

penalty_df = penalty_df.withColumn("TotalPenaltyPaid", (col("Penalty") * col("LateEMIPaymntDaysCnt"))).orderBy("LoanID")

display(penalty_df)

# COMMAND ----------

# Add PrincipalBalPymntLeft & TotalPenaltyPaid aggregated column value to Customer Aggregation Dataframe

cust_rp_df = cust_rp_df.join(prin_bal_df, cust_rp_df.LoanID == prin_bal_df.LoanID, "left").select(cust_rp_df['*'],prin_bal_df['PrincipalBalPymntLeft'])

cust_rp_df = cust_rp_df.join(penalty_df, cust_rp_df.LoanID == penalty_df.LoanID, "left").select(cust_rp_df['*'],penalty_df['TotalPenaltyPaid'],penalty_df['LateEMIPaymntDaysCnt'])

cust_rp_df = cust_rp_df.orderBy('BorrowerID')

display(cust_rp_df)

# COMMAND ----------

cust_rp_df.createOrReplaceTempView("loan_df_detail")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists pa186030;
# MAGIC
# MAGIC create table if not exists pa186030.customer_loan (
# MAGIC BorrowerID String,
# MAGIC CustomerName String,
# MAGIC CustomerCity String,
# MAGIC CustomerState String,
# MAGIC CustomerCountry String,
# MAGIC LoanID String,
# MAGIC LoanAccountNumber String,
# MAGIC LoanAmount bigint,
# MAGIC InterestRate double,
# MAGIC LoanTerm bigint,
# MAGIC LoanType String,
# MAGIC LoanStatus String,
# MAGIC OriginationDate String,
# MAGIC EMIAmount double,
# MAGIC PrincipalBalPymntLeft double,
# MAGIC TotalPenaltyPaid double,
# MAGIC LateEMIPaymntDaysCnt bigint,
# MAGIC ActiveInd Char(1),
# MAGIC EndDate timestamp)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC MERGE INTO pa186030.customer_loan tgtTbl
# MAGIC USING (
# MAGIC SELECT distinct srcTbl1.BorrowerID as mergekey, srcTbl1.*
# MAGIC   FROM loan_df_detail srcTbl1
# MAGIC   UNION ALL
# MAGIC   SELECT DISTINCT NULL as mergeKey, srcTbl.* FROM loan_df_detail srcTbl join pa186030.customer_loan tg
# MAGIC   ON srcTbl.BorrowerID = tg.BorrowerID
# MAGIC   WHERE (
# MAGIC 	srcTbl.CustomerName <> tg.CustomerName OR
# MAGIC 	srcTbl.CustomerCity <> tg.CustomerCity OR
# MAGIC 	srcTbl.CustomerState <> tg.CustomerState OR
# MAGIC 	srcTbl.CustomerCountry <> tg.CustomerCountry OR
# MAGIC 	srcTbl.LoanAccountNumber <> tg.LoanAccountNumber OR
# MAGIC 	srcTbl.LoanAmount <> tg.LoanAmount OR
# MAGIC 	srcTbl.InterestRate <> tg.InterestRate OR
# MAGIC 	srcTbl.LoanTerm <> tg.LoanTerm OR
# MAGIC 	srcTbl.LoanType <> tg.LoanType OR
# MAGIC 	srcTbl.LoanStatus <> tg.LoanStatus OR
# MAGIC 	srcTbl.OriginationDate <> tg.OriginationDate OR
# MAGIC 	srcTbl.PrincipalBalPymntLeft <> tg.PrincipalBalPymntLeft OR
# MAGIC 	srcTbl.TotalPenaltyPaid <> tg.TotalPenaltyPaid OR
# MAGIC 	srcTbl.LateEMIPaymntDaysCnt <> tg.LateEMIPaymntDaysCnt
# MAGIC   )
# MAGIC )staged_loan
# MAGIC ON tgtTbl.BorrowerID = mergekey
# MAGIC WHEN MATCHED  
# MAGIC THEN 
# MAGIC UPDATE SET tgtTbl.ActiveInd = 'N', tgtTbl.EndDate = current_timestamp()
# MAGIC WHEN NOT MATCHED BY TARGET
# MAGIC THEN INSERT (BorrowerID, CustomerName, CustomerCity, CustomerState, CustomerCountry, LoanID, LoanAccountNumber, LoanAmount, InterestRate, LoanTerm, LoanType, LoanStatus, OriginationDate, EMIAmount, PrincipalBalPymntLeft, TotalPenaltyPaid, LateEMIPaymntDaysCnt, ActiveInd, EndDate)          
# MAGIC VALUES (staged_loan.BorrowerID, staged_loan.CustomerName, staged_loan.CustomerCity, staged_loan.CustomerState, staged_loan.CustomerCountry, staged_loan.LoanID, staged_loan.LoanAccountNumber, staged_loan.LoanAmount, staged_loan.InterestRate, staged_loan.LoanTerm, staged_loan.LoanType, staged_loan.LoanStatus, staged_loan.OriginationDate, staged_loan.EMIAmount, staged_loan.PrincipalBalPymntLeft, staged_loan.TotalPenaltyPaid, staged_loan.LateEMIPaymntDaysCnt, 'Y', dateadd(YEAR, staged_loan.LoanTerm, staged_loan.OriginationDate))
# MAGIC WHEN NOT MATCHED BY SOURCE 
# MAGIC THEN UPDATE SET tgtTbl.ActiveInd = 'N', tgtTbl.EndDate = current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pa186030.customer_loan
# MAGIC ORDER BY BorrowerID;

# COMMAND ----------

dbutils.fs.unmount("/mnt/pa186030")
