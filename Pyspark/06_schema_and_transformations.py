from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col

spark = SparkSession.builder.enableHiveSupport().master('yarn').getOrCreate()

data = [Row(1,"John",30,"Sales",50000.0),
        Row(2,"Alice",28,"Marketing",60000.0),
        Row(3,"Bob",32,"Finance",55000.0),
        Row(4,"Sarah",29,"Sales",52000.0),
        ]

schema = StructType([StructField("id",IntegerType(),nullable=False),
        StructField("name", StringType(),nullable=False),
        StructField("age",IntegerType(),nullable=False),
        StructField("department",StringType(),nullable=False),
        StructField("salary",DoubleType(),nullable=False)
        ])

employeeDF = spark.createDataFrame(data,schema)
employeeDF.show()

#Calculating average_salary for each department
avg_DF = employeeDF.groupBy("department").agg(F.avg("salary").alias("avg_sal_by_dept"))

#Add a new column named "bonus" that is 10% of salary for all employees
employee_new_DF = employeeDF.selectExpr('*','salary*0.1 as bonus')

#Group data by department and find employee with highest salary in each department
windowSpec = Window.partitionBy("department").orderBy("salary")
highestSalByDept = employeeDF.withColumn("row_number",row_number().over(windowSpec))
highestSalByDept.filter(highestSalByDept.row_number==1).show()

#Find top 3 departments with highest total salary
top3dept = employeeDF.groupBy("department").agg(F.sum("salary").alias("totalSalByDept")).orderBy(desc("totalSalByDept"))
top3dept.show()

#Find top most department having highest salary
employee_CTE = employeeDF.groupBy("department").agg(F.sum("salary").alias("total_sal"))
windowSpec = Window.orderBy(col("total_sal").desc())
department_rnk = employee_CTE.withColumn("row_number",row_number().over(windowSpec))
result = department_rnk.filter(department_rnk.row_number==1).select("department")
result.show()

#Filter to keep only employees aged 30 or above and working in "Sales" department
above30Sales = employeeDF.filter((employeeDF.age >=30) & (employeeDF.department=='Sales'))
above30Sales.show()

#Calculate difference between each employee's salary and average salary of their respective department
windowSpec = Window.partitionBy("department")
employeeDF = employeeDF.withColumn("avg_sal_by_depart",F.avg(col("salary")).over(windowSpec))
sal_Diff = employeeDF.withColumn("sal_diff_by_dpt", col("salary")-col("avg_sal_by_depart"))
sal_Diff.show()

#Calculate sum of salaries for employees whose names starts with letter "J"

sumSalaries = employeeDF.filter(col("name").startswith("J")).agg(F.sum(col("salary")).alias("total_salary"))
sumSalaries.show()

#Sort DF based on age col in asc order and then by salary in desc order
sortedDF = employeeDF.orderBy(col("age"),col("salary").desc())
sortedDF.show()

#Replace the department name "Finance" with "Financial Services" in DF
updatedDF = employeeDF.withColumn("department", F.when(col("department") == "Finance", "Financial Services").otherwise(col("department")))

#Calculate the percentage of total salary each employee contributes to resp department
windowSpec = Window.partitionBy("department")
employeeDF = employeeDF.withColumn("total_salary_dept", F.sum(col("salary")).over(windowSpec))
percentageContribution = (col("salary") / col("total_salary_dept"))*100
employeeDF = employeeDF.withColumn("percentage_contribution",F.round(percentageContribution,2))
employeeDF.show()