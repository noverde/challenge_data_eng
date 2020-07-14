# Noverde - Data Challenge

## Introduction - Let's Get Started

In this challenge where you'll be put through a series of tests, in order to assess different skills that are valuable to our Data Team.

At Noverde our Data Team is comprised of a wide variety of functions and personalities.
We always encourage our Data Engineers and Analyst to put your Expertise and Studies to test, using Any Language, Framework and Platform, that might help solve any challenge.

__*Data Engineer*__ your greatest responsibility is to create and maintain a secure, scalable and robust ETL pipeline to process files any formats and store them in a structured and easy to understand manner.

__*Data Analyst*__ your greater resposability it's to aggregate data and transform it into information, insights, dashboards and databases to meet all the Internal (Data Team) and External (Bussiness Areas) needs.

Noverde is a Data Driven Company that works with a variety of Products.
One of Products is __*Loan*__, that said, it happens our Data Analysts, Data Scientists and End Users need data to build their Dashboards, Studies, Models and to make decisions.

> Note: You're free to use any language and technology to reach your goal. Languages, frameworks, platforms are not a constraint.

## Part 1 - ETL Building

### Objective

The key Objective it's Build a table named  `loan_documents`, saved as a Parquet File.

### Data Source

> Working with different Files Formats and aggregate them is part of the job.

All data comes from our service architecture. For this test the Service Team offered us a dump from different bases that treats the Loan Product:
- Loan data as CSV files stored in this url: https://noverde-data-engineering-test.s3.amazonaws.com/loans_sample.csv
- Installments data as JSON files stored in this url: https://noverde-data-engineering-test.s3.amazonaws.com/installments_sample.json
- Payments data as Parquet files stored in this url: https://noverde-data-engineering-test.s3.amazonaws.com/payments_sample.parquet

This files are similar to the ones we work when we speak of __*Loan*__.

### Structute

Each Row of the file `loan_documents` it's composed by the aggregate information of *loan*, *installments*, *payments* and some pre calculated metrics.

```sql
CREATE EXTERNAL TABLE loan_documents (
  loan_id INT,
  period INT,
  accepted_at TIMESTAMP,
  payday INT,
  interest_rate DOUBLE,
  installments MAP<INT, STRING>,
  payments ARRAY<STRUCT<id: INT, payment_date: STRING, method: STRING, amount: DOUBLE>>,
  metrics STRUCT<latency: ARRAY<BOOLEAN>, over30: ARRAY<BOOLEAN>>
)
ROW FORMAT SERDE                                                   
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'    
WITH SERDEPROPERTIES (                                             
  'path'='<PAQUET FILE PATH>')    
STORED AS INPUTFORMAT                                              
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'  
OUTPUTFORMAT                                                       
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION '<PAQUET FILE PATH>'
```

Example Row (As a YAML doc):

```yaml
loan_id: 9243
period: 3
accepted_at: "2017-05-19 10:09:47.285105"
payday: 4
interest_rate: 3.12
installments:
  1: "2017-06-04"
  2: "2017-07-04"
  3: "2017-08-04"
payments:
- id: "7abaf860-9632-40e9-bfe0-13b67ded8c6f"
  payment_date: "2017-06-06"
  method: boleto
  amount: 283.09
- id: "6abaf860-9632-40e9-bfe0-33b67ded8c6d"
  payment_date: "2017-07-01"
  method: boleto
  amount: 280.00
- id: "8abaf860-9632-40e9-bfe0-33b67ded8c6d"
  payment_date: "2017-09-01"
  method: boleto
  amount: 295.17
metrics:
  latency: [false, false, false, true, true, true, false, ...]
  over05: [false, false, true, ...]
  over15: [false, false, true, ...]
  over30: [false, false, false, ...]
```

> Note 1: There is no need to verifying if this Hive Schema is compatible. The main quest here it's to work with complex data Types like array, dictionary and nested structures.
> Note 2: __*`installments`*__ is a Dictionary using __`installment_number`__ as it's *key* and __`due_date`__ as it's *value*

### Metrics
Each Metric it's stored as an Array where every index it's a specific day from it's origination date *`loan.accepted_at_`* until yesterday.

##### "Latency"
The `latency` metric is a list of Boolean values, where each value indicates if there is a delay in payment for the *Metric[x]* corresponding date.

- Latency[0]: Origination date *`loan.accepted_at_`*. Example: `2020-07-01`
- Latency[1]: One day later after the origination date *`loan.accepted_at_`*. Example: `2020-07-02`
- Latency[n]: Yesterday. Example: `2020-07-07`

#### "Over(N)" Metric

The `OverN` metric is a list of Boolean values, where each value indicates if payment of a specific installment *Metric[x]* had a `N` number of days in delay in payment.

- Over05[0]: The `payment_date` of the `installment number`[0] was made 05 days later than the `due date`
- Over15[1]: The `payment_date` of the `installment number`[1] was made 15 days later than the `due date`
- Over30[n]: The `payment_date` of the `installment number`[N] was made 30 days later than the `due date`

## Part 2 - Data Analysis

### Objective

The key Objective it's to respond to a series of business questions made by our Business Areas with the result Table from Part - 1.
The Answer for each question must have at least the Query used to generate the result and one of the following representations :
- Table View
- Parquet File
- Chart

### Analysis

1) What is Received Amount Ratio per Month and Year? What the day that __Noverde__ received value was the higher in each month?

  Result Example:
  ```
  |Month|Year|Expected_Received_Amount|Received_Amount|Received_Amount_Ratio|
  |-----|----|------------------------|---------------|---------------------|
  |01   |2019|10000.00                |9000.00        |90%                  |
  |02   |2019|20000.00                |1600.00        |80%                  |
  ---------------------------------------------------------------------------

  |Day|Month|Year|Received_Amount|
  |---|-----|----|---------------|
  |01 |08   |2019|10000.00       |
  |23 |09   |2019|20000.00       |
  ```

2) One of our Investor wants to know what are the Key Portfolio Highlights for each Month?
  > Note: The Highlits are: `Average Payment Date`, `Average Interest Rate` and `Most Frequently Payday`.

  Result Example:
  ```
  |Month|Year|Avg_Period|Avg_Interest_Ratio|Freq_Payday|
  |-----|----|----------|------------------|-----------|
  |03   |2019|10.5      |8.0%              |20         |
  |04   |2019|10.6      |7.8%              |15         |
  |05   |2019|10.0      |7.5%              |20         |
  ```

3)	Thinking about our entire client portfolio, what is the real distribution between contracts with different terms and rates?
  > Note: The distribution are given by the amount of contracts (loans) per duration and tax rate

  Result Example:
  ```
  |Duration|Interest_Rate|Avg_Interest_Ratio|Freq_Payday|Amount_Contracts|
  |--------|-------------|------------------|-----------|----------------|
  |06      |3.12%        |20%               |           |                |
  |09      |3.12%        |17%               |           |                |
  |12      |3.12%        |8%                |           |                |
  |06      |5.15%        |0%                |           |                |
  |09      |5.15%        |2%                |           |                |
  ```

4)	What's the Ratio of Matured Loans in our Portfolio that has at least one installment with Over 30 in payment delay?
  > Note: Matured Loan in this question means any Loan that the first `installment_date` occurred at least one month ago--

  Result Example:
  ```
  |Portfolio|Matured|Over30_True|Matured_Ratio|
  |---------|-------|-----------|-------------|
  |15.000   |10.000 |3.500      |35%          |
  ```

## Deliverable

You know the drill: Any language, any framework, any platform. Feel free to use anything to help you finishing this tasks.

*Tips*
- All of your Queries need to perform well, be maintainable and return accurate data.
- All of you Solutions needs to be scalable! The SRE Team insists that we approach carefully with the "Execution Size" in each month. 

__*Data Engineer*__ your solution must be inside a docker image, script or notebook ready to be run. If you provide a docker image, please send it with a backend service. Running this container should start the necessary infrastructure to provide the endpoints
__*Data Analyst*__ your solution must be inside notebook ready to be run

## References

- Databricks Community: https://community.cloud.databricks.com
- all-spark-notebook docker: https://hub.docker.com/r/jupyter/all-spark-notebook/
- Google Colab: https://colab.research.google.com/

### For Google Colab

Following these instructions and you'll be able to use Spark on the Google Colab Notebook (Adaptation from https://movile.blog/introducao-a-spark-usando-o-google-colab/)

```
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://www-eu.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
!tar xf spark-2.4.4-bin-hadoop2.7.tgz
!pip install -q findspark
!pip install -q pyspark
```

```
import os

os.environ["SPARK_HOME"] = "/content/spark-2.4.4-bin-hadoop2.7"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
```

```
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
```
#After this box of code you will be able to use Spark freely#

```
df = spark.read.csv("localfile.csv")

```
