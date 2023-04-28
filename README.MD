# PySpark Course
Course:
https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/learn/lecture/24786062#overview

Course coverage:
- Use DataFrames and Structured Streaming in Spark 3
- Use the MLLib machine learning library to answer common data mining questions
- Understand how Spark Streaming lets your process continuous streams of data in real time
- Frame big data analysis problems as Spark problems
- Use Amazon's Elastic MapReduce service to run your job on a cluster with Hadoop YARN
- Install and run Apache Spark on a desktop computer or on a cluster
- Use Spark's Resilient Distributed Datasets to process and analyze large data sets across many CPU's
- Implement iterative algorithms such as breadth-first-search using Spark
- Understand how Spark SQL lets you work with structured data
- Tune and troubleshoot large jobs running on a cluster
- Share information between nodes on a Spark cluster using broadcast variables and accumulators
- Understand how the GraphX library helps with network analysis problems


If you are pretending running this, please send me a message.

## Ubuntu Instalation
- Follow http://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm
- Scala: https://github.com/lampepfl/dotty/releases/download/3.2.2/scala3-3.2.2.tar.gz (from https://www.scala-lang.org/download/3.2.2.html )
  - tar xzvf scala3-3.2.2.tgz 
  - sudo mv scala3-3.2.2 /usr/local/scala
  - export PATH = $PATH:/usr/local/scala/bin
  - scala -version
    ```
    # Scala code runner version 2.12.2 -- Copyright 2002-2017, LAMP/EPFL and Lightbend, Inc.
    ```
- Spark 3.3.2: https://spark.apache.org/downloads.html
  - wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
  - ./spark-shell --version
  ```
  23/03/09 16:21:18 WARN Utils: Your hostname, denis-dell resolves to a loopback address: 127.0.1.1; using 192.168.0.4 instead (on interface wlp2s0)
  23/03/09 16:21:18 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
  Welcome to
        ____              __
       / __/__  ___ _____/ /__
      _\ \/ _ \/ _ `/ __/  '_/
     /___/ .__/\_,_/_/ /_/\_\   version 3.3.2
        /_/
                          
  Using Scala version 2.12.15, OpenJDK 64-Bit Server VM, 11.0.18
  ```
  - PySpark. You need to check your python syntax
     - pip install pyspark

## Running

- Instruction: `spark-submit <file.py>`
- example:
  - `spark-submit customer-totals.py`
    ```
    Time to calculate 2.7768971920013428
    ITE00100554     90.14F
    EZE00100082     90.14F
    ```








