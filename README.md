# BigDataWithSpark

This very simple Big Data with Apache Spark.

## What Do this Code?

This code finds the name of the country from the domain up and counts how many IPs each country has.

## This Code for

first you need download bin file ip2location.  
second if you need add more data in ip range.  
third you need fix all path files.

for download bin file see this [link](https://lite.ip2location.com).

For run scala :
> sbt compile
> sbt run

Note: you can add your master server for clustering(in python code we do this).

For python:
> pip install -r req.txt
> py main.py

And output can be like This (you can see export file in folder out and out.cvs):

```txt
+---+-----+
|col|count|
+---+-----+
| SX|   60|
| NF|   60|
| MS|   60|
| NU|   60|
| TK|   60|
+---+-----+
```
