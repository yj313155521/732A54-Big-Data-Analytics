# Lab 2

**Members**:  jinya425 (Jin Yan), siyli424 (Siyu Liu)

**We take first 20 rows of the result on each assignment, please check the `output` folder if need to see all results**

### Q1: What are the lowest and highest temperatures measured each year for the period 1950-2014. Provide the lists sorted in the descending order with respect to the maximum temperature.

```
+----+---------------+---------------+                                          
|year|yearly_max_temp|yearly_min_temp|
+----+---------------+---------------+
|2014|           34.4|          -42.5|
|2013|           31.6|          -40.7|
|2012|           31.3|          -42.7|
|2011|           32.5|          -42.0|
|2010|           34.4|          -41.7|
|2009|           31.5|          -38.5|
|2008|           32.2|          -39.3|
|2007|           32.2|          -40.7|
|2006|           32.7|          -40.6|
|2005|           32.1|          -39.4|
|2004|           30.2|          -39.7|
|2003|           32.2|          -41.5|
|2002|           33.3|          -42.2|
|2001|           31.9|          -44.0|
|2000|           33.0|          -37.6|
|1999|           32.4|          -49.0|
|1998|           29.2|          -42.7|
|1997|           31.8|          -40.2|
|1996|           30.8|          -41.7|
|1995|           30.8|          -37.6|
+----+---------------+---------------+
only showing top 20 rows
```

### Q2: Count the number of readings for each month in the period of 1950-2014 which are higher than 10 degrees.

```
+----+-----+-----+                                                              
|year|month|count|
+----+-----+-----+
|2014|   12|    1|
|2014|   11|  158|
|2014|   10|  270|
|2014|    9|  296|
|2014|    8|  296|
|2014|    7|  297|
|2014|    6|  298|
|2014|    5|  296|
|2014|    4|  254|
|2014|    3|  169|
|2014|    2|   15|
|2013|   12|    8|
|2013|   11|  114|
|2013|   10|  270|
|2013|    9|  299|
|2013|    8|  300|
|2013|    7|  301|
|2013|    6|  302|
|2013|    5|  301|
|2013|    4|  208|
+----+-----+-----+
only showing top 20 rows
```

### Q3: Find the average monthly temperature for each available station in Sweden. Your result should include average temperature for each station for each month in the period of 1960- 2014. Bear in mind that not every station has the readings for each month in this timeframe.

```
+----+-----+-------+----------------+                                           
|year|month|station|avg_monthly_temp|
+----+-----+-------+----------------+
|2014|   12|  99450|             1.9|
|2014|   12|  99280|             2.3|
|2014|   12|  99270|             2.4|
|2014|   12|  98490|            -1.6|
|2014|   12|  98290|             0.5|
|2014|   12|  98230|             0.3|
|2014|   12|  98210|             0.6|
|2014|   12|  98180|             0.0|
|2014|   12|  98040|             0.4|
|2014|   12|  97530|            -1.8|
|2014|   12|  97510|            -1.2|
|2014|   12|  97400|            -0.9|
|2014|   12|  97370|            -1.6|
|2014|   12|  97280|             0.3|
|2014|   12|  97120|            -0.7|
|2014|   12|  97100|            -0.8|
|2014|   12|  96560|            -2.4|
|2014|   12|  96550|            -2.5|
|2014|   12|  96350|            -1.2|
|2014|   12|  96190|            -0.9|
+----+-----+-------+----------------+
only showing top 20 rows
```

### Q4: Provide a list of stations with their associated maximum measured temperatures and maximum measured daily precipitation. Show only those stations where the maximum temperature is between 25 and 30 degrees and maximum daily precipitation is between 100 mm and 200mm.

```
+-------+----------------+--------+                                             
|station|max_daliy_precip|max_temp|
+-------+----------------+--------+
+-------+----------------+--------+
```

### Q5: Calculate the average monthly precipitation for the Östergotland region (list of stations is provided in the separate file) for the period 1993-2016. In order to do this, you will first need to calculate the total monthly precipitation for each station before calculating the monthly average (by averaging over stations).

```
+----+-----+------------------+                                                 
|year|month|avg_monthly_precip|
+----+-----+------------------+
|2016|    7|               0.0|
|2016|    6|              47.7|
|2016|    5|              29.3|
|2016|    4|              26.9|
|2016|    3|              20.0|
|2016|    2|              21.6|
|2016|    1|              22.3|
|2015|   12|              28.9|
|2015|   11|              63.9|
|2015|   10|               2.3|
|2015|    9|             101.3|
|2015|    8|              27.0|
|2015|    7|             119.1|
|2015|    6|              78.7|
|2015|    5|              93.2|
|2015|    4|              15.3|
|2015|    3|              42.6|
|2015|    2|              24.8|
|2015|    1|              59.1|
|2014|   12|              35.5|
+----+-----+------------------+
only showing top 20 rows
```