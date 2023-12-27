# Lab 1

**Members**:  jinya425 (Jin Yan), siyli424 (Siyu Liu)

**We take first 20 rows of the result on each assignment, please check the `output` folder if need to see all results**

### Q1: What are the lowest and highest temperatures measured each year for the period 1950-2014. Provide the lists sorted in the descending order with respect to the maximum temperature.

max temperature

```
('2014', ('102170', 34.4))
('2013', ('102170', 31.6))
('2012', ('102190', 31.3))
('2011', ('102190', 32.5))
('2010', ('102190', 34.4))
('2009', ('102190', 31.5))
('2008', ('102190', 32.2))
('2007', ('102190', 32.2))
('2006', ('102190', 32.7))
('2005', ('102190', 32.1))
('2004', ('102190', 30.2))
('2003', ('102190', 32.2))
('2002', ('102190', 33.3))
('2001', ('102190', 31.9))
('2000', ('102190', 33.0))
('1999', ('102210', 32.4))
('1998', ('102210', 29.2))
('1997', ('102210', 31.8))
('1996', ('102210', 30.8))
('1995', ('102210', 30.8))
```

min temperature

```
('2014', ('102170', -42.5))
('2013', ('102170', -40.7))
('2012', ('102190', -42.7))
('2011', ('102190', -42.0))
('2010', ('102190', -41.7))
('2009', ('102190', -38.5))
('2008', ('102190', -39.3))
('2007', ('102190', -40.7))
('2006', ('102190', -40.6))
('2005', ('102190', -39.4))
('2004', ('102190', -39.7))
('2003', ('102190', -41.5))
('2002', ('102190', -42.2))
('2001', ('102190', -44.0))
('2000', ('102190', -37.6))
('1999', ('102210', -49.0))
('1998', ('102210', -42.7))
('1997', ('102210', -40.2))
('1996', ('102210', -41.7))
('1995', ('102210', -37.6))
```

### Q2: Count the number of readings for each month in the period of 1950-2014 which are higher than 10 degrees.

```
(('2014', '12'), 1)
(('2014', '11'), 158)
(('2014', '10'), 270)
(('2014', '09'), 296)
(('2014', '08'), 296)
(('2014', '07'), 297)
(('2014', '06'), 298)
(('2014', '05'), 296)
(('2014', '04'), 254)
(('2014', '03'), 169)
(('2014', '02'), 15)
(('2013', '12'), 8)
(('2013', '11'), 114)
(('2013', '10'), 270)
(('2013', '09'), 299)
(('2013', '08'), 300)
(('2013', '07'), 301)
(('2013', '06'), 302)
(('2013', '05'), 301)
(('2013', '04'), 208)
```

### Q3: Find the average monthly temperature for each available station in Sweden. Your result should include average temperature for each station for each month in the period of 1960- 2014. Bear in mind that not every station has the readings for each month in this timeframe.

```
(('2014', '12', '99450'), 1.9)
(('2014', '12', '99280'), 2.3)
(('2014', '12', '99270'), 2.4)
(('2014', '12', '98490'), -1.6)
(('2014', '12', '98290'), 0.5)
(('2014', '12', '98230'), 0.3)
(('2014', '12', '98210'), 0.6)
(('2014', '12', '98180'), 0.0)
(('2014', '12', '98040'), 0.4)
(('2014', '12', '97530'), -1.8)
(('2014', '12', '97510'), -1.2)
(('2014', '12', '97400'), -0.9)
(('2014', '12', '97370'), -1.6)
(('2014', '12', '97280'), 0.3)
(('2014', '12', '97120'), -0.7)
(('2014', '12', '97100'), -0.8)
(('2014', '12', '96560'), -2.4)
(('2014', '12', '96550'), -2.5)
(('2014', '12', '96350'), -1.2)
(('2014', '12', '96190'), -0.9)
```

### Q4: Provide a list of stations with their associated maximum measured temperatures and maximum measured daily precipitation. Show only those stations where the maximum temperature is between 25 and 30 degrees and maximum daily precipitation is between 100 mm and 200mm.

```
The result is empty
```

### Q5: Calculate the average monthly precipitation for the Östergotland region (list of stations is provided in the separate file) for the period 1993-2016. In order to do this, you will first need to calculate the total monthly precipitation for each station before calculating the monthly average (by averaging over stations).

```
(('2016', '07'), 0.0)
(('2016', '06'), 47.7)
(('2016', '05'), 29.3)
(('2016', '04'), 26.9)
(('2016', '03'), 20.0)
(('2016', '02'), 21.6)
(('2016', '01'), 22.3)
(('2015', '12'), 28.9)
(('2015', '11'), 63.9)
(('2015', '10'), 2.3)
(('2015', '09'), 101.3)
(('2015', '08'), 27.0)
(('2015', '07'), 119.1)
(('2015', '06'), 78.7)
(('2015', '05'), 93.2)
(('2015', '04'), 15.3)
(('2015', '03'), 42.6)
(('2015', '02'), 24.8)
(('2015', '01'), 59.1)
(('2014', '12'), 35.5)
```