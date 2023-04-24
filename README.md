### NBA Logs (Part 1)
This repository contains 2 scripts and one csv file to answer the question in hand related to comfortable zone of shooting as a matrix of {SHOT DIST, CLOSE DEF DIST, SHOT CLOCK}.

#### Getting Started
Proceed to clone the repository into the already existing test_python folder in spark examples git (https://github.com/yingmao/spark-examples).
git clone https://github.com/rosamsierrap/test-lab2.git

#### Data
The data used in this analysis was obtained from an Kaggle website, which can be accessed here: (https://www.kaggle.com/datasets/dansbecker/nba-shot-logs)

#### Usage
To run this code, first you need to install Spark environment, in our case we used 3 node cluster hosted on Google Cloud. Make sure if you're going to use a different CSV file, stored it in the  corresponding directory or change the part1.sh file to reference the new directory.

#### Findings
•	lebron james: [[4.9381, 5.0135, 19.7335], [4.3557, 2.5567, 11.1062], [22.0512, 5.0729, 10.0928], [7.4103, 2.7603, 4.5103]]
•	chris paul: [[6.0174, 3.8826, 16.4449], [9.4607, 3.7344, 7.223], [18.2138, 5.0337, 15.5571], [21.0702, 5.0585, 6.8415]]
•	stephen curry: [[23.4638, 5.8014, 7.8217], [13.6704, 4.0535, 15.3338], [4.0831, 3.1622, 15.2093], [24.0569, 5.6875, 17.4181]]
•	james harden: [[14.2963, 3.9556, 8.7778], [23.9505, 5.3021, 15.1814], [21.807, 4.1, 4.2279], [4.0505, 3.0248, 15.8842]]

---

### NYC Parking and Tickets (Part 2)

This repository contains 2 scripts and one csv file to answer the question in hand related to the probability that it will get a ticket, given a Black vehicle parking illegally at 34510, 10030, 34050, 25390 and 22040 street codes.

#### Data
The data used in this analysis was obtained from the NYC Open Data website, which can be accessed here: https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2023/pvqr-7yc4

#### Usage
To run this code, first you need to install Spark environment, in our case we used 3 node cluster hosted on Google Cloud. Make sure if you're going to use a different CSV file, stored it in the  corresponding directory or change the part1.sh file to reference the new directory.

#### Findings
-	3.85% is the probability of getting a ticket.
---

## NYC Parking and Tickets (Part 3)
This repository contains 2 scripts and one csv file to answer the question in hand related to hours that are more likely to get a ticket.

#### Data
The data used in this analysis was obtained from the NYC Open Data website, which can be accessed here: https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2023/pvqr-7yc4

#### Usage
To run this code, first you need to install Spark environment, in our case we used 3 node cluster hosted on Google Cloud. Make sure if you're going to use a different CSV file, stored it in the  corresponding directory or change the part1.sh file to reference the new directory.

#### Findings
- 4PM is the time at which more tickets are issued.

---
