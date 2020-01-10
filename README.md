# apache-beam-pipelines
Apache beam pipeline implementations and examples. This README serves as a skeleton for getting the implementations to work on your own machine. All the implementations are coded in JAVA and Python.

## Preliminaries
1. You need a java JDK: I used version 8. This is a useful [tutorial](https://www.javahelps.com/2015/03/install-oracle-jdk-in-ubuntu.html)
2. Install Apache Maven (I used version 3.6.1)
3. It's probably useful to have done the MinimalWordCount example through Apache's tutorial on their [website](https://beam.apache.org/get-started/wordcount-example/#minimalwordcount-example) and to look through some of the Apache Documentation to get an idea of the programming model.
4. VS Code is a useful IDE that I used.

## Dependencies
Dependencies can be found in 'dependencies.txt'
 
1.	apache-beam==2.16.0
2.	avro==1.9.1
3.	fastavro==0.21.24
4.	jsonschema==2.6.0
5.	numpy==1.15.1
6.	py==1.8.1
7.	scipy==1.1.0
8.	virtualenv==16.7.9

# Ambrite.py
Ambrite.py reads in a .csv file consisting of a user ID, username and password.  All digits, capitals and duplicates are removed and then written to a new .csv file as well as an .avro file. The schema for the .avro file is defined by 'user.avsc'.

Run: $ python Ambrite.py --input input10.csv --output output

# AmbriteTest.py
AmbriteTest.py run several unit tests for each function in Ambrite.py.

Run: $ python AmbriteTest.py

# Ambrite.java
Ambrite.java reads in a .csv file consisting of a user ID, username and password.  All digits, capitals and duplicates are removed and then written to a new .csv file as well as an .avro file. The schema for the .avro file is defined by 'user.avsc'.

Run: $mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.Ambrite -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner

# AmbriteTest.java
AmbriteTest.py run several unit tests for each function in Ambrite.java.

Run: $ mvn test

# Useful Links
1. [Beam Guide](https://beam.apache.org/documentation/programming-guide/#additional-outputs)
2. [Getting started with AVRO](https://avro.apache.org/docs/current/gettingstartedpython.html)
3. [AVRO write](https://beam.apache.org/releases/pydoc/2.5.0/apache_beam.io.avroio.html)