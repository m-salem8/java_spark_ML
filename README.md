# Java Spark Machine Learning Pipeline

## Overview

This project focuses on using Apache Spark with Java for building a regression model to predict the median value of inhabited houses per city in thousands of dollars. The dataset used for this project includes various features like crime rate, residential area proportion, etc.

## Prerequisites

- Apache Spark
- Java Development Kit (JDK)
- Maven

## Project Steps

### 1. Data Exploration

- **Count Cities Near the River:**
  - Identify the number of cities near the river and those not near the river.

- **Average Number of Rooms:**
  - Count the occurrences of the average number of rooms per dwelling.

- **Display Different Modalities of RAD:**
  - Show the different categories of the highway accessibility index (RAD).

### 2. Data Transformation

- **Create Spark Session and Context:**
  - Initialize a Spark Session and create a Spark Context.

- **Convert RDD to DataFrame:**
  - Convert the raw data RDD to a DataFrame.

- **Define Schema:**
  - Define the schema for the DataFrame.

- **Convert Columns to Double Format:**
  - Transform all columns to the Double format.

- **Display DataFrame Statistics:**
  - Show statistics using the filter, groupBy, and count methods.

### 3. Machine Learning Pipeline

- **Linear Regression with Cross-Validation:**
  - Implement a Linear Regression model with cross-validation.
  
- **Evaluate Model:**
  - Use the RegressionEvaluator to assess the model's performance.

## How to Run

1. Clone the repository.
2. Install Apache Spark, JDK, and Maven.
3. Compile the Java code (ML_pipline.java) using Maven. which will use the RddToDataFrameConvertor Function. this function was created to make it readable. 
4. Run the compiled Java program.


## Author

[Mahmoud Salem]