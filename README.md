# population-rollup
This project is developed with Python.
The purpose is to analyze Census Tract Population data from 2000 to 2010, and calculate the following metrics for each Core Based Statistical Area
*total number of census tracts
*total population in 2000
*total population in 2010 
*average population percent change for census tracts in this Core Based Statistical Area

# Table of Contents
1. [Problem](README.md#problem)
2. [Approach](README.md#Approach)
3. [Performance Test](README.md#Performance)
4. [Run Instruction](README.md#Run)

# Problem

## Input
The input file is taken from the data file at [2000 to 2010 Census Tract Population Change](https://www.census.gov/data/tables/time-series/dec/metro-micro/tract-change-00-10.html), which contains population counts for census tracts and how much they've changed over the decade. While census tracts are fairly small geographical areas inhabited by 1,200 to 8,000 people, they also can be grouped into larger Metropolitan and Micropolitan Statistical Areas called Core Based Statistical Areas. These core areas comprise a set of communities, often with a population center and shared economic and social ties. 

Each line of the input file, except for the first-line header, represents population data for a census tract. Consult this [file layout document produced by the Census](https://www2.census.gov/programs-surveys/metro-micro/technical-documentation/file-layout/tract-change-00-10/censustract-00-10-layout.doc) for a description of each field.

## Output
The program creates an output file, `report.csv`, which contains the following information:
* Core Based Statstical Area Code (i.e., CBSA09)
* Core Based Statistical Area Code Title (i.e., CBSA_T)
* total number of census tracts
* total population in the CBSA in 2000
* total population in the CBSA in 2010
* average population percent change for census tracts in this CBSA. Round to two decimal places using standard rounding conventions

# Approach
## Read Data
The csv file is created on a windows system, so the new line is specified by "\r\n". This might lead to mislocation, because python will neglect "\r" during processing. To avoid this situation, "rb" access mode is used to open the file.

## Class CBSA 
Class CBSA contains all aggregated data (counts, total population in 2000/2020, and population change)of each Core Based Statistical Area. It also contains a bundle of functions which help to get data, update data, add new cbsa and save the data as `report.csv`.

## Multi-processing
Multi-processing is implemented to speed up data processing, especially when dealing with big data. A file of large size is split into n chunks, and each chunk is assigned to an independent process. Each process operates a single CBSA object seperately, and merge all the CBSA objects together at the end.

![Screenshot](https://github.com/markice25/population-rollup/blob/main/doc/Slide1.jpg)


# Performance Test
1.  Data augmentation
The provided data set is only 10MB. In order to test the performance of multi-processing, data is augmented by repeating the same data by 1000 times. The augmented data-set is 5GB
2.  Multi-processing vs Single processing
Performance test is run on a computer with Intel CORE i5 8th Gen, and memeroy of 8G. Multi-processing program with 8 process takes 65 seconds to finish while sigle-processing takes 278 seconds to complete. Multi-processing program is much more efficient!

# Run instruction
<pre>
process3.py [-h] [-i INPUT] [-o OUTPUT] [-c NUM_CPUS] [-b BUF_SIZE]
optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        input file name
  -o OUTPUT, --output OUTPUT
                        input file name
  -c NUM_CPUS, --num-cpus NUM_CPUS
                        number of cpus for parallel processing, 0: single
                        process, >0: number of worker processes, <0: number of
                        workers equal to number of CPUs
  -b BUF_SIZE, --buf-size BUF_SIZE
                        number of lines buffered before the CSV parsing, just
                        a minor performance optimization

You can run `sh run.sh` to test the program
<pre>

