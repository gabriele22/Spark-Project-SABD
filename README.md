# Spark-Project-SABD

The goal of this project is to answer the queries in the document: <https://drive.google.com/file/d/1mVxpYWk_OQdDZBCSqUck_pTQ8WwbEC21/view?usp=sharing>.
Furthermore it is possible to create a standalone cluster with docker and docker-compose to test everything.

## Pre requisites
You need to have installed:
* docker
* docker-compose

## Build and Run all required containers
First download and unzip the file: https://drive.google.com/file/d/1K19JKYNCa3cac_eyXBDJmuwDZ31gAzFk/view?usp=sharing.
In the directory created, open a terminal and execute this command:
```bash
sh start.sh
```
## Usage
Execute this command for the first query: 
```bash
sh submitQuery1.sh
```
Execute this command for third query:
```bash
sh submitQuery3.sh
```

To access the spark UI copy this link on your browser: 
<http://10.5.0.2:8080/> 

## Stop all container and remove temp file
```bash
sh stopAndClean.sh
```
