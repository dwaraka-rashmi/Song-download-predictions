# Crystal Discoball

## Authors: Biswajeeth Dey, Rashmi Dwaraka

This project aims to predict the number of downloads for a given song primarily through the attributes provided by the million song dataset.
More details of the project can be found at http://janvitek.org/pdpmr/f17/project.html

# Pre-requisite packages:
1. Java (v1.8+)
2. Spark (v2.2.0 compiled with Scala 2.11.X and hadoop-2.7)
3. Scala (v2.11.X)
4. .Renviron should exist to generate the report in R

Build the project by typing command `make build`. Run the project locally by running `make run`

# Pre-requisites:
1. Update the variables SCALA_HOME and SPARK_HOME as per your configuration in Makefile.
2. The model can be loaded in memory by running the `make run` command. It assumes that you have a partition /mnt/pdpmr of size 100 MB or
  uses the /tmp as the scratch directory for loading the model.

# Project structure
1. The project has both java and scala files. `src/main/java` contains the java files. `src/main/scala` contains the scala files.
2. The RandomForest model is built using RFEngine. 
3. The loading/deployment of the model is part of the Model class.
