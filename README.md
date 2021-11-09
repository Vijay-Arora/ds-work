# Tech Stack Used

1. GIT
2. Databricks
3. SQL
4. Python
5. Pyspark
6. AWS S3

# Steps to run 

1. Download the main.py file 
2. Import it to databricks workspace
3. Create a cluster (if not created earlier)
     - Pass the cluster name 
     - Select the databricks runtime version on which you want to run the notebooks
     - Un check auto scaling as the data we're dealing with isn't that large
     - We can skip all the other parameters for now and finally,
     - Click on create cluster

4. Assuming that the cluster is created and the notebook is imported - attach the cluster to the notebook 
5. Start the cluster 
6. Click on "Run all" to run the notebook
