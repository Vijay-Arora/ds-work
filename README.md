# Tech Stack Used

1. GIT
2. Databricks
3. SQL
4. Python
5. Pyspark
6. AWS S3

# Steps To Run

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

# Note 

1. The other way to perform the same action is to directly configure databricks to use github via github access token but I've just stated the other way so that if you want to do it for one time you won't need to use github access tokens.
2. I've placed the input file in AWS s3 bucket from where I'm reading the file and I've used the same path to write the output files as well. So, I'm not sure if while running the script you'd be able to run it without the errors while accessing the AWS locations. I've never tried to give access to someone outside the organization.
