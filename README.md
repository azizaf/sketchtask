I created a Postgresql database on RDS with the necessary credentials
Created a connection to the database using psycopg2
using AWS cli i created 2 buckets legacy-s3-aziz-123 for the legacy images and prods3-aziz-123 for production 

i uploaded 3 images to the legacy bucket and created a path for them in postgres image/*.png
uploaded 1 image in production bucket and created a path for it  in postgres avatar/*.png

Defined a function with sql query to get all paths beginning with image and check them against the legacy bucket if the images are actually available in the bucket then it checks if they are available in production bucket and if not it then uses the copy function to copy all the images that are available in legacy to production then updates their paths in database 


for high performance i used s3 client copy() which will support a multipart copy in multiple threads  if necessary 
