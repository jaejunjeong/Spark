# Fork the CLAIMED library from github
https://github.com/claimed-framework/component-library/tree/master/component-library

# Clone (download) the library  
%%bash
rm -Rf claimedㅁ
git clone https://github.com/IBM/claimed.git
cd claimed

# Pull the data from CLAIMED library
!ipython ./claimed/component-library/input/input-hmp.ipynb data_dir=./data/ sample=0.01

# Comvert CSV to PARQUET
!ipython ./claimed/component-library/transform/spark-csv-to-parquet.ipynb data_dir=./data/

# Condense parquet file
!ipython ./claimed/component-library/transform/spark-condense-parquet.ipynb data_dir=./data/

# Obtain access to Cloud Object Store (IBM Clud)
Bucket :cloud-object-storage-cos-standard-bav
Location :jp-tok
access_key_id: 
secret_access_key: 
endpoint : s3.jp-tok.cloud-object-storage.appdomain.cloud

# Upload file to Cloud Object Storage
%%bash
export access_key_id='access_key_id=<your_access_key_id_goes_here>'
export secret_access_key='secret_access_key=<your_secret_access_key_goes_here>'
export endpoint='endpoint=https://<your_endpoint_goes_here>'
export bucket_name='bucket_name=<your_cos_bucket_name_goes_here>'
export source_file='source_file=data_condensed.parquet'
export destination_file='destination_file=data.parquet'
export data_dir='data_dir=./data/'
ipython ./claimed/component-library/output/upload-to-cos.ipynb $access_key_id $secret_access_key $endpoint $bucket_name $source_file $destination_file $data_dir

