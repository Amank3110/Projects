import configparser
import csv
import pandas as pd
import boto3
import requests
import json
import os

class Utilities:
    def __init__(self):
        """Read config1.ini and set up S3 resource"""
        config = configparser.ConfigParser()
        config.read('config1.ini')
        self.url = config['config']['url']
        self.s3_bucket_name = config['aws']['s3_bucket_name']
        self.aws_default_region = config['aws']['aws_default_region']
        
        # Set up S3 resource with credentials
        self.s3_resource = boto3.resource('s3', 
                                          region_name=self.aws_default_region, 
                                          aws_access_key_id=os.getenv('AWS_ACCESS_KEY'), 
                                          aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
        
    def request_api_data(self):
        """Send HTTP GET request to API and store response"""
        response = requests.get(self.url)
        self.data = response.json()
        
    def json_to_csv(self):
        """Convert JSON data to CSV format"""
        self.request_api_data()
        df = pd.json_normalize(self.data)
        self.csv_buffer = df.to_csv(index=False).encode('utf-8')
        
    def upload_to_s3(self):
        """Upload CSV data to S3 bucket"""
        self.json_to_csv()
        key = 'client_info/data.csv'
        self.s3_resource.Object(self.s3_bucket_name, key).put(Body=self.csv_buffer)
        
if __name__ == '__main__':
    # Set up S3 resource with AWS credentials from environment variables
    Utilities()
