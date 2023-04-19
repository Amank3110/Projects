import configparser
import csv
import pandas as pd
import boto3
import requests
import json
import io
from datetime import datetime

class utilities():
    def __init__(self):
        """read config1.ini and load on s3"""
        try:
            config = configparser.ConfigParser()
            config.read('configFile.ini')
            self.url = config['config']['url']
            self.s3_bucket_name = config['aws']['s3_bucket_name']
            self.aws_default_region = config['aws']['aws_default_region']
            self.aws_access_key = config['aws']['aws_access_key']
            self.aws_secret_access_key = config['aws']['aws_secret_access_key']
            self.s3_client = boto3.client('s3', aws_access_key_id=self.aws_access_key,
                                     aws_secret_access_key=self.aws_secret_access_key,
                                     region_name=self.aws_default_region)
        except Exception as e:
            print(f'Error Found: {e}')
            raise

    def change_date_format(self, date_string, from_format, to_format):
        """change date format by taking params"""
        try:
            date_obj = datetime.strptime(date_string, from_format)
            new_date_string = datetime.strftime(date_obj, to_format)
            return new_date_string
        except Exception as e:
            print(f'Error Found: {e}')
            raise

    def RequestAPIData(self):
        """request data from API/URL"""
        try:
            self.response = requests.get(self.url)
            if self.response.status_code == 200:
                content_type = self.response.headers['Content-Type']
                # check Data Format (csv or Json)
                if 'json' in content_type:
                    data = self.response.json()
                    self.df = pd.json_normalize(data)
                    self.df['DATE'] = self.df['DATE'].apply(
                        lambda x: self.change_date_format(x, '%d/%m/%Y', '%Y-%m-%d'))
                    return self.df
                else:
                    data = self.response.content.decode('utf-8')
                    self.df = pd.read_csv(io.StringIO(data))
                    self.df['DATE'] = self.df['DATE'].apply(
                        lambda x: self.change_date_format(x, '%d/%m/%Y', '%Y-%m-%d'))
                    return self.df
            else:
                print("Error: API response status code is not 200.")
                return None
        except Exception as e:
            print(f'Error Found: {e}')
            raise

    def splitByDate_UploadTos3(self):
        """split files according to Date Column"""
        try:
            df = self.RequestAPIData()
            if df is None:
                return
            unique_dates = df['DATE'].unique()
            for date in unique_dates:
                fileName = f'data_{date}'
                filtered_df = df[df['DATE'] == date]
                csv_file = filtered_df.to_csv(index=False).encode('utf-8')
                # Upload split files to s3
                self.s3_client.put_object(Bucket=self.s3_bucket_name, Key=f'client_info/{fileName}.csv', Body=csv_file)
        except Exception as e:
            print(f'Error Found: {e}')
            raise
    def UploadToS3(self):
        self.splitByDate_UploadTos3()
val = utilities()
val.UploadToS3()
