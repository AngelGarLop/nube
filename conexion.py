import boto3
from dotenv import load_dotenv
import os

load_dotenv(override=True)

class ConectorAWS():
   def conectarse(self):
      client = boto3.resource(
         'ec2',
         aws_access_key_id=os.getenv("aws_access_key_id"),
         aws_secret_access_key=os.getenv("aws_secret_access_key"),
         aws_session_token=os.getenv("aws_session_token"),
         region_name=os.getenv("REGION")
      )
      return client
    
   def conectarse_client(self):
      client = boto3.client(
         'ec2',
         aws_access_key_id=os.getenv("aws_access_key_id"),
         aws_secret_access_key=os.getenv("aws_secret_access_key"),
         aws_session_token=os.getenv("aws_session_token"),
         region_name=os.getenv("REGION")
      )
      return client
    
   def conectarse_efs(self):
      client = boto3.client(
         'efs',
         aws_access_key_id=os.getenv("aws_access_key_id"),
         aws_secret_access_key=os.getenv("aws_secret_access_key"),
         aws_session_token=os.getenv("aws_session_token"),
         region_name=os.getenv("REGION")
      )
      return client
   
   def conectarse_s3(self):
      client = boto3.client(
         's3',
         aws_access_key_id=os.getenv("aws_access_key_id"),
         aws_secret_access_key=os.getenv("aws_secret_access_key"),
         aws_session_token=os.getenv("aws_session_token"),
         region_name=os.getenv("REGION")
      )
      return client
   
   def conectarse_athena(self):
      client = boto3.client(
         'athena',
         aws_access_key_id=os.getenv("aws_access_key_id"),
         aws_secret_access_key=os.getenv("aws_secret_access_key"),
         aws_session_token=os.getenv("aws_session_token"),
         region_name=os.getenv("REGION")
      )
      return client