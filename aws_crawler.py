from pprint import pprint
import boto3

__TableName__ = "Music_cow"

client = boto3.client('dynamodb')

DB = boto3.resource('dynamodb')
table = DB.Table(__TableName__)

def html_aws()


Primary_Key = 1

response = table.put_item(
    Item = {}
)