# cmd
# cd C:/dynamodb_local_latest
# java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb

# cmd
# cd C:/dynamodb_local_latest
# aws dynamodb list-tables --endpoint-url http://localhost:8000

import os

os.chdir('C:/dynamodb_local_latest')
os.system('java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb')

os.chdir('C:/dynamodb_local_latest')
os.system('aws dynamodb list-tables --endpoint-url http://localhost:8000')
