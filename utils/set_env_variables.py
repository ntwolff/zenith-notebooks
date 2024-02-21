import os

# Function to set environment variables
def set_environment_variables():
    os.environ['NEPTUNE_ENDPOINT'] = 'neptunecdkclusterinstance1f-0c3kvhl8un1k.cvvkcjt8raco.us-east-2.neptune.amazonaws.com'
    os.environ['NEPTUNE_PORT'] = '8182'
    os.environ['NEPTUNE_AUTH_MODE'] = 'DEFAULT'
    os.environ['S3_ROLE_ARN'] = 'arn:aws:iam::299651194944:role/AWSNeptuneNotebookRole-CDK'
    os.environ['SSL'] = 'true'
    os.environ['SSL_VERIFY'] = 'true'
    os.environ['AWS_REGION'] = 'us-east-2'