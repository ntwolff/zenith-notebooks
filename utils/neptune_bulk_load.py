import boto3
import requests  # To call the Neptune Loader API

def bulk_load_neptune(neptune_cluster_id, neptune_endpoint, neptune_port, s3_data_path, s3_data_format, role_arn, region):
    """
    Initiates an asynchronous bulk load process to load data from an S3 bucket into an Amazon Neptune cluster.
    Checks if the IAM role is already attached to the Neptune cluster before attaching it.
    
    Parameters:
    - neptune_cluster_id (str): Identifier of the Neptune cluster.
    - neptune_endpoint (str): Endpoint of the Neptune cluster.
    - neptune_port (str): Port of the Neptune cluster.
    - s3_data_path (str): The S3 bucket URI where the data is stored.
    - s3_data_format (str): The format of the file in the S3 bucket.
    - role_arn (str): The IAM role ARN with permissions for Neptune to access the S3 data.
    - region (str): AWS region where the Neptune cluster and S3 bucket are located.
    """
    
    client = boto3.client('neptune', region_name=region)
    
    # Check if the IAM role is already attached to the DB cluster
    try:
        clusters_info = client.describe_db_clusters(DBClusterIdentifier=neptune_cluster_id)
        for cluster in clusters_info['DBClusters']:
            for role in cluster['AssociatedRoles']:
                if role['RoleArn'] == role_arn:
                    print(f"Role {role_arn} is already associated with the cluster {neptune_cluster_id}.")
                    break
            else:
                # If the role is not found, attach the role
                attach_iam_response = client.add_role_to_db_cluster(
                    DBClusterIdentifier=neptune_cluster_id,
                    RoleArn=role_arn
                )
                print("Role attached successfully.")
                print(attach_iam_response)
                break
    except client.exceptions.DBClusterNotFoundFault:
        print(f"DB cluster {neptune_cluster_id} not found.")
        return
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return
    
    load_url = f'https://{neptune_endpoint}:{neptune_port}/loader'

    # Initialize and start the bulk load process
    params = {
        'source': s3_data_path,
        'format': s3_data_format,  # 'csv', 'turtle', 'rdfxml', 'ntriples', 'nquads', 'json'
        'iamRoleArn': role_arn,
        'region': region,
        'failOnError': 'TRUE',
        'parallelism': 'MEDIUM'
    }

    # Send the load request to Neptune
    response = requests.post(load_url, json=params)

    # Check the response
    if response.status_code == 200:
        print("Load request submitted successfully.")
        print(response.json())
    else:
        print("Failed to submit load request.")
        print(response.text)
