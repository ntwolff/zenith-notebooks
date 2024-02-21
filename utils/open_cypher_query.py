import boto3

class NeptuneCypherUtility:
    def __init__(self, cluster_endpoint, region_name):
        """
        Initializes the utility class with the necessary parameters to connect to an Amazon Neptune cluster.
        
        Parameters:
        - cluster_endpoint (str): Endpoint of the Neptune cluster.
        - region_name (str): AWS region where the Neptune cluster is located.
        """
        self.cluster_endpoint = cluster_endpoint
        self.region_name = region_name
        # Note: Adjust the endpoint_url if your actual Neptune access pattern is different.
        self.client = boto3.client('neptunedata', endpoint_url=f'https://{cluster_endpoint}:8182', region_name=region_name)

    def execute_query(self, query):
        """
        Executes an Open Cypher query on the Neptune cluster.
        
        Parameters:
        - query (str): The Open Cypher query to execute.
        """
        try:
            # Call the execute_open_cypher_query method with minimal parameters
            response = self.client.execute_open_cypher_query(
                openCypherQuery=query
            )
            # Use the correct key to access the query results
            # return response['results']
            return response
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    ###
    #SPECIFIC ALGOS
    #CALL not yet supported - https://docs.aws.amazon.com/neptune/latest/userguide/feature-opencypher-compliance.html
    ###
    
    def eval_label_propagation(self):
        """
        Executes a query to retrieve community data from the graph.
        """
        query = """
        MATCH (n)
        CALL neptune.algo.labelPropagation(n)
        YIELD community
        RETURN community, count(n) as size
        ORDER BY size DESC
        """
        
        print(query)
        
        return self.execute_query(query)

    def mutate_label_propagation(self, property_name):
        """
        Executes a query to persist community data to the graph using the label propagation algorithm.

        Parameters:
        - property_name (str): The property name where the community data will be written.
        """
        # Ensure the property name is correctly quoted in the query
        query = f"""
        CALL neptune.algo.labelPropagation.mutate({{writeProperty: '{property_name}'}})
        """
        return self.execute_query(query)

    def closeness_centrality(self, limit):
        """
        Executes the closeness centrality algorithm and limits the number of results returned.

        Parameters:
        - limit (int): The maximum number of results to return.
        """
        # Directly format the limit into the query, as it is a numerical value
        query = f"""
        MATCH (n) 
        CALL neptune.algo.closenessCentrality(n, {{numSources: 8192}})
        YIELD score
        RETURN n, score 
        ORDER BY score DESC LIMIT {limit}
        """
        return self.execute_query(query)