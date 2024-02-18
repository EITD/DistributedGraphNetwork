import networkx as nx

class ConvertFile:
        """
        A class providing methods to convert a file to a NetworkX graph.
        """
        
        @staticmethod
        def toGraph(filePath, d, weight = False):
                """
                Converts a file into a NetworkX graph.

                Args:
                - filePath (str): The path to the input file.
                - d (str): The delimiter used in the file.
                - weight (bool): Determines if the graph is weighted (default: False).

                Returns:
                - graph (networkx.Graph): The NetworkX graph converted from the file.
                """
                return nx.read_weighted_edgelist(filePath, delimiter = d) if weight else nx.read_edgelist(filePath, create_using=nx.DiGraph(), delimiter = d)