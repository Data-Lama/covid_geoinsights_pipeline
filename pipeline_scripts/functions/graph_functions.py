# Graph Functions
import pandas as pd
import numpy as np
import igraph as ig



def get_personalized_ppr(nodes, edges, weighted_edges = False):
    '''
    Method for computing the personlized pagerank for the given nodes
    '''


    # Create the graph
    G = ig.Graph()

    # Adds the values
    G.add_vertices(nodes.identifier.values)        
                
    if edges.shape[0] > 0:
        G.add_edges(edges.apply(lambda df: (df.id1, df.id2), axis = 1))

    # Reset weights cannot be all zeros
    if nodes.weight.sum() == 0:
        nodes['weight'] = 1
    
    # Checks if weighted
    if weighted_edges:
        G.es['weight'] = edges.weight.values        
        personalized_page_rank = G.personalized_pagerank(weights = 'weight', directed = False, reset = nodes['weight'].values)

    else:
        personalized_page_rank = G.personalized_pagerank(directed = False, reset = nodes['weight'].values)
    

    return personalized_page_rank