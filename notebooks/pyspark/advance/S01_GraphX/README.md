# Introduction of Graph

The 

## Tools to work with graph data

We can divide the existing tools into three categories when we want to work with graph data:

 - Graph Databases
 - Computation engine for Graph Data Analytics
 - Graph Data Visualization


### Graph Databases

This kind of tools provides an all-in-one solution. It provides a database backend to store the graph (often in private
format). It often provides a Computation engine and query language to do the `Graph Data Analytics`. And you have a 
user interface to visualize the graph. 

The disadvantage of this kind of solution is that it's hard to handle big data with distributed servers

Existing tools:  
- [Neo4j](https://neo4j.com/product/): The cluster is for HA, not for improving calculation speed. and Require licence for cluster mode
- [DataStax Enterprise Graph](https://www.datastax.com/products/datastax-enterprise-graph): Require licence
- [AllegroGraph](https://allegrograph.com/) : has free version
- [InfiniteGraph](https://objectivity.com/infinitegraph/): web app
- [OrientDB](https://orientdb.org/): Require Licence

### Computation engine for Graph Data Analytics

This kind of tools provides below functionalities:
- pre-processing of data (which includes loading, transformation, and filtering)
- graph creation
- analysis
- post-processing (export graph data to portable format such as csv, parquet, etc.)

The disadvantage is that they do not provide visualization tools

Existing tools:
 - Spark GraphX
 - Apache Flink's Gelly
 - GraphLab: dead no more support

### Graph Data Visualization

Once we start storing connected data in a graph database and run analytics on the graph data, we need tools to 
visualize the patterns behind the relationships between the data entities.

Existing tools: 
  - D3.js, 
  - Linkurious
  - [GraphLab Canvas](https://github.com/apple/turicreate/). 

> Data analytics efforts are not complete without data visualization tools.


## Graph Use Cases

There are a variety of use cases where graph databases are better fit to manage the data than other solutions 
like relational databases or other NoSQL data stores. Some of these use cases include the following:

- **Recommendations and Personalization**: Graph analysis can be used to generate recommendation and 
         personalization models on their customers and to make key decisions from the insights found in the 
         data analysis. This helps the enterprises to effectively influence customers to purchase their product. 
         This analysis also helps with marketing strategy and customer service behavior.
- **Fraud Detection**: Graph data solutions also help to find fraudulent transactions in a payment processing 
        application based on the connected data that include the entities like users, products, transactions, 
        and events. 
- **Topic Modeling**: This includes techniques to cluster documents and extract topical representations from 
           the data in those documents.
- **Community Detection**: Alibaba website uses graph data analytics techniques like community detection to solve 
          ecommerce problems.
- **Flight Performance**: Other use cases include on-time flight performance as discussed in this [article](https://www.databricks.com/blog/2016/03/16/on-time-flight-performance-with-graphframes-for-apache-spark.html), to 
         analyze flight performance data organized in graph structures and find out statistics like airport ranking 
         and shortest paths between cities.
- **Shortest Distance**: Shortest distances and paths are also useful in social network applications. 
         They can be used for measuring the relevance of a particular user in the network. Users with smaller 
         the shortest distances are more relevant than users farther away.


## Graph Analytics Algorithms

Graph algorithms help with executing the analytics on graph data sets without having to write our own implementations of those algorithms. Below is a list of various algorithms that help with finding patterns in the graphs.

- PageRank
- Connected components (Strongly connected components)
- Label propagation
- SVD++ (Singular Value Decomposition)
- Triangle count (Community Detection)
- Single-Source-Shortest-Paths


### PageRank

**PageRank algorithm is used to determine the relative importance of an object inside a graph data set**. It 
measures the importance of each node in a graph, assuming an edge from another node to this node represents an 
endorsement.

Google's search engine is a classic example of PageRank. Google uses PageRank as one of the measures to determine 
the importance of a web page based on how many other web pages reference it.

Another example is social network website like Twitter. If a Twitter user is followed by lot of other users, then 
that user has a higher influence in the network. This metric can be used for ad selection/placement to the users 
that follow the first user (100,000 users follow a chef=> probably food lovers)

GraphX provides two implementations of PageRank: 
 - Static PageRank: This algorithm runs for a fixed number of iterations to generate PageRank values for a given 
                    set of nodes in a graph data set.
 - Dynamic PageRank: On the other hand, Dynamic PageRank algorithm runs until PageRank values converge 
                      based on a pre-defined tolerance value.

### Connected Components

A Connected Component in a graph is a connected subgraph where two vertices are connected to each other by an edge 
and there are no additional vertices in the main graph. This means the two nodes belong to the same connected 
component when there is a relationship between them. The lowest numbered vertex number of ID in the subgraph is 
used to label the connected components in a graph. Connected components can be used to create clusters in the 
graph for example in a social network.

There are two ways of traversing the graph for computing connected components:
 - Breadth-first Search [BFS](https://en.wikipedia.org/wiki/Breadth-first_search)
 - Depth-first Search [DFS](https://en.wikipedia.org/wiki/Depth-first_search)

> There is another algorithm called Strongly Connected Components (SCC) in graph data processing. If all nodes in 
a graph are reachable from every single node, then the graph is considered to be strongly connected.


### Triangle Counting

Triangle counting is a community detection graph algorithm which is used to determine the number of triangles passing 
through each vertex in the graph data set. A vertex is part of a triangle when it has two adjacent vertices with an 
edge between. The triangle is a three-node subgraph, where every two nodes are connected. This algorithm returns a 
Graph object and we extract vertices from this triangle counting graph.

Triangle counting is used heavily in social network analysis. It provides a measure of clustering in the graph 
data which is useful for finding communities and measuring the cohesiveness of local communities in social network 
websites like LinkedIn or Facebook. Clustering Coefficient, an important metric in a social network, shows how 
much community around one node is tightly connected.

Other use cases where Triangle Counting algorithm is used are spam detection and link recommendations.

> Triangle counting is a message heavy and computationally expensive algorithm compared to other graph algorithms. So, 
make sure you run the Spark program on a decent computer when you test Triangle Count algorithm. Note that PageRank 
is a measure of relevancy whereas Triangle Count is a measure of clustering.

