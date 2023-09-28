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


