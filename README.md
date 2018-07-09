# neo4j_test
neo4j procedures for creating node &amp; relationship, detecting cycle
///////////////////////////////////////////////////////////////////////////////////////////////////
createNodesAndRelations(<node_names_array>, N)

<node_names_array> - the list of nodes to create.
N - the number of directional relations to create between random nodes.
 
///////////////////////////////////////////////////////////////////////////////////////////////////
boolean isThereCycles(<node_names_array>)

should return is there any relation cycles for nodes specified. 
For example,
createNodesAndRelations(["A", "B", "C", "D"], 5), should create 4 nodes with names A-D, and 5 directional relationships between nodes in random way. 
And if there is any cycle path created, isThereCycles(["A", "B", "C", "D"]) return true, false otherwise. Cycle path means A->B->C->A for example.
