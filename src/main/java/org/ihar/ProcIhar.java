package org.ihar;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

public class ProcIhar {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;
    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @Description("org.ihar.createNodesAndRelations(nodeNames, nRel) | Create nodes with nodeNames , nRel relations between random nodes")
    @Procedure(name = "org.ihar.createNodesAndRelations", mode = Mode.WRITE)
    public void createNodesAndRelations(@Name("nodeNames") List<String> nodeNames,
                                        @Name("nRel") long nRel) {
        try (Transaction tx = db.beginTx()) {
            List<Node> nodes = new ArrayList<Node>();
            //create nodes by nodeNames
            for (String nodeName : nodeNames) {
                Node node = db.findNode(Label.label(POINT_LABEL), "name", nodeName);
                if (node == null) {
                    node = db.createNode(Label.label(POINT_LABEL));
                    node.setProperty("name", nodeName);

                }
                nodes.add(node);
            }

            //create relations between random nodes
            Random rd = new Random();
            int idx1, idx2;
            Node node1, node2;
            for (int i = 0; i < nRel; i++) {
                idx1 = rd.nextInt(nodes.size());
                //index for another node
                do {
                    idx2 = rd.nextInt(nodes.size());
                } while (idx1 == idx2);
                node1 = nodes.get(idx1);
                node2 = nodes.get(idx2);
                node1.createRelationshipTo(node2, RelationshipType.withName(POINT_REL));
            }
            tx.success();
        }
    }

    //detect cycle in nodes(by nodeNames) using traversal framework
    @Description("org.ihar.isThereCycles(nodeNames) | Check whether cycles exist in nodeNames")
    @Procedure(name = "org.ihar.isThereCycles", mode = Mode.READ)
    public Stream<BooleanResult> isThereCycles(@Name("nodeNames") List<String> nodeNames) {
        boolean result = false;
        //use special evaluator and expander
        cycleEvaluator = new CycleEvaluator(nodeNames);
        cycleExpander = new CycleExpander(nodeNames);
        try (Transaction tx = db.beginTx()) {
            TraversalDescription td = db.traversalDescription().depthFirst()
                    .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                    .expand(cycleExpander)
                    .evaluator(cycleEvaluator);
            //set start node
            for (int i = 0; i < nodeNames.size() && !result; i++) {
                //skip nodes which is visited  and checked relations by CycleEvaluator, CycleExpander
                if (!cycleExpander.getCheckedNodes().contains(nodeNames.get(i))) {
                    Traverser traverser = td.traverse(db.findNode(Label.label(POINT_LABEL), "name", nodeNames.get(i)));
                    if (traverser.nodes().iterator().hasNext()) {
                        result = true;
                    }
                }
            }
            tx.success();
        }
        return Stream.of(new BooleanResult(result));
    }

    //detect cycle in nodes(by nodeNames) using cypher query
    @Description("org.ihar.isThereCyclesByQuery(nodeNames) | Check whether cycles exist in nodeNames")
    @Procedure(name = "org.ihar.isThereCyclesByQuery", mode = Mode.READ)
    public Stream<BooleanResult> isThereCyclesByQuery(@Name("nodeNames") List<String> nodeNames) {
        boolean result;
        ResourceIterator<Node> resultIterator;
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("nodeNames", nodeNames);
            resultIterator = db.execute(QUERY, parameters).columnAs("p");
            result = resultIterator != null && resultIterator.hasNext();
            tx.success();
        }
        return Stream.of(new BooleanResult(result));
    }

    public static class BooleanResult {
        public boolean iscycle;

        BooleanResult(boolean iscycle) {
            this.iscycle = iscycle;
        }
    }

    private static final String POINT_LABEL = "Point";

    private static final String POINT_REL = "WAY_TO";

    //cypher query for cycle path
    private static final String QUERY = "MATCH path=(p:" + POINT_LABEL + ")-[" + POINT_REL + "*]->(p)\n" +
            "WHERE ALL(node_on_path in NODES(path) \n" +
            "  WHERE node_on_path.name IN $nodeNames) \n" +
            "  return p";

    //evaluator for cycle path
    private static CycleEvaluator cycleEvaluator;

    //expander for cycle path
    private static CycleExpander cycleExpander;
}
