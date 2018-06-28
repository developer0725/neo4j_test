package org.ihar;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ProcIhar {

    private static final String POINT_LABEL = "Point";

    private static final String POINT_REL = "WAY_TO";

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
            for (String nodeName : nodeNames) {
                Node node = db.createNode(Label.label(POINT_LABEL));
                node.setProperty("name", nodeName);
                nodes.add(node);
            }

            Random rd = new Random();
            int idx1, idx2;
            Node node1, node2;
            for (int i = 0; i < nRel; i++) {
                idx1 = rd.nextInt(nodes.size());
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


    @Description("org.ihar.isThereCycles(nodeNames) | Check whether cycles exist in nodeNames")
    @Procedure(name = "org.ihar.isThereCycles", mode = Mode.READ)
    public Stream<BooleanResult> isThereCycles(@Name("nodeNames") List<String> nodeNames) {
        boolean result = false;
        try (Transaction tx = db.beginTx()) {
            ArrayList<String> nNames = new ArrayList<String>();
            TraversalDescription td = db.traversalDescription().depthFirst()
                    .relationships(RelationshipType.withName(POINT_REL), Direction.OUTGOING)
                    .evaluator(new Evaluator() {
                        @Override
                        public Evaluation evaluate(Path path) {
                            if (nNames.contains(path.endNode().getProperty("name"))) {

                                return Evaluation.INCLUDE_AND_PRUNE;
                            }
                            nNames.add((String) path.endNode().getProperty("name"));
                            return Evaluation.EXCLUDE_AND_CONTINUE;
                        }
                    }).uniqueness(Uniqueness.NODE_PATH);
            for (String nodeName : nodeNames) {
                if (!nNames.contains(nodeName) && td.traverse(db.findNode(Label.label(POINT_LABEL), "name", nodeName)).nodes().iterator().hasNext()) {
                    result = true;
                }
            }

            tx.success();
        }

        return Stream.of(new BooleanResult(result));
    }

    @Description("org.ihar.isThereCyclesByQuery(nodeNames) | Check whether cycles exist in nodeNames")
    @Procedure(name = "org.ihar.isThereCyclesByQuery", mode = Mode.READ)
    public Stream<BooleanResult> isThereCyclesByQuery(@Name("nodeNames") List<String> nodeNames) {
        boolean result;
        ResourceIterator<Node> resultIterator;
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("label", POINT_LABEL);
            parameters.put("rel", POINT_REL);
            parameters.put("nodeNames", nodeNames);
            resultIterator = db.execute(QUERY, parameters).columnAs("p");
            result = resultIterator != null && resultIterator.hasNext();
            tx.success();
        }
        return Stream.of(new BooleanResult(result));
    }

    private static final String QUERY = "MATCH path=(p:$label)-[$rel*]->(p)\n" +
            "WHERE ALL(node_on_path in NODES(path) \n" +
            "  WHERE node_on_path.name IN $nodeNames) \n" +
            "  return p";

    public static class BooleanResult {
        public boolean iscycle;

        BooleanResult(boolean iscycle) {
            this.iscycle = iscycle;
        }
    }
}
