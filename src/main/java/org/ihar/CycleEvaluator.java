package org.ihar;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.PathEvaluator;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CycleEvaluator implements PathEvaluator {

    private List<String> nodeNames;//inputed nodes

    public CycleEvaluator(List<String> nodeNames) {
        this.nodeNames = nodeNames;
    }

    @Override
    public Evaluation evaluate(Path path, BranchState branchState) {
        if (nodeNames.contains(path.endNode().getProperty("name"))) {
            AtomicInteger nodeCount = new AtomicInteger();
            path.nodes().forEach(node -> {
                if (node.equals(path.endNode())) {
                    nodeCount.getAndIncrement();
                }
            });
            if (nodeCount.intValue() > 1) {
                return Evaluation.INCLUDE_AND_PRUNE;//cycle path(number of same node:2)
            } else {
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
        }
        return Evaluation.EXCLUDE_AND_PRUNE;//not inputed node
    }

    @Override
    public Evaluation evaluate(Path path) {
        return null;
    }
}
