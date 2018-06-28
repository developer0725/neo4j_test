package org.ihar;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BranchState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CycleExpander implements PathExpander {

    private List<String> nodeNames;//inputed nodes

    private List<String> checkedNodes = new ArrayList<String>();//the expanded nodes

    public CycleExpander(List<String> nodeNames) {
        this.nodeNames = nodeNames;
    }

    @Override
    public Iterable<Relationship> expand(Path path, BranchState branchState) {
        if (!nodeNames.contains(path.endNode().getProperty("name"))) {
            return Collections.emptyList();
        }
        checkedNodes.add((String) path.endNode().getProperty("name"));
        return path.endNode().getRelationships(Direction.OUTGOING);
    }

    @Override
    public PathExpander reverse() {
        return null;
    }

    public List<String> getCheckedNodes() {
        return checkedNodes;
    }

}
