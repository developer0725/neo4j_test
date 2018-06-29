package org.ihar;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ProcIharTest {

    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withProcedure(ProcIhar.class);

    @Test
    public void createNodesAndRelations() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run(QUERY10);
            Record result1 = session.run(QUERY11).single();
            Long relNum = result1.get("relNum").asLong();

            session.run(QUERY10);
            result1 = session.run(QUERY11).single();//number of created nodes,number of created relations
            assertEquals(result1.get("pNum").asLong(), 5L);
            assertEquals(result1.get("relNum").asLong() - relNum, 4L);
        }
    }

    @Test
    public void isThereCycles() {

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {
            StatementResult result0 = session.run(QUERY20);
            StatementResult result1 = session.run(QUERY21);
            StatementResult result2 = session.run(QUERY22);

            assertThat(result0.single().get("iscycle").asBoolean(), equalTo(true));
            assertThat(result1.single().get("iscycle").asBoolean(), equalTo(false));
            assertThat(result2.single().get("iscycle").asBoolean(), equalTo(true));
        }
    }

    @Test
    public void isThereCyclesByQuery() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {
            StatementResult result0 = session.run(QUERY30);
            StatementResult result1 = session.run(QUERY31);
            StatementResult result2 = session.run(QUERY32);

            assertThat(result0.single().get("iscycle").asBoolean(), equalTo(true));
            assertThat(result1.single().get("iscycle").asBoolean(), equalTo(false));
            assertThat(result2.single().get("iscycle").asBoolean(), equalTo(true));
        }
    }

    private static final String QUERY10 = "CALL org.ihar.createNodesAndRelations(['a7','a8','a9','a10','a11'],4)";
    private static final String QUERY11 = "MATCH (p:Point),(a:Point)-[r]-(aa:Point) " +
            "WHERE p.name IN ['a7','a8','a9','a10','a11'] AND a.name IN ['a7','a8','a9','a10','a11'] AND aa.name IN ['a7','a8','a9','a10','a11'] " +
            "RETURN COUNT(DISTINCT p) AS pNum,COUNT(DISTINCT r) AS relNum";

    private static final String QUERY20 = "CALL org.ihar.isThereCycles(['a1','a2','a3']) yield iscycle";
    private static final String QUERY21 = "CALL org.ihar.isThereCycles(['a1','a4','a5','a6']) yield iscycle";
    private static final String QUERY22 = "CALL org.ihar.isThereCycles(['a1','a2','a4','a5','a6']) yield iscycle";

    private static final String QUERY30 = "CALL org.ihar.isThereCyclesByQuery(['a1','a2','a3','a4']) yield iscycle";
    private static final String QUERY31 = "CALL org.ihar.isThereCyclesByQuery(['a1','a2','a4','a6']) yield iscycle";
    private static final String QUERY32 = "CALL org.ihar.isThereCyclesByQuery(['a1','a2','a3','a4','a5','a6']) yield iscycle";

    private static final String MODEL_STATEMENT =
            // (a1)-->(a2)-->(a3)-->(a1)
            // (a2)-->(a4)-->(a5)-->(a1)
            // (a3)-->(a5)
            // (a6)
            "CREATE (a1:Point {name:'a1'})" +
                    "CREATE (a2:Point {name:'a2'})" +
                    "CREATE (a3:Point {name:'a3'})" +
                    "CREATE (a4:Point {name:'a4'})" +
                    "CREATE (a5:Point {name:'a5'})" +
                    "CREATE (a6:Point {name:'a6'})" +
                    "CREATE (a1)-[:WAY_TO]->(a2)-[:WAY_TO]->(a3)-[:WAY_TO]->(a1)" +
                    "CREATE (a3)-[:WAY_TO]->(a5)" +
                    "CREATE (a2)-[:WAY_TO]->(a4)-[:WAY_TO]->(a5)-[:WAY_TO]->(a1)";
}