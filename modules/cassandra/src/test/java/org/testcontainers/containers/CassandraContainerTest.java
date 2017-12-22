package org.testcontainers.containers;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import junit.framework.AssertionFailedError;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.CassandraQueryWaitStrategy;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Eugeny Karpov
 */
public class CassandraContainerTest {

    private static final Logger logger = LoggerFactory.getLogger(CassandraContainerTest.class);
    private static final String TEST_CLUSTER_NAME_IN_CONF = "Test Cluster Integration Test";

    @Test
    public void testSimple() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer()
                .withLogConsumer(new Slf4jLogConsumer(logger));
        performWithContainer(cassandraContainer, () -> {
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT release_version FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
            assertNotNull("Result set has no release_version", resultSet.one().getString(0));
        });
    }

    @Test
    public void testSpecificVersion() throws Exception {
        String cassandraVersion = "3.0.15";
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer("cassandra:" + cassandraVersion)
                .withLogConsumer(new Slf4jLogConsumer(logger));
        performWithContainer(cassandraContainer, () -> {
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT release_version FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
            assertEquals("Cassandra has wrong version", cassandraVersion, resultSet.one().getString(0));
        });
    }

    @Test
    public void testConfigurationOverride() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer()
                .withConfigurationOverride("cassandra-test-configuration-example")
                .withLogConsumer(new Slf4jLogConsumer(logger));
        performWithContainer(cassandraContainer, () -> {
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT cluster_name FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
            assertEquals("Cassandra configuration is not overridden", TEST_CLUSTER_NAME_IN_CONF, resultSet.one().getString(0));
        });
    }

    @Test(expected = ContainerLaunchException.class)
    public void testEmptyConfigurationOverride() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer()
                .withConfigurationOverride("cassandra-empty-configuration")
                .withLogConsumer(new Slf4jLogConsumer(logger));
        performWithContainer(cassandraContainer, () -> {
        });
    }

    @Test
    public void testInitScript() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer()
                .withInitScript("initial.cql")
                .withLogConsumer(new Slf4jLogConsumer(logger));
        testInitScript(cassandraContainer);
    }

    @Test
    public void testInitScriptWithLegacyCassandra() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer("cassandra:2.2.11")
                .withInitScript("initial.cql")
                .withLogConsumer(new Slf4jLogConsumer(logger));
        testInitScript(cassandraContainer);
    }

    @Test
    public void testCassandraQueryWaitStrategy() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer()
                .waitingFor(new CassandraQueryWaitStrategy())
                .withLogConsumer(new Slf4jLogConsumer(logger));
        performWithContainer(cassandraContainer, () -> {
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT release_version FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
        });
    }

    private void testInitScript(CassandraContainer cassandraContainer) {
        performWithContainer(cassandraContainer, () -> {
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT * FROM IgniteTest.catalog_category");
            assertTrue("Query was not applied", resultSet.wasApplied());
            Row row = resultSet.one();
            assertEquals("Inserted row is not in expected state", 1, row.getLong(0));
            assertEquals("Inserted row is not in expected state", "test_category", row.getString(1));
        });
    }

    private void performWithContainer(CassandraContainer cassandraContainer, Runnable runnable) {
        cassandraContainer.start();
        try {
            runnable.run();
        } finally {
            cassandraContainer.stop();
        }
    }

    private ResultSet performQuery(CassandraContainer cassandraContainer, String cql) {
        try (Cluster cluster = Cluster.builder()
                .addContactPoint(cassandraContainer.getContainerIpAddress())
                .withPort(cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT))
                .build()) {
            Session session = cluster.newSession();

            return session.execute(cql);
        }
    }
}
