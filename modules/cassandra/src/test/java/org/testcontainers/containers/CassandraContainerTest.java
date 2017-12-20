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
    private static final String TEST_STRING_IN_CONF = "._/test_string\\_.";

    @Test
    public void testSimple() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer()
                .withLogConsumer(new Slf4jLogConsumer(logger));
        cassandraContainer.start();
        try {
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT release_version FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
            assertNotNull("Result set has no release_version", resultSet.one().getString(0));
        } finally {
            cassandraContainer.stop();
        }
    }

    @Test
    public void testSpecificVersion() throws Exception {
        String cassandraVersion = "3.0.15";
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer("cassandra:" + cassandraVersion)
                .withLogConsumer(new Slf4jLogConsumer(logger));
        cassandraContainer.start();
        try {
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT release_version FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
            assertEquals("Cassandra has wrong version", cassandraVersion, resultSet.one().getString(0));
        } finally {
            cassandraContainer.stop();
        }
    }

    @Test
    public void testConfigurationOverride() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer()
                .withConfigurationOverride("cassandra-test-configuration-example")
                .withLogConsumer(new Slf4jLogConsumer(logger));
        cassandraContainer.start();
        try {
            Container.ExecResult execResult = cassandraContainer.execInContainer("cat", "/etc/cassandra/cassandra.yaml");
            assertTrue("Cassandra configuration is not overridden", execResult.getStdout().contains(TEST_STRING_IN_CONF));
        } catch (IOException | InterruptedException e) {
            throw new AssertionFailedError("Could not check cassandra configuration");
        } finally {
            cassandraContainer.stop();
        }
    }

    @Test
    public void testInitScript() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer()
                .withInitScript("initial.cql")
                .withLogConsumer(new Slf4jLogConsumer(logger));
        assertNotNull("is null", cassandraContainer);
        testInitScript(cassandraContainer);
    }

    @Test
    public void testInitScriptWithLegacyCassandra() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer("cassandra:2.2.11")
                .withInitScript("initial.cql")
                .withLogConsumer(new Slf4jLogConsumer(logger));
        assertNotNull("is null", cassandraContainer);
        testInitScript(cassandraContainer);
    }

    @Test
    public void testCassandraQueryWaitStrategy() throws Exception {
        CassandraContainer cassandraContainer = (CassandraContainer) new CassandraContainer()
                .waitingFor(new CassandraQueryWaitStrategy())
                .withLogConsumer(new Slf4jLogConsumer(logger));
        cassandraContainer.start();
        try {
            assertTrue("Cassandra container is not running", cassandraContainer.isRunning());
        } finally {
            cassandraContainer.stop();
        }
    }

    private void testInitScript(CassandraContainer cassandraContainer) {
        cassandraContainer.start();
        try {
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT * FROM IgniteTest.catalog_category");
            assertTrue("Query was not applied", resultSet.wasApplied());
            Row row = resultSet.one();
            assertEquals("Inserted row is not in expected state", 1, row.getLong(0));
            assertEquals("Inserted row is not in expected state", "test_category", row.getString(1));
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
