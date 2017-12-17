package org.testcontainers.containers.delegate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.delegate.AbstractDatabaseDelegate;
import org.testcontainers.delegate.exception.ConnectionCreationException;
import org.testcontainers.jdbc.ext.ScriptUtils;

import static org.testcontainers.containers.CassandraContainer.CQL_PORT;

/**
 * Cassandra database delegate
 *
 * @author Eugeny Karpov
 */
@Slf4j
public class CassandraDatabaseDelegate extends AbstractDatabaseDelegate<CassandraContainer, Session> {

    public CassandraDatabaseDelegate(CassandraContainer cassandraContainer) {
        super(cassandraContainer);
    }

    @Override
    protected Session createNewConnection() {
        try {
            return Cluster.builder()
                    .addContactPoint(container.getContainerIpAddress())
                    .withPort(container.getMappedPort(CQL_PORT))
                    .build()
                    .newSession();
        } catch (DriverException e) {
            log.error("Could not obtain cassandra connection");
            throw new ConnectionCreationException("Could not obtain cassandra connection", e);
        }
    }

    @Override
    public void execute(String statement, String scriptPath, int lineNumber, boolean continueOnError, boolean ignoreFailedDrops) {
        try {
            ResultSet result = getConnection().execute(statement);
            if (result.wasApplied()) {
                log.debug("Statement {} was applied", statement);
            } else {
                throw new ScriptUtils.ScriptStatementFailedException(statement, lineNumber, scriptPath);
            }
        } catch (DriverException e) {
            throw new ScriptUtils.ScriptStatementFailedException(statement, lineNumber, scriptPath, e);
        }
    }

    @Override
    public void closeConnectionQuietly() {
        try {
            getConnection().getCluster().close();
        } catch (Exception e) {
            log.error("Could not close cassandra connection", e);
        }
    }
}
