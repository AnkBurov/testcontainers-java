package org.testcontainers.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.delegate.AbstractDatabaseDelegate;
import org.testcontainers.exception.ConnectionCreationException;
import org.testcontainers.ext.ScriptUtils;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class ContainerLessJdbcDatabaseDelegate extends AbstractDatabaseDelegate<Connection> {

    private Connection connection;

    public ContainerLessJdbcDatabaseDelegate(Connection connection) {
        this.connection = connection;
    }

    @Override
    protected void closeConnectionQuietly(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            log.error("Could not obtain JDBC connection");
            throw new ConnectionCreationException("Could not obtain JDBC connection", e);
        }
    }

    @Override
    protected Connection createNewConnection() {
        return connection;
    }

    @Override
    public void execute(String statement, String scriptPath, int lineNumber, boolean continueOnError, boolean ignoreFailedDrops) {
        try {
            boolean rowsAffected = getConnection().createStatement().execute(statement);
            log.debug("{} returned as updateCount for SQL: {}", rowsAffected, statement);
        } catch (SQLException ex) {
            boolean dropStatement = statement.trim().toLowerCase().startsWith("drop");
            if (continueOnError || (dropStatement && ignoreFailedDrops)) {
                log.debug("Failed to execute SQL script statement at line {} of resource {}: {}", lineNumber, scriptPath, statement, ex);
            } else {
                throw new ScriptUtils.ScriptStatementFailedException(statement, lineNumber, scriptPath, ex);
            }
        }
    }
}
