package org.testcontainers.delegate;

import java.util.Collection;

/**
 * @param <CONTAINER> testcontainers container
 * @param <CONNECTION> connection to the database
 * @author Eugeny Karpov
 */
public abstract class AbstractDatabaseDelegate<CONTAINER, CONNECTION> implements DatabaseDelegate {

    /**
     * Testcontainers container
     */
    protected CONTAINER container;

    /**
     * Database connection
     */
    private CONNECTION connection;

    public AbstractDatabaseDelegate(CONTAINER container) {
        this.container = container;
    }

    /**
     * Get or create new connection to the database
     */
    protected CONNECTION getConnection() {
        if (connection == null) {
            this.connection = createNewConnection();
        }
        return connection;
    }

    @Override
    public void execute(Collection<String> statements, String scriptPath, boolean continueOnError, boolean ignoreFailedDrops) {
        try (DatabaseDelegate closeableDelegate = this) {
            int lineNumber = 0;
            for (String statement : statements) {
                lineNumber++;
                execute(statement, scriptPath, lineNumber, continueOnError, ignoreFailedDrops);
            }
        };
    }

    @Override
    public void close() {
        closeConnectionQuietly();
    }

    /**
     * Quietly close the connection
     */
    public abstract void closeConnectionQuietly();

    /**
     * Template method for creating new connections to the database
     */
    protected abstract CONNECTION createNewConnection();
}
