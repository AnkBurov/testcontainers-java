package org.testcontainers.delegate;

import java.util.Collection;

/**
 * Database delegate
 * <p>
 * Gives an abstraction from concrete database
 *
 * @author Eugeny Karpov
 */
public interface DatabaseDelegate extends AutoCloseable {

    /**
     * Execute statement by the implementation of the delegate
     */
    void execute(String statement, String scriptPath, int lineNumber, boolean continueOnError, boolean ignoreFailedDrops);

    /**
     * Execute collection of statements and close underlying connection
     */
    void execute(Collection<String> statements, String scriptPath, boolean continueOnError, boolean ignoreFailedDrops);

    /**
     * Close connection to the database
     * <p>
     * Overridden to suppress throwing Exception
     */
    @Override
    void close();
}
