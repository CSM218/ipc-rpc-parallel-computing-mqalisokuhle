package pdc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.*;

/**
 * JUnit 5 tests for the Worker class.
 */
class WorkerTest {

    private Worker worker;
    private ExecutorService testExecutor;

    @BeforeEach
    void setUp() {
        worker = new Worker();
        testExecutor = Executors.newCachedThreadPool();
    }

    @Test
    @Timeout(5)
    void testWorker_Join_Logic() {
        // Just verify that joinCluster doesn't throw exceptions
        assertDoesNotThrow(() -> {
            worker.joinCluster("localhost", 9999);
        });
    }

    @Test
    @Timeout(5)
    void testWorker_Execute_Invocation() {
        // Verify that execute can be called without throwing
        assertDoesNotThrow(() -> {
            // Run in background since it might block
            Future<?> future = testExecutor.submit(() -> {
                worker.execute();
                return null;
            });
            
            Thread.sleep(500);
            
            // Should either be running or completed without exception
            if (future.isDone()) {
                future.get(1, TimeUnit.SECONDS); // This will throw if execution failed
            }
            
            future.cancel(true);
        });
    }
}