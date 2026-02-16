// src/main/java/pdc/Worker.java
package pdc;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple Worker implementation
 */
public class Worker {
    private String workerId;
    private Socket masterSocket;
    private DataInputStream in;
    private DataOutputStream out;
    private volatile boolean running = true;
    private ExecutorService taskExecutor = Executors.newFixedThreadPool(4);
    private String authToken;  // Token received from master
    
    public Worker() {
        // Get worker ID from environment or generate one
        workerId = System.getenv("WORKER_ID");
        if (workerId == null || workerId.isEmpty()) {
            workerId = "worker-" + System.nanoTime();
        }
    }
    
    /**
     * Connects to Master and starts processing tasks
     */
    public void joinCluster(String masterHost, int port) {
        try {
            System.out.println("Worker " + workerId + " connecting to master at " + masterHost + ":" + port);
            
            // Connect to master
            masterSocket = new Socket(masterHost, port);
            in = new DataInputStream(masterSocket.getInputStream());
            out = new DataOutputStream(masterSocket.getOutputStream());
            
            // Register with master
            register();
            
            // Start processing tasks
            execute();
            
        } catch (IOException e) {
            System.err.println("Worker " + workerId + " failed to connect: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Register with master
     */
    private void register() throws IOException {
        Message regMsg = new Message();
        regMsg.messageType = "REGISTER_WORKER";
        regMsg.payload = workerId;
        
        regMsg.sendToSocket(out);
        System.out.println("Worker " + workerId + " registered");
        
        // Wait for ACK with token
        Message ackMsg = Message.readFromSocket(in);
        if ("WORKER_ACK".equals(ackMsg.messageType)) {
            authToken = ackMsg.payload;
            System.out.println("Worker " + workerId + " received auth token: " + authToken);
        } else {
            throw new IOException("Expected WORKER_ACK, got " + ackMsg.messageType);
        }
    }
    
    /**
     * Main execution loop - processes tasks from master
     */
    public void execute() {
        try {
            while (running) {
                // Read message from master
                Message msg = Message.readFromSocket(in);
                
                if ("RPC_REQUEST".equals(msg.messageType)) {
                    // Submit task to thread pool
                    taskExecutor.submit(() -> processTask(msg));
                    
                } else if ("HEARTBEAT".equals(msg.messageType)) {
                    // Respond to heartbeat
                    Message response = new Message();
                    response.messageType = "HEARTBEAT";
                    response.sendToSocket(out);
                    
                } else if ("SHUTDOWN".equals(msg.messageType)) {
                    // Shutdown worker
                    running = false;
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Worker " + workerId + " connection lost: " + e.getMessage());
        } finally {
            cleanup();
        }
    }
    
    /**
     * Process a single task
     */
    private void processTask(Message taskMsg) {
        try {
            String taskData = taskMsg.payload;
            
            // Extract token and verify
            String[] tokenSplit = taskData.split("\\|", 2);
            if (tokenSplit.length < 2) {
                throw new IOException("Invalid task format - missing token");
            }
            
            String receivedToken = tokenSplit[0];
            String actualTaskData = tokenSplit[1];
            
            // Validate token
            if (!receivedToken.equals(authToken)) {
                throw new IOException("Token mismatch: expected " + authToken + ", got " + receivedToken);
            }
            
            System.out.println("Worker " + workerId + " processing task: " + actualTaskData.substring(0, Math.min(50, actualTaskData.length())) + "...");
            
            // Parse task
            String[] parts = actualTaskData.split(":");
            String taskId = parts[0];
            String operation = parts[1];
            
            String result = "";
            
            if ("MATRIX_MULTIPLY".equals(operation)) {
                // New format: taskId:MATRIX_MULTIPLY:blockStart:blockEnd:rows:cols:matrix_data
                int blockStart = Integer.parseInt(parts[2]);
                int blockEnd = Integer.parseInt(parts[3]);
                int rows = Integer.parseInt(parts[4]);
                int cols = Integer.parseInt(parts[5]);
                
                // Parse full matrix
                int[][] matrix = new int[rows][cols];
                int idx = 6;
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        matrix[i][j] = Integer.parseInt(parts[idx++]);
                    }
                }
                
                // Compute this block's result (simple square matrix multiply)
                int[][] resultMatrix = new int[blockEnd - blockStart][cols];
                for (int i = blockStart; i < blockEnd; i++) {
                    for (int j = 0; j < cols; j++) {
                        for (int k = 0; k < cols; k++) {
                            resultMatrix[i - blockStart][j] += matrix[i][k] * matrix[k][j];
                        }
                    }
                }
                
                // Build result string
                StringBuilder sb = new StringBuilder();
                sb.append(taskId).append(":RESULT:").append(blockStart).append(":").append(blockEnd);
                sb.append(":").append(cols);
                for (int i = 0; i < (blockEnd - blockStart); i++) {
                    for (int j = 0; j < cols; j++) {
                        sb.append(":").append(resultMatrix[i][j]);
                    }
                }
                result = sb.toString();
                
            } else if ("MULTIPLY".equals(operation)) {
                // Legacy format: taskId:MULTIPLY:rowsA:colsA:matrixA_data:rowsB:colsB:matrixB_data
                int rowsA = Integer.parseInt(parts[2]);
                int colsA = Integer.parseInt(parts[3]);
                
                // Parse matrix A
                int[][] matrixA = new int[rowsA][colsA];
                int idx = 4;
                for (int i = 0; i < rowsA; i++) {
                    for (int j = 0; j < colsA; j++) {
                        matrixA[i][j] = Integer.parseInt(parts[idx++]);
                    }
                }
                
                int rowsB = Integer.parseInt(parts[idx++]);
                int colsB = Integer.parseInt(parts[idx++]);
                
                // Parse matrix B
                int[][] matrixB = new int[rowsB][colsB];
                for (int i = 0; i < rowsB; i++) {
                    for (int j = 0; j < colsB; j++) {
                        matrixB[i][j] = Integer.parseInt(parts[idx++]);
                    }
                }
                
                // Multiply matrices
                int[][] resultMatrix = multiply(matrixA, matrixB);
                
                // Build result string
                StringBuilder sb = new StringBuilder();
                sb.append(taskId).append(":RESULT:").append(rowsA).append(":").append(colsB);
                for (int i = 0; i < rowsA; i++) {
                    for (int j = 0; j < colsB; j++) {
                        sb.append(":").append(resultMatrix[i][j]);
                    }
                }
                result = sb.toString();
            }
            
            // Send result back
            Message response = new Message();
            response.messageType = "TASK_COMPLETE";
            response.payload = result;
            response.sendToSocket(out);
            
            System.out.println("Worker " + workerId + " completed task " + taskId);
            
        } catch (Exception e) {
            System.err.println("Worker " + workerId + " task failed: " + e.getMessage());
            
            // Send error
            try {
                Message errorMsg = new Message();
                errorMsg.messageType = "TASK_ERROR";
                errorMsg.payload = ("Task failed: " + e.getMessage());
                errorMsg.sendToSocket(out);
            } catch (IOException ex) {
                System.err.println("Failed to send error message: " + ex.getMessage());
            }
        }
    }
    
    /**
     * Simple matrix multiplication
     */
    private int[][] multiply(int[][] a, int[][] b) {
        int rowsA = a.length;
        int colsA = a[0].length;
        int colsB = b[0].length;
        
        int[][] result = new int[rowsA][colsB];
        
        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsB; j++) {
                for (int k = 0; k < colsA; k++) {
                    result[i][j] += a[i][k] * b[k][j];
                }
            }
        }
        
        return result;
    }
    
    /**
     * Clean up resources
     */
    private void cleanup() {
        try {
            taskExecutor.shutdown();
            if (masterSocket != null && !masterSocket.isClosed()) {
                masterSocket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
        System.out.println("Worker " + workerId + " stopped");
    }
    
    /**
     * Main entry point
     */
    public static void main(String[] args) {
        String masterHost = System.getenv("MASTER_HOST");
        if (masterHost == null) masterHost = "localhost";
        
        String portStr = System.getenv("MASTER_PORT");
        int masterPort = portStr != null ? Integer.parseInt(portStr) : 9999;
        
        Worker worker = new Worker();
        worker.joinCluster(masterHost, masterPort);
    }
}