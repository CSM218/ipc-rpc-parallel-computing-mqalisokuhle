// src/main/java/pdc/Master.java
package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/**
 * Simple Master implementation
 */
public class Master {
    
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private Map<String, TaskInfo> pendingTasks = new ConcurrentHashMap<>();
    private ServerSocket serverSocket;
    private volatile boolean running = true;
    private ScheduledExecutorService heartbeatScheduler = Executors.newScheduledThreadPool(1);
    
    /**
     * Worker information
     */
    private static class WorkerInfo {
        String id;
        Socket socket;
        DataInputStream in;
        DataOutputStream out;
        long lastHeartbeat;
        boolean busy;
        String currentTask;
        String authToken;  // Token for authentication
        
        WorkerInfo(String id, Socket socket, DataInputStream in, DataOutputStream out) {
            this.id = id;
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.lastHeartbeat = System.currentTimeMillis();
            this.busy = false;
            this.authToken = null;
        }
    }
    
    /**
     * Task information
     */
    private static class TaskInfo {
        String id;
        String operation;
        String data;
        String assignedWorker;
        String workerToken;  // Token of assigned worker for validation
        long startTime;
        CompletableFuture<String> future = new CompletableFuture<>();
        
        TaskInfo(String id, String operation, String data) {
            this.id = id;
            this.operation = operation;
            this.data = data;
            this.startTime = System.currentTimeMillis();
        }
    }
    
    /**
     * Coordinate distributed matrix multiplication
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (workers.isEmpty()) {
            System.out.println("No workers available");
            return null;
        }
        
        if ("BLOCK_MULTIPLY".equals(operation) || "MATRIX_MULTIPLY".equals(operation)) {
            return distributeMatrixMultiplication(data);
        }
        
        return null;
    }
    
    /**
     * Distribute matrix multiplication across workers
     */
    private int[][] distributeMatrixMultiplication(int[][] matrix) {
        int size = matrix.length;
        int numWorkers = Math.max(1, workers.size());
        
        // Calculate block size per worker
        int blockSize = (size + numWorkers - 1) / numWorkers; // Round up
        int result[][] = new int[size][size];
        
        // Create tasks for each block of rows
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int blockStart = 0; blockStart < size; blockStart += blockSize) {
            final int bs = blockStart;
            final int blockEnd = Math.min(blockStart + blockSize, size);
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                // Create task for this block
                String taskId = "task-" + System.currentTimeMillis() + "-" + bs;
                
                // Build task data: taskId:MULTIPLY:blockStart:blockEnd:matrixData
                StringBuilder sb = new StringBuilder();
                sb.append(taskId).append(":MULTIPLY:");
                sb.append(bs).append(":").append(blockEnd).append(":");
                sb.append(size).append(":").append(size);
                
                // Add the full matrix data
                for (int i = 0; i < size; i++) {
                    for (int j = 0; j < size; j++) {
                        sb.append(":").append(matrix[i][j]);
                    }
                }
                
                // Submit task and wait
                String resultStr = submitTask(taskId, "MATRIX_MULTIPLY", sb.toString());
                
                // Parse result and store in result matrix
                if (resultStr != null && resultStr.startsWith(taskId)) {
                    String[] parts = resultStr.split(":");
                    if (parts.length >= 4 && "RESULT".equals(parts[1])) {
                        int resBlockStart = Integer.parseInt(parts[2]);
                        int resBlockEnd = Integer.parseInt(parts[3]);
                        int cols = Integer.parseInt(parts[4]);
                        
                        int idx = 5;
                        for (int i = resBlockStart; i < resBlockEnd && i < size; i++) {
                            for (int j = 0; j < cols && j < size; j++) {
                                if (idx < parts.length) {
                                    result[i][j] = Integer.parseInt(parts[idx++]);
                                }
                            }
                        }
                    }
                }
            }, systemThreads);
            
            futures.add(future);
        }
        
        // Wait for all tasks to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        return result;
    }
    
    /**
     * Submit a task to a worker
     */
    private String submitTask(String taskId, String operation, String taskData) {
        TaskInfo task = new TaskInfo(taskId, operation, taskData);
        pendingTasks.put(taskId, task);
        
        // Find an available worker
        assignTask(task);
        
        // Wait for result
        try {
            return task.future.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            pendingTasks.remove(taskId);
            return "ERROR: " + e.getMessage();
        }
    }
    
    /**
     * Assign task to an available worker
     */
    private void assignTask(TaskInfo task) {
        for (WorkerInfo worker : workers.values()) {
            if (!worker.busy && worker.authToken != null) {
                try {
                    // Create RPC request with token
                    Message taskMsg = new Message();
                    taskMsg.messageType = "RPC_REQUEST";
                    taskMsg.payload = worker.authToken + "|" + task.data;
                    
                    worker.busy = true;
                    worker.currentTask = task.id;
                    task.assignedWorker = worker.id;
                    task.workerToken = worker.authToken;
                    
                    taskMsg.sendToSocket(worker.out);
                    System.out.println("Assigned task " + task.id + " to worker " + worker.id);
                    return;
                    
                } catch (IOException e) {
                    worker.busy = false;
                    System.err.println("Failed to send task to worker " + worker.id + ": " + e.getMessage());
                }
            }
        }
        
        // No worker available, queue task
        System.out.println("No available workers for task " + task.id);
    }
    
    /**
     * Start listening for worker connections
     */
    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("Master listening on port " + port);
        
        // Start heartbeat checker
        startHeartbeatChecker();
        
        // Accept connections
        while (running) {
            try {
                Socket workerSocket = serverSocket.accept();
                systemThreads.submit(() -> handleWorkerConnection(workerSocket));
            } catch (IOException e) {
                if (running) {
                    System.err.println("Accept failed: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Handle new worker connection
     */
    private void handleWorkerConnection(Socket socket) {
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            
            // Read registration message
            Message regMsg = Message.readFromSocket(in);
            
            if ("REGISTER_WORKER".equals(regMsg.messageType)) {
                String workerId = regMsg.payload;
                
                // Generate token
                String token = "CSM218_TOKEN_" + workerId + "_" + System.currentTimeMillis();
                
                // Store worker
                WorkerInfo worker = new WorkerInfo(workerId, socket, in, out);
                worker.authToken = token;
                workers.put(workerId, worker);
                
                System.out.println("Worker registered: " + workerId);
                
                // Send ACK back with token
                Message ackMsg = new Message();
                ackMsg.messageType = "WORKER_ACK";
                ackMsg.payload = token;
                ackMsg.sendToSocket(out);
                
                // Start listening for messages from this worker
                handleWorkerMessages(worker);
            }
            
        } catch (IOException e) {
            System.err.println("Error handling worker connection: " + e.getMessage());
        }
    }
    
    /**
     * Handle messages from a worker
     */
    private void handleWorkerMessages(WorkerInfo worker) {
        try {
            while (running) {
                Message msg = Message.readFromSocket(worker.in);
                
                if ("TASK_COMPLETE".equals(msg.messageType)) {
                    // Parse Task ID from payload
                    String resultStr = msg.payload;
                    String[] parts = resultStr.split(":");
                    
                    if (parts.length < 2) {
                        System.err.println("Invalid result format from worker " + worker.id);
                        continue;
                    }
                    
                    String taskId = parts[0];
                    TaskInfo task = pendingTasks.get(taskId);
                    
                    // Validate worker and token
                    if (task != null && task.assignedWorker != null && 
                        task.assignedWorker.equals(worker.id) && 
                        task.workerToken != null && task.workerToken.equals(worker.authToken)) {
                        
                        pendingTasks.remove(taskId);
                        task.future.complete(resultStr);
                        
                        worker.busy = false;
                        worker.currentTask = null;
                        long executionTime = System.currentTimeMillis() - task.startTime;
                        worker.lastHeartbeat = System.currentTimeMillis();
                        
                        System.out.println("Task " + taskId + " completed by worker " + worker.id + 
                                         " in " + executionTime + "ms");
                        
                        // Check for queued tasks
                        reassignTasks();
                    } else {
                        System.err.println("Task validation failed for task " + taskId + 
                                         " from worker " + worker.id);
                        if (task != null) {
                            task.future.completeExceptionally(
                                new IOException("Invalid token from worker"));
                            pendingTasks.remove(taskId);
                        }
                    }
                    
                } else if ("HEARTBEAT".equals(msg.messageType)) {
                    worker.lastHeartbeat = System.currentTimeMillis();
                    
                    // Send ACK back
                    Message ackMsg = new Message();
                    ackMsg.messageType = "WORKER_ACK";
                    ackMsg.sendToSocket(worker.out);
                    
                } else if ("TASK_ERROR".equals(msg.messageType)) {
                    String error = msg.payload;
                    System.err.println("Worker " + worker.id + " error: " + error);
                    
                    if (worker.currentTask != null) {
                        // Requeue task
                        TaskInfo task = pendingTasks.get(worker.currentTask);
                        if (task != null) {
                            task.assignedWorker = null;
                            task.workerToken = null;
                            assignTask(task);
                        }
                    }
                    
                    worker.busy = false;
                    worker.currentTask = null;
                }
            }
        } catch (IOException e) {
            // Worker disconnected
            System.out.println("Worker " + worker.id + " disconnected: " + e.getMessage());
            handleWorkerFailure(worker);
        }
    }
    
    /**
     * Handle worker failure
     */
    private void handleWorkerFailure(WorkerInfo worker) {
        workers.remove(worker.id);
        
        // Reassign task if any
        if (worker.currentTask != null) {
            TaskInfo task = pendingTasks.get(worker.currentTask);
            if (task != null) {
                System.out.println("Reassigning task " + task.id + " from failed worker");
                assignTask(task);
            }
        }
        
        // Clean up socket
        try {
            if (worker.socket != null && !worker.socket.isClosed()) {
                worker.socket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
    }
    
    /**
     * Start heartbeat checker
     */
    private void startHeartbeatChecker() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            List<String> deadWorkers = new ArrayList<>();
            
            for (WorkerInfo worker : workers.values()) {
                if (now - worker.lastHeartbeat > 5000) { // 5 second timeout
                    // Send heartbeat
                    try {
                        Message heartbeat = new Message();
                        heartbeat.messageType = "HEARTBEAT";
                        heartbeat.sendToSocket(worker.out);
                    } catch (IOException e) {
                        deadWorkers.add(worker.id);
                    }
                }
            }
            
            // Remove dead workers
            for (String workerId : deadWorkers) {
                WorkerInfo worker = workers.remove(workerId);
                if (worker != null) {
                    System.out.println("Worker " + workerId + " timed out");
                    handleWorkerFailure(worker);
                }
            }
            
        }, 0, 2, TimeUnit.SECONDS);
    }
    
    /**
     * Reassign pending tasks
     */
    private void reassignTasks() {
        // Check for any pending tasks without assigned workers
        for (TaskInfo task : pendingTasks.values()) {
            if (task.assignedWorker == null || !workers.containsKey(task.assignedWorker)) {
                assignTask(task);
            }
        }
    }
    
    /**
     * System health check
     */
    public void reconcileState() {
        System.out.println("Reconciling cluster state...");
        System.out.println("Active workers: " + workers.size());
        System.out.println("Pending tasks: " + pendingTasks.size());
        
        // Check for dead workers
        long now = System.currentTimeMillis();
        List<String> toRemove = new ArrayList<>();
        
        for (WorkerInfo worker : workers.values()) {
            if (now - worker.lastHeartbeat > 10000) { // 10 seconds
                toRemove.add(worker.id);
            }
        }
        
        // Remove dead workers
        for (String workerId : toRemove) {
            WorkerInfo worker = workers.remove(workerId);
            if (worker != null) {
                System.out.println("Removing dead worker: " + workerId);
                try {
                    worker.socket.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
        
        // Reassign tasks
        reassignTasks();
    }
    
    /**
     * Main entry point
     */
    public static void main(String[] args) {
        String portStr = System.getenv("MASTER_PORT");
        int port = portStr != null ? Integer.parseInt(portStr) : 9999;
        
        Master master = new Master();
        
        try {
            master.listen(port);
        } catch (IOException e) {
            System.err.println("Failed to start master: " + e.getMessage());
            e.printStackTrace();
        }
    }
}