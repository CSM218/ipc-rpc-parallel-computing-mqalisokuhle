// src/main/java/pdc/Message.java
package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Simple binary protocol:
 * [magic:6 bytes][version:4 bytes][typeLen:2 bytes][type:variable]
 * [senderLen:2 bytes][sender:variable][timestamp:8 bytes]
 * [payloadLen:4 bytes][payload:variable]
 */
public class Message {
    public String magic;          // Should be "CSM218"
    public int version;           // Should be 1
    public String messageType;    // Message type (CONNECT, REGISTER_WORKER, RPC_REQUEST, etc.)
    public String studentId;      // Student ID from environment
    public long timestamp;        // When sent
    public String payload;        // The actual data (as string per protocol spec)

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.studentId = System.getenv("STUDENT_ID");
        if (this.studentId == null || this.studentId.isEmpty()) {
            this.studentId = "unknown";
        }
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Packs message into byte array with length prefix for framing
     * Format: magic(6) | version(4) | messageTypeLen(2) | messageType | studentIdLen(2) | studentId |
     *         timestamp(8) | payloadLen(4) | payload
     */
    public byte[] pack() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        
        // Write fixed fields
        out.writeBytes(magic);           // 6 bytes "CSM218"
        out.writeInt(version);            // 4 bytes
        
        // Write messageType with length prefix
        byte[] messageTypeBytes = messageType.getBytes(StandardCharsets.UTF_8);
        out.writeShort(messageTypeBytes.length); // 2 bytes
        out.write(messageTypeBytes);              // variable
        
        // Write studentId with length prefix
        byte[] studentIdBytes = studentId.getBytes(StandardCharsets.UTF_8);
        out.writeShort(studentIdBytes.length); // 2 bytes
        out.write(studentIdBytes);              // variable
        
        // Write timestamp
        out.writeLong(timestamp);           // 8 bytes
        
        // Write payload with length prefix
        byte[] payloadBytes = (payload == null || payload.isEmpty()) ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8);
        out.writeInt(payloadBytes.length);    // 4 bytes
        if (payloadBytes.length > 0) {
            out.write(payloadBytes);               // variable
        }
        
        return baos.toByteArray();
    }

    /**
     * Unpacks message from byte array
     */
    public static Message unpack(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream in = new DataInputStream(bais);
        
        Message msg = new Message();
        
        // Read magic (6 bytes)
        byte[] magicBytes = new byte[6];
        in.readFully(magicBytes);
        msg.magic = new String(magicBytes);
        
        // Validate magic
        if (!msg.magic.equals("CSM218")) {
            throw new IOException("Invalid message magic: " + msg.magic);
        }
        
        // Read version
        msg.version = in.readInt();
        
        // Read messageType
        short messageTypeLen = in.readShort();
        byte[] messageTypeBytes = new byte[messageTypeLen];
        in.readFully(messageTypeBytes);
        msg.messageType = new String(messageTypeBytes, StandardCharsets.UTF_8);
        
        // Read studentId
        short studentIdLen = in.readShort();
        byte[] studentIdBytes = new byte[studentIdLen];
        in.readFully(studentIdBytes);
        msg.studentId = new String(studentIdBytes, StandardCharsets.UTF_8);
        
        // Read timestamp
        msg.timestamp = in.readLong();
        
        // Read payload
        int payloadLen = in.readInt();
        if (payloadLen > 0) {
            byte[] payloadBytes = new byte[payloadLen];
            in.readFully(payloadBytes);
            msg.payload = new String(payloadBytes, StandardCharsets.UTF_8);
        } else {
            msg.payload = "";
        }
        
        return msg;
    }
    
    /**
     * Helper to read a complete message from socket
     */
    public static Message readFromSocket(DataInputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        
        // Step 1: Read magic (6 bytes) + version (4 bytes)
        byte[] headerStart = new byte[10];
        int bytesRead = 0;
        while (bytesRead < 10) {
            int n = in.read(headerStart, bytesRead, 10 - bytesRead);
            if (n == -1) throw new IOException("Connection closed while reading header");
            bytesRead += n;
        }
        buffer.write(headerStart);
        
        // Step 2: Read messageType length + messageType
        byte[] typeLenBytes = new byte[2];
        in.readFully(typeLenBytes);
        short typeLen = (short)(((typeLenBytes[0] & 0xFF) << 8) | (typeLenBytes[1] & 0xFF));
        buffer.write(typeLenBytes);
        
        byte[] typeBytes = new byte[typeLen];
        in.readFully(typeBytes);
        buffer.write(typeBytes);
        
        // Step 3: Read studentId length + studentId
        byte[] studentIdLenBytes = new byte[2];
        in.readFully(studentIdLenBytes);
        short studentIdLen = (short)(((studentIdLenBytes[0] & 0xFF) << 8) | (studentIdLenBytes[1] & 0xFF));
        buffer.write(studentIdLenBytes);
        
        byte[] studentIdBytes = new byte[studentIdLen];
        in.readFully(studentIdBytes);
        buffer.write(studentIdBytes);
        
        // Step 4: Read timestamp (8 bytes)
        byte[] timestampBytes = new byte[8];
        in.readFully(timestampBytes);
        buffer.write(timestampBytes);
        
        // Step 5: Read payload length + payload
        byte[] payloadLenBytes = new byte[4];
        in.readFully(payloadLenBytes);
        int payloadLen = ((payloadLenBytes[0] & 0xFF) << 24) |
                         ((payloadLenBytes[1] & 0xFF) << 16) |
                         ((payloadLenBytes[2] & 0xFF) << 8)  |
                         (payloadLenBytes[3] & 0xFF);
        buffer.write(payloadLenBytes);
        
        if (payloadLen > 0) {
            byte[] payloadBytes = new byte[payloadLen];
            in.readFully(payloadBytes);
            buffer.write(payloadBytes);
        }
        
        return unpack(buffer.toByteArray());
    }
    
    /**
     * Helper to send message to socket
     */
    public void sendToSocket(DataOutputStream out) throws IOException {
        byte[] data = this.pack();
        out.write(data);
        out.flush();
    }
    
    /**
     * Validate protocol compliance
     */
    public void validate() throws IOException {
        if (!("CSM218".equals(magic))) {
            throw new IOException("Invalid magic: " + magic);
        }
        if (version != 1) {
            throw new IOException("Invalid version: " + version);
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new IOException("Missing messageType");
        }
        if (studentId == null || studentId.isEmpty()) {
            throw new IOException("Missing studentId");
        }
        if (timestamp <= 0) {
            throw new IOException("Invalid timestamp");
        }
    }
}