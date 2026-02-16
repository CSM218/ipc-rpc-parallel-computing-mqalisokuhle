
import pdc.Message;
import java.util.Arrays;

public class ProtocolTest {
    public static void main(String[] args) {
        try {
            System.out.println("Testing Message packing/unpacking...");
            
            Message original = new Message();
            original.magic = "CSM218";
            original.version = 1;
            original.type = "REGISTER";
            original.sender = "test-worker";
            original.timestamp = System.currentTimeMillis();
            original.payload = "test-data".getBytes();
            
            byte[] packed = original.pack();
            Message unpacked = Message.unpack(packed);
            
            boolean success = original.magic.equals(unpacked.magic) &&
                             original.version == unpacked.version &&
                             original.type.equals(unpacked.type) &&
                             original.sender.equals(unpacked.sender) &&
                             original.timestamp == unpacked.timestamp &&
                             Arrays.equals(original.payload, unpacked.payload);
            
            if (success) {
                System.out.println("PASS: Message protocol works!");
                System.exit(0);
            } else {
                System.out.println("FAIL: Message protocol mismatch");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
