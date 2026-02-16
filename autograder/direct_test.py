# direct_test.py
import subprocess
import os
import sys
import json
import time

print("=" * 50)
print("CSM218 DIRECT TEST RUNNER")
print("=" * 50)

# Step 1: Compile Java files manually
print("\n[1/3] Compiling Java files...")
os.chdir("..")  # Go to project root

# Find all Java files
java_files = []
for root, dirs, files in os.walk("src/main/java"):
    for file in files:
        if file.endswith(".java"):
            java_files.append(os.path.join(root, file))

# Create build directory
os.makedirs("build/classes/main", exist_ok=True)

# Compile
cmd = ["javac", "-d", "build/classes/main"] + java_files
result = subprocess.run(cmd, capture_output=True, text=True)

if result.returncode != 0:
    print("❌ COMPILATION FAILED")
    print("\nErrors:")
    print(result.stderr)
    
    # Save errors to file
    with open("compilation_errors.txt", "w") as f:
        f.write(result.stderr)
    print("\nErrors saved to compilation_errors.txt")
    sys.exit(1)
else:
    print("✅ Compilation successful!")

# Step 2: Run basic protocol test
print("\n[2/3] Testing Message protocol...")

# Create a simple test class
test_code = """
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
"""

# Write test file
with open("src/main/java/ProtocolTest.java", "w") as f:
    f.write(test_code)

# Compile test
subprocess.run(["javac", "-cp", "build/classes/main", "-d", "build/classes/main", 
                "src/main/java/ProtocolTest.java"], capture_output=True)

# Run test
result = subprocess.run(["java", "-cp", "build/classes/main", "ProtocolTest"], 
                       capture_output=True, text=True)

print(result.stdout)
if result.returncode == 0:
    print("✅ Protocol test passed!")
    protocol_score = 20
else:
    print("❌ Protocol test failed")
    print(result.stderr)
    protocol_score = 0

# Step 3: Check for basic structure
print("\n[3/3] Checking code structure...")

structure_score = 0
structure_total = 10

# Check if Master.java has required methods
with open("src/main/java/pdc/Master.java", "r") as f:
    master_code = f.read()
    if "coordinate" in master_code:
        structure_score += 3
        print("✅ Master.coordinate() found")
    if "listen" in master_code:
        structure_score += 3
        print("✅ Master.listen() found")
    if "reconcileState" in master_code:
        structure_score += 2
        print("✅ Master.reconcileState() found")

# Check if Worker.java has required methods
with open("src/main/java/pdc/Worker.java", "r") as f:
    worker_code = f.read()
    if "joinCluster" in worker_code:
        structure_score += 2
        print("✅ Worker.joinCluster() found")

print(f"\nStructure score: {structure_score}/{structure_total}")

# Calculate total
total_score = protocol_score + structure_score
max_score = 30  # Just for these basic tests

print("\n" + "=" * 50)
print("RESULTS SUMMARY")
print("=" * 50)
print(f"Protocol Test:    {protocol_score}/20")
print(f"Structure Check:  {structure_score}/{structure_total}")
print(f"TOTAL:            {total_score}/{max_score} ({total_score/max_score*100:.1f}%)")
print("=" * 50)

# Save results
results = {
    "scores": {
        "protocol": {"earned": protocol_score, "available": 20},
        "structure": {"earned": structure_score, "available": structure_total}
    },
    "total": {"earned": total_score, "available": max_score}
}

with open("direct_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("\nResults saved to direct_results.json")