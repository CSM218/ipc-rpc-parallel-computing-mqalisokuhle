# run_test.ps1
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "CSM218 MANUAL TEST" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Step 1: Compile the code
Write-Host "`nStep 1: Compiling Java files..." -ForegroundColor Yellow

# Create build directory
New-Item -ItemType Directory -Path "build/classes/main" -Force | Out-Null

# Compile all Java files
$javaFiles = Get-ChildItem -Path "src/main/java/pdc" -Filter *.java | ForEach-Object { $_.FullName }
Write-Host "Found $($javaFiles.Count) Java files to compile"

$compileResult = javac -d build/classes/main $javaFiles 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ COMPILATION FAILED!" -ForegroundColor Red
    Write-Host $compileResult
    exit 1
}
Write-Host "✅ Compilation successful!" -ForegroundColor Green

# Step 2: Create a simple test
Write-Host "`nStep 2: Creating test..." -ForegroundColor Yellow

$testCode = @'
public class SimpleTest {
    public static void main(String[] args) {
        try {
            System.out.println("Testing Message class...");
            
            pdc.Message msg = new pdc.Message();
            msg.messageType = "REGISTER_WORKER";
            msg.studentId = "test123";
            msg.timestamp = System.currentTimeMillis();
            msg.payload = "test-data";
            
            System.out.println("✓ Message created");
            System.out.println("  Type: " + msg.messageType);
            System.out.println("  Student: " + msg.studentId);
            System.out.println("  Payload: " + msg.payload);
            
            byte[] packed = msg.pack();
            System.out.println("✓ Packed " + packed.length + " bytes");
            
            pdc.Message unpacked = pdc.Message.unpack(packed);
            System.out.println("✓ Unpacked successfully");
            System.out.println("  Unpacked type: " + unpacked.messageType);
            System.out.println("  Unpacked payload: " + unpacked.payload);
            
            if (msg.messageType.equals(unpacked.messageType)) {
                System.out.println("\n✅ TEST PASSED! Protocol works.");
                System.exit(0);
            } else {
                System.out.println("\n❌ TEST FAILED! Data mismatch.");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("\n❌ ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
'@

# Write test file
$testCode | Out-File -FilePath "SimpleTest.java" -Encoding ASCII

# Step 3: Compile test
Write-Host "`nStep 3: Compiling test..." -ForegroundColor Yellow
javac -cp "build/classes/main" SimpleTest.java 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Test compilation failed!" -ForegroundColor Red
    Remove-Item "SimpleTest.java" -Force -ErrorAction SilentlyContinue
    exit 1
}
Write-Host "✅ Test compiled successfully!" -ForegroundColor Green

# Step 4: Run test
Write-Host "`nStep 4: Running test..." -ForegroundColor Yellow
Write-Host "----------------------------------------"
java -cp ".;build/classes/main" SimpleTest
Write-Host "----------------------------------------"

# Step 5: Clean up
Remove-Item "SimpleTest.java" -Force -ErrorAction SilentlyContinue
Remove-Item "SimpleTest.class" -Force -ErrorAction SilentlyContinue

Write-Host "`nTest completed." -ForegroundColor Cyan