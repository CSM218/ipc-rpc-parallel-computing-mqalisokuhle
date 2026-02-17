# compile.ps1
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "CSM218 DIRECT COMPILATION SCRIPT" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Create build directory
$buildDir = "build/classes/main"
New-Item -ItemType Directory -Path $buildDir -Force | Out-Null
Write-Host "✅ Created build directory: $buildDir" -ForegroundColor Green

# Find all Java files
Write-Host "`nSearching for Java files..." -ForegroundColor Yellow
$javaFiles = Get-ChildItem -Path "src/main/java" -Recurse -Filter *.java | ForEach-Object { $_.FullName }

if ($javaFiles.Count -eq 0) {
    Write-Host "❌ No Java files found!" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Found $($javaFiles.Count) Java files" -ForegroundColor Green

# Create a temporary file with all source files (properly formatted for javac)
$sourcesFile = "sources.txt"
$javaFiles | ForEach-Object { $_ } | Set-Content -Path $sourcesFile -Encoding ASCII

Write-Host "`nCompiling Java files..." -ForegroundColor Yellow

# Compile with javac using the argument file [citation:5][citation:8]
# Note: The backtick before @ is important in PowerShell [citation:5]
$compileCommand = "javac -d $buildDir `@$sourcesFile"
Write-Host "Running: $compileCommand" -ForegroundColor Gray

$result = Invoke-Expression $compileCommand 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ COMPILATION SUCCESSFUL!" -ForegroundColor Green
    Write-Host "   Class files are in: $buildDir" -ForegroundColor Green
} else {
    Write-Host "❌ COMPILATION FAILED!" -ForegroundColor Red
    Write-Host $result -ForegroundColor Red
    exit 1
}

# Clean up temporary file
Remove-Item $sourcesFile -Force -ErrorAction SilentlyContinue

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "COMPILATION COMPLETE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan