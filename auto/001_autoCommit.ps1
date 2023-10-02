function Read-EnvVariable {
    param (
        [string]$envPath,
        [string]$variableName
    )

    $envPath = "../.env"
    if (!(Test-Path -Path $envPath)) {
        Write-Error "The .env file does not exist at path $envPath"
        Exit 1
    }

    $envVars = @{}
    Get-Content -Path $envPath | ForEach-Object {
        $key, $value = $_ -split '='
        $envVars[$key] = $value
    }

    return $envVars[$variableName]
}

function LogErrorHandling {
    param (
        [string]$errorMessage,
        [string]$logFilePath
    )

    $errorMessage | Out-File -Append -FilePath $logFilePath
    Write-Error $errorMessage
    Exit 1
}

function ExecuteBackgroundJob {
    param (
        [string]$repoDir,
        [string]$logDir,
        [string]$logFilePath
    )

    try {
        Set-Location -Path $repoDir
        Clear-Host
        git pull
        git add .
        $commitMessage = "Update some codes"
        git commit -m $commitMessage
        git push
    } catch {
        $errorMessage = "Error: $($_.Exception.Message)"
        LogErrorHandling -errorMessage $errorMessage -logFilePath $logFilePath
    }
}

# Main Script
$envPath = ".env"
$repoDir = Read-EnvVariable -envFile $envPath -variableName "autoCommitMain"
$logDir = Read-EnvVariable -envFile $envPath -variableName "autoCommitLogs"

# Check the environment variables whether it contains any quotation mark, because PowerShell could interpret it as part of a string rather than a variable which ends up with error.
    if ($logDir -match '^"' -or $repoDir -match '^"') {
        Write-Error "The logsdir variable in .env starts with a quotation mark"
        Exit 1
    }

$logFileName = "errorlog_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
$logFilePath = Join-Path -Path $logDir -ChildPath $logFileName
$executionLogPath = Join-Path -Path $logDir -ChildPath "auto_commit.log"

$ErrorActionPreference = "Stop"
$startTime = Get-Date

ExecuteBackgroundJob -repoDir $repoDir -logFilePath $logFilePath

$endTime = Get-Date
$timeTaken = $endTime - $startTime

$executionLog = "Script executed at: $($startTime.ToString('yyyy-MM-dd HH:mm:ss'))"
$executionLog += "`r`nTime taken: $($timeTaken.TotalSeconds) seconds"
$executionLog | Out-File -FilePath $executionLogPath

Write-Host "Execution details logged to $executionLogPath"
