function Read-EnvVariable {
    param (
        [string]$envFile,
        [string]$variableName
    )

    $envVars = @{}
    Get-Content -Path $envFile | ForEach-Object {
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
        [string]$workDir,
        [string]$logDir,
        [string]$logFilePath
    )

    try {
        Set-Location -Path $workDir
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
$envFile = ".env"
$workDir = Read-EnvVariable -envFile $envFile -variableName "workdir"
$logDir = Read-EnvVariable -envFile $envFile -variableName "logsdir"

$logFileName = "errorlog_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
$logFilePath = Join-Path -Path $logDir -ChildPath $logFileName
$executionLogPath = Join-Path -Path $logDir -ChildPath "execution_time.log"

$ErrorActionPreference = "Stop"
$startTime = Get-Date

ExecuteBackgroundJob -repoDir $workDir -logFilePath $logFilePath

$endTime = Get-Date
$timeTaken = $endTime - $startTime

$executionLog = "Script executed at: $($startTime.ToString('yyyy-MM-dd HH:mm:ss'))"
$executionLog += "`r`nTime taken: $($timeTaken.TotalSeconds) seconds"
$executionLog | Out-File -FilePath $executionLogPath

Write-Host "Execution details logged to $executionLogPath"
