# Requires -Modules @{ModuleName='SQLServer'; ModuleVersion='21.0.17178'}
# Requires -Modules @{ModuleName='Healthwise.HAB.MessageProxy'; ModuleVersion='1.0.0'}
# Requires -Modules @{ModuleName='Healthwise.Azure.SQLMaintenance'; ModuleVersion='3.0.0'}

Param(
  [Parameter(Mandatory=$True)] [string] $ServerName,
  [Parameter(Mandatory=$True)] [string] $ResourceGroupName,
  [Parameter(Mandatory=$True)] [string] $SQLAccountCredentialName,
  [string] $DBName = 'All',
  [array] $ExclusionList,
  [array] $ExcludeFromReOrg,
  [string] $MessageProxyCredentialName = 'UncleHAB-MessageProxy-Prod'
)

Function Send-Notification {
  Param(
    [Parameter(Mandatory = $True)]
    [string] $Message
  )
  # Force powershell to include TLS1.2 as a supported protocol
  [Net.ServicePointManager]::SecurityProtocol += "Tls12"

  # Output notification to job console
  $Date = Get-Date
  Write-Output "[$Date] $Message"

  # Send notification through Uncle HAB
  $MessageProxyKey = (Get-AutomationPSCredential -Name $MessageProxyCredentialName).GetNetworkCredential().Password
  Send-UncleHABMessage -Message $Message -FunctionKey $MessageProxyKey -Environment 'Prod'
}

Try
{
  # Authenticate using Azure Automation account
  $Conn = Get-AutomationConnection -Name AzureRunAsConnection
  Add-AzureRMAccount -ServicePrincipal -Tenant $Conn.TenantID -ApplicationId $Conn.ApplicationID -CertificateThumbprint $Conn.CertificateThumbprint

  # Get SQL credentials
  $Credential = Get-AutomationPSCredential -Name $SQLAccountCredentialName

  # Get list of databases and filter out any exclusions
  If ($DBName -eq 'All')
  {
    $DBs = Get-AzureRmSQLDatabase -ServerName $ServerName.ToLower() -ResourceGroupName $ResourceGroupName | Where-Object {$_.DatabaseName -ne 'Master'}
    If ($ExclusionList)
    {
      Foreach ($Exclusion in $ExclusionList)
      {
        $DBs = $DBs | Where-Object {$_.DatabaseName -ne $Exclusion}
      }
    }
  }
  Else
  {
    $DBs = Get-AzureRmSQLDatabase -ServerName $ServerName.ToLower() -ResourceGroupName $ResourceGroupName | Where-Object {$_.DatabaseName -eq $DBName}
  }

  If ($DBs.count -gt 0)
  {
    # Verify maintenance solution is installed, if not, install it.
    Write-Output "Verifying maintenance solution is installed on each database, if not the solution will be installed."
    $DBs | Install-DBMaintenanceSolution -Credential $Credential
    Write-Output "Finished verifying maintenance solution is installed on each database"

    Write-Output "Attempting to start index maintenance on each database"

    # Separate DBs excluded from index reorg from DBs using default commands
    $DBS_ExcludeReorg = $DBs | Where-Object {$ExcludeFromReOrg -Contains $_.DatabaseName}
    $DBS_Default = $DBs | Where-Object {$ExcludeFromReOrg -NotContains $_.DatabaseName}

    # Process databases excluded from index reorg
    Write-Output "Databases to exclude from Reorg operation: $($ExcludeFromReOrg -join ', ')"
    $DBS_ExcludeReorg | Invoke-DBMaintenance -SQLCMDCustomOptions @{FragmentationMedium = "'INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE'"} -Credential $Credential -ServerName $ServerName -MaintenanceType 'Index'

    # Initiate index maintenance for databases not being excluded from ReOrg
    $DBS_Default | Invoke-DBMaintenance -Credential $Credential -MaintenanceType 'Index' -ServerName $ServerName
  }
  Else
  {
    Write-Error "No databases assigned for maintenance on Server $ServerName in resource group $ResourceGroupName"
  }

  # Check for non-terminating errors and report them.
  If ($Error)
  {
    Send-Notification -Message $Error
  }

  Write-Output "Maintenance completed"
}
Catch
{
  Send-Notification -Message $Error
}