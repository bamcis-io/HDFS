[System.Collections.Hashtable]$script:Sessions = @{}

Function New-HDFSSession {
	<#


	#>
	[CmdletBinding(DefaultParameterSetName = "User")]
	[OutputType([System.String])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$Namenode,

		[Parameter()]
	    [ValidateRange(0, 65535)]
		[System.Int32]$Port = 50070,

		[Parameter()]
		[ValidateSet("v1")]
		[System.String]$Version = "v1",

		[Parameter(ParameterSetName = "User")]
		[ValidateNotNullOrEmpty()]
		[System.String]$Username = [System.String]::Empty,

		[Parameter(ParameterSetName = "Delegation")]
		[ValidateNotNullOrEmpty()]
		[System.String]$DelegationToken = [System.String]::Empty,

		[Parameter()]
		[Switch]$UseSsl
	)

	Begin {
	}

	Process {
		[System.String]$Scheme = if ($UseSsl) { "https" } else { "http" }
		
		[System.String]$HostPath = [System.String]::Empty

		if ($UseSsl -and $Port -eq 443)
		{
			$HostPath = $Namenode
		}
		elseif (-not $UseSsl -and $Port -eq 80)
		{
			$HostPath = $Namenode
		}
		else
		{
			$HostPath = "$Namenode`:$Port"
		}


		$Url = [System.String]$Url = "$Scheme`://$HostPath/webhdfs/$Version"

		$script:Sessions.Add($Namenode, @{Server = $Namenode; Port = $Port; Version = $Version; Scheme = $Scheme; BaseUrl = $Url})

		switch ($PSCmdlet.ParameterSetName)
		{
			"User" {
				$script:Sessions.Get_Item($Namenode).Add("User", $Username)
				break
			}
			"Delegation" {
				$script:Sessions.Get_Item($Namenode).Add("Delegation", $DelegationToken)
				break
			}
			"Kerberos" {

				break
			}
		}
	}

	End {

	}
}

Function Get-HDFSItem {
	<#

	#>
	[CmdletBinding(DefaultParameterSetName = "Status")]
	[OutputType([System.String], [System.Management.Automation.PSCustomObject])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[System.String]$Path,

		[Parameter(ParameterSetName = "Status")]
		[Switch]$Status,

		[Parameter(ParameterSetName = "Open")]
		[Switch]$Open,

		[Parameter()]
		[ValidateScript({
			$script:Sessions.ContainsKey($_.ToLower())
		})]
		[System.String]$Session = [System.String]::Empty
	)

	Begin {

	}

	Process {
		[System.Collections.Hashtable]$SessionInfo = $null

        if (-not [System.String]::IsNullOrEmpty($Session)) {
            $SessionInfo = $script:Sessions.Get_Item($Session)
        }
        else {
            $SessionInfo = $script:Sessions.GetEnumerator() | Select-Object -First 1 -ExpandProperty Value
			$Session = $SessionInfo.Server
        }

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path"

		switch ($PSCmdlet.ParameterSetName)
		{
			"Open" {
				$Uri += "?op=OPEN"
				break
			}
			"Status" {
				$Uri += "?op=GETFILESTATUS"
				break
			}
			default {
				throw "Unknown parameter set."
			}
		}

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try {
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell
			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			switch ($PSCmdlet.ParameterSetName)
			{
				"Open" {
					Write-Output -InputObject $Result.Content
					break
				}
				"Status" {
					Write-Output -InputObject ([PSCustomObject](ConvertFrom-Json -InputObject $Result.Content).FileStatus)
					break
				}
			}
		}
		else
		{
			Write-Warning -Message "There was an issue getting the item: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Get-HDFSChildItem {
	<#

	#>
	[CmdletBinding()]
	[OutputType([System.Management.Automation.PSCustomObject[]])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[System.String]$Path,

		[Parameter()]
		[ValidateScript({
			$script:Sessions.ContainsKey($_.ToLower())
		})]
		[System.String]$Session = [System.String]::Empty
	)

	Begin {

	}

	Process {
		[System.Collections.Hashtable]$SessionInfo = $null

        if (-not [System.String]::IsNullOrEmpty($Session)) {
            $SessionInfo = $script:Sessions.Get_Item($Session)
        }
        else {
            $SessionInfo = $script:Sessions.GetEnumerator() | Select-Object -First 1 -ExpandProperty Value
			$Session = $SessionInfo.Server
        }

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=LISTSTATUS"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try {
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell	
			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			Write-Output -InputObject ([PSCustomObject[]](ConvertFrom-Json -InputObject $Result.Content).FileStatuses.FileStatus)
		}
		else
		{
			Write-Warning -Message "There was an issue getting the child items item: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function New-HDFSItem {
	<#

	#>
	[CmdletBinding()]
	[OutputType([System.Management.Automation.PSCustomObject[]])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[System.String]$Path,

		[Parameter(Mandatory = $true, ValueFromPipeline = $true)]
		[ValidateNotNull()]
		[System.Object]$InputObject,

		[Parameter()]
		[Switch]$Overwrite,

		[Parameter()]
		[ValidateRange(1, [System.Int32]::MaxValue)]
		[System.Int32]$BlockSize,

		[Parameter()]
		[ValidateRange(1, [System.Int32]::MaxValue)]
		[System.Int32]$Replication,

		[Parameter()]
		[ValidateRange(0, 1777)]
		[System.Int32]$Permission = 755,

		[Parameter()]
		[ValidateRange(1, [System.Int32]::MaxValue)]
		[System.Int32]$BufferSize,

		[Parameter()]
		[ValidateScript({
			$script:Sessions.ContainsKey($_.ToLower())
		})]
		[System.String]$Session = [System.String]::Empty
	)

	Begin {

	}

	Process {
		[System.Collections.Hashtable]$SessionInfo = $null

        if (-not [System.String]::IsNullOrEmpty($Session)) {
            $SessionInfo = $script:Sessions.Get_Item($Session)
        }
        else {
            $SessionInfo = $script:Sessions.GetEnumerator() | Select-Object -First 1 -ExpandProperty Value
			$Session = $SessionInfo.Server
        }

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=CREATE"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		if ($Overwrite)
		{
			$Uri += "&overwrite=true"
		}

		if ($PSBoundParameters.ContainsKey("BlockSize"))
		{
			$Uri += "&blocksize=$BlockSize"
		}

		if ($PSBoundParameters.ContainsKey("Replication"))
		{
			$Uri += "&replication=$Replication"
		}

		if ($PSBoundParameters.ContainsKey("Permission"))
		{
			$Uri += "&permission=$Permission"
		}

		if ($PSBoundParameters.ContainsKey("BufferSize"))
		{
			$Uri += "&buffersize=$BufferSize"
		}

		try {
			[Microsoft.PowerShell.Commands.WebResponseObject]$InitialResults = Invoke-WebRequest -Uri $Uri -Method Put -MaximumRedirection 0 -ErrorAction Stop -UserAgent PowerShell	
			
			if ($InitialResults.StatusCode -eq 307)
			{
				$Location = $InitialResults.Headers["Location"]
				Write-Verbose -Message "Redirect location: $Location"

				$ContentSplat = @{}

				if ($InputObject.GetType().IsPrimitive -or 
					($InputObject.GetType().IsArray -and ($InputObject.GetType().GetElementType().IsPrimitive -or $InputObject.GetType().GetElementType() -eq [System.String[]])) -or 
					$InputObject.GetType() -eq [System.String])
				{
					$ContentSplat.Add("Body", $InputObject)
				}
				else
				{
					$ContentSplat.Add("Body", (ConvertTo-Json -InputObject $InputObject))
				}

				[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Location -Method Put -ErrorAction Stop -UserAgent PowerShell	@ContentSplat

				$StatusCode = $Result.StatusCode
				$Reason = $Result.StatusDescription
			}
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 201)
		{
			Write-Output -InputObject ([PSCustomObject[]](ConvertFrom-Json -InputObject $Result.Content).FileStatuses.FileStatus)
		}
		else
		{
			Write-Warning -Message "There was an issue creating the item: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}