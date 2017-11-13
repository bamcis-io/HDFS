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

		[Parameter(ParameterSetName = "Open")]
		[ValidateNotNull()]
		[System.Text.Encoding]$Encoding,

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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell
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
					[System.Byte[]]$Bytes = $Result.Content

					if ($PSBoundParameters.ContainsKey("Encoding"))
					{
						Write-Output -InputObject $Encoding.GetString($Bytes)
					}
					else
					{
						Write-Output -InputObject $Bytes
					}

					break
				}
				"Status" {
					$Stat = ([PSCustomObject](ConvertFrom-Json -InputObject $Result.Content).FileStatus)
					$Stat | Add-Member -MemberType NoteProperty -Name "name" -Value $Path
					Write-Output -InputObject $Stat
					break
				}
			}
		}
		else
		{
			Write-Warning -Message "There was an issue getting the item: $StatusCode $Reason - $([System.Text.Encoding]::UTF8.GetString($Result.Content))"
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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell	
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
			Write-Warning -Message "There was an issue getting the child items item: $StatusCode $Reason - $([System.Text.Encoding]::UTF8.GetString($Result.Content))"
		}
	}

	End {

	}
}

Function New-HDFSItem {
	<#

	#>
	[CmdletBinding()]
	[OutputType([System.Management.Automation.PSCustomObject], [System.Boolean])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[System.String]$Path,

		[Parameter(ValueFromPipeline = $true)]
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
		[ValidateSet("File", "Directory", "SymbolicLink")]
		[System.String]$ItemType = "File",

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

		switch ($ItemType)
		{
			"File" {
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
					Write-Warning -Message "There was an issue creating the item: $StatusCode $Reason - $([System.Text.Encoding]::UTF8.GetString($Result.Content))"
				}

				break
			}
			"Directory" {
				[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=MKDIRS"

				if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
				{
					$Uri += "&user.name=$($SessionInfo.User)"
				}
				elseif($SessionInfo.ContainsKey("Delegation"))
				{
					$Uri += "&delegation=$($SessionInfo.Delegation)"
				}

				if ($PSBoundParameters.ContainsKey("Permission"))
				{
					$Uri += "&permission=$Permission"
				}

				try {
					[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

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
					Write-Output -InputObject ([System.Boolean](ConvertFrom-Json -InputObject $Result.Content).boolean)
				}
				else
				{
					Write-Warning -Message "There was an issue creating the item: $StatusCode $Reason - $([System.Text.Encoding]::UTF8.GetString($Result.Content))"
				}

				break
			}
			"SymbolicLink" {
				[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=CREATESYMLINK&destination=$InputObject"

				if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
				{
					$Uri += "&user.name=$($SessionInfo.User)"
				}
				elseif($SessionInfo.ContainsKey("Delegation"))
				{
					$Uri += "&delegation=$($SessionInfo.Delegation)"
				}

				try {
					[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

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
					Write-Output -InputObject ([System.Boolean](ConvertFrom-Json -InputObject $Result.Content).boolean)
				}
				else
				{
					Write-Warning -Message "There was an issue creating the item: $StatusCode $Reason - $([System.Text.Encoding]::UTF8.GetString($Result.Content))"
				}
				break
			}
			default {
				throw "Unknown parameter set."
			}
		}
	}

	End {
	}
}