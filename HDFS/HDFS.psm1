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
	[OutputType([System.String], [System.Byte[]], [System.Management.Automation.PSCustomObject])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter(ParameterSetName = "Status")]
		[Switch]$Status,

		[Parameter(ParameterSetName = "Summary")]
		[Switch]$Summary,

		[Parameter(ParameterSetName = "Checksum")]
		[Switch]$Checksum,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path"

		switch ($PSCmdlet.ParameterSetName)
		{
			"Status" {
				$Uri += "?op=GETFILESTATUS"
				break
			}
			"Summary" {
				$Uri += "?op=GETCONTENTSUMMARY"
				break
			}
			"Checksum" {
				$Uri += "?op=GETFILECHECKSUM"
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
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			switch ($PSCmdlet.ParameterSetName)
			{
				"Status" {
					$Stat = ([PSCustomObject](ConvertFrom-Json -InputObject $Result.Content).FileStatus)
					$Stat | Add-Member -MemberType NoteProperty -Name "name" -Value $Path
					Write-Output -InputObject $Stat
					break
				}
				"Summary" {
					$Summary = ([PSCustomObject](ConvertFrom-Json -InputObject $Result.Content).ContentSummary)
					$Summary | Add-Member -MemberType NoteProperty -Name "name" -Value $Path
					Write-Output -InputObject $Summary
					break
				}
				"Checksum" {
					$Checksum = ([PSCustomObject](ConvertFrom-Json -InputObject $Result.Content).FileChecksum)
					$Checksum | Add-Member -MemberType NoteProperty -Name "name" -Value $Path
					Write-Output -InputObject $Checksum
					break
				}
			}
		}
		else
		{
			$Message = ""

			if ($Result -ne $null -and $Result.Content -ne $null)
			{
				if ($Result.Content.GetType() -eq [System.Byte[]] -and $Result.Content.Length -gt 0)
				{
					$Message = [System.Text.Encoding]::UTF8.GetString($Result.Content)
				}
				else
				{
					$Message = $Result.Content
				}
			}

			Write-Warning -Message "There was an issue getting the item: $StatusCode $Reason - $Message"
		}
	}

	End {

	}
}

Function Get-HDFSContent {
	<#

	#>
	[CmdletBinding()]
	[OutputType([System.String], [System.Byte[]])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[ValidateScript({
			$_.Length -gt 2
		})]
		[System.String]$Path,

		[Parameter()]
		[ValidateRange(0, [System.Int64]::MaxValue)]
		[System.Int64]$Offset,

		[Parameter()]
		[ValidateRange(1, [System.Int64]::MaxValue)]
		[System.Int64]$Length,

		[Parameter()]
		[ValidateRange(1, [System.Int32]::MaxValue)]
		[System.Int32]$BufferSize,

		[Parameter()]
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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=OPEN"

		if ($PSBoundParameters.ContainsKey("Offset"))
		{
			$Uri += "&offset=$Offset"
		}

		if ($PSBoundParameters.ContainsKey("Length"))
		{
			$Uri += "&length=$Length"
		}

		if ($PSBoundParameters.ContainsKey("BufferSize"))
		{
			$Uri += "&buffersize=$BufferSize"
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
			# Returns an octet stream
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			[System.Byte[]]$Bytes = $Result.Content

			if ($PSBoundParameters.ContainsKey("Encoding"))
			{
				Write-Output -InputObject $Encoding.GetString($Bytes)
			}
			else
			{
				Write-Output -InputObject $Bytes
			}
		}
		else
		{
			$Message = ""

			if ($Result -ne $null -and $Result.Content -ne $null)
			{
				if ($Result.Content.GetType() -eq [System.Byte[]] -and $Result.Content.Length -gt 0)
				{
					$Message = [System.Text.Encoding]::UTF8.GetString($Result.Content)
				}
				else
				{
					$Message = $Result.Content
				}
			}

			Write-Warning -Message "There was an issue getting the item: $StatusCode $Reason - $Message"
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
		[AllowEmptyString()]
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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
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
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
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
			Write-Warning -Message "There was an issue getting the child items: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Get-HDFSHomeDirectory {

	[CmdletBinding()]
	[OutputType([System.String])]
	Param(
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

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/?op=GETHOMEDIRECTORY"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try{
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell	

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			Write-Output -InputObject ([PSCustomObject[]](ConvertFrom-Json -InputObject $Result.Content).Path)
		}
		else
		{
			Write-Warning -Message "There was an issue getting the home directory: $StatusCode $Reason - $($Result.Content)"
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
		[ValidateNotNullOrEmpty()]
		[ValidateScript({
			$_.Length -gt 2
		})]
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
		[Switch]$PassThru,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
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
					# WebHDFS uses a two part process to create a file, the redirect provides the datanode via the location header
					# where the client will send the data to create the file
					[Microsoft.PowerShell.Commands.WebResponseObject]$RedirectResult = Invoke-WebRequest -Uri $Uri -Method Put -MaximumRedirection 0 -ErrorAction Stop -UserAgent PowerShell	
			
					if ($RedirectResult.StatusCode -eq 307)
					{
						$Location = $RedirectResult.Headers["Location"]

						Write-Verbose -Message "Redirect location: $Location"

						$ContentSplat = @{}

						# If it's a primitive type, string, or array of primitives or strings, send that data as is,
						# otherwise, convert the object to a JSON string and send that
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

						# No content returned
						[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Location -Method Put -ErrorAction Stop -UserAgent PowerShell	@ContentSplat

						$StatusCode = $Result.StatusCode
						$Reason = $Result.StatusDescription
					}
					else
					{
						$StatusCode = $RedirectResult.StatusCode
						$Reason = $RedirectResult.StatusDescription
					}
				}
				catch [System.Net.WebException] {
					[System.Net.HttpWebResponse]$Response = $_.Exception.Response
					$StatusCode = [System.Int32]$Response.StatusCode
					
					[System.IO.Stream]$Stream = $Response.GetResponseStream()
					[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
					[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
					$Content = $Reader.ReadToEnd()

					$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
				}
				catch [Exception]  {
					$Reason = $_.Exception.Message
				}

				if ($StatusCode -ne 201)
				{					
					$Message = ""

					if ($Result -ne $null -and $Result.Content -ne $null)
					{
						if ($Result.Content.GetType() -eq [System.Byte[]] -and $Result.Content.Length -gt 0)
						{
							$Message = [System.Text.Encoding]::UTF8.GetString($Result.Content)
						}
						else
						{
							$Message = $Result.Content
						}

						$Result.Dispose()
					}

					Write-Warning -Message "There was an issue creating the item: $StatusCode $Reason - $Message"
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
					# Returns a boolean
					[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

					$StatusCode = $Result.StatusCode
					$Reason = $Result.StatusDescription
				}
				catch [System.Net.WebException] {
					[System.Net.HttpWebResponse]$Response = $_.Exception.Response
					$StatusCode = [System.Int32]$Response.StatusCode

					[System.IO.Stream]$Stream = $Response.GetResponseStream()
					[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
					[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
					$Content = $Reader.ReadToEnd()

					$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
				}
				catch [Exception]  {
					$Reason = $_.Exception.Message
				}

				if ($StatusCode -eq 200)
				{
					if ($PassThru)
					{
						Write-Output -InputObject ([System.Boolean](ConvertFrom-Json -InputObject $Result.Content).boolean)
					}
				}
				else
				{
					Write-Warning -Message "There was an issue creating the item: $StatusCode $Reason - $($Result.Content)"
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
					# No content returned
					[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

					$StatusCode = $Result.StatusCode
					$Reason = $Result.StatusDescription
				}
				catch [System.Net.WebException] {
					[System.Net.HttpWebResponse]$Response = $_.Exception.Response
					$StatusCode = [System.Int32]$Response.StatusCode
					
					[System.IO.Stream]$Stream = $Response.GetResponseStream()
					[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
					[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
					$Content = $Reader.ReadToEnd()

					$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
				}
				catch [Exception]  {
					$Reason = $_.Exception.Message
				}

				if ($StatusCode -eq 200)
				{
					if ($PassThru)
					{
						Write-Output -InputObject ([System.Boolean](ConvertFrom-Json -InputObject $Result.Content).boolean)
					}
				}
				else
				{
					Write-Warning -Message "There was an issue creating the item: $StatusCode $Reason - $($Result.Content)"
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

Function Remove-HDFSItem {
	<#

	#>
	[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "HIGH")]
	[OutputType([System.Boolean])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter()]
		[Switch]$Recursive,

		[Parameter()]
		[Switch]$PassThru,

		[Parameter()]
		[Switch]$Force,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=DELETE"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		if ($Recursive)
		{
			$Uri += "&recursive=true"
		}

		try {
			# Returns a boolean
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Delete -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}
		
		if ($StatusCode -eq 200)
		{
			if ($PassThru)
			{
				Write-Output -InputObject ([System.Boolean](ConvertFrom-Json -InputObject $Result.Content).boolean)
			}
		}
		else
		{
			Write-Warning -Message "There was an issue deleting the item: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Add-HDFSContent {
	<#

	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[ValidateScript({
			$_.Length -gt 2
		})]
		[System.String]$Path,

		[Parameter(ValueFromPipeline = $true)]
		[ValidateNotNull()]
		[System.Object]$InputObject,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=APPEND"

		if ($PSBoundParameters.ContainsKey("BufferSize"))
		{
			$Uri += "&buffersize=$BufferSize"
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
			[Microsoft.PowerShell.Commands.WebResponseObject]$RedirectResult = Invoke-WebRequest -Uri $Uri -MaximumRedirection 0 -Method Post -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $RedirectResult.StatusCode
			$Reason = $RedirectResult.StatusDescription
			
			if ($StatusCode -eq 307)
			{
				$Location = $RedirectResult.Headers["Location"]

				Write-Verbose -Message "Redirect location: $Location"

				$ContentSplat = @{}

				# If it's a primitive type, string, or array of primitives or strings, send that data as is,
				# otherwise, convert the object to a JSON string and send that
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

				# No content returned
				[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Location -Method Post -ErrorAction Stop -UserAgent PowerShell @ContentSplat

				$StatusCode = $Result.StatusCode
				$Reason = $Result.StatusDescription	
			}
			else
			{
				$StatusCode = $RedirectResult.StatusCode
				$Reason = $RedirectResult.StatusDescription
			}
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -ne 200)
		{
			Write-Warning -Message "There was an issue appending to the item: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Merge-HDFSItem {
	<#

	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[ValidateScript({
			$_.Length -gt 2
		})]
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String[]]$Sources,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=CONCAT&paths=$([System.String]::Join(",", $Sources))"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Post -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -ne 200)
		{
			Write-Warning -Message "There was an issue concatenating the items: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Rename-HDFSItem {
	<#

	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[ValidateScript({
			$_.Length -gt 2
		})]
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[ValidateScript({
			$_.Length -gt 2
		})]
		[System.String]$NewName,

		[Parameter()]
		[Switch]$PassThru,

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

		if (-not $NewName.StartsWith("/"))
		{
			$NewName = "/$NewName"
		}

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=RENAME&destination=$NewName"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			if ($PassThru)
			{
				Write-Output -InputObject ([System.Boolean](ConvertFrom-Json -InputObject $Result.Content).boolean)
			}
		}
		else
		{
			Write-Warning -Message "There was an issue renaming the item: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Resize-HDFSItem {
	<#

	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[ValidateScript({
			$_.Length -gt 2
		})]
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[ValidateRange(1, [System.Int64]::MaxValue)]
		[System.Int64]$NewLength,

		[Parameter()]
		[Switch]$PassThru,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=TRUNCATE&newlength=$NewLength"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			# Returns a boolean
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Post -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			if ($PassThru)
			{
				Write-Output -InputObject ([System.Boolean](ConvertFrom-Json -InputObject $Result.Content).boolean)
			}
		}
		else
		{
			Write-Warning -Message "There was an issue truncating the item: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Set-HDFSItem {
	<#


	#>
	[CmdletBinding()]
	[OutputType([System.Boolean])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter(ParameterSetName = "Permission")]
		[ValidateRange(1, 1777)]
		[System.Int32]$Permission,

		[Parameter(ParameterSetName = "Owner")]
		[ValidateNotNullOrEmpty()]
		[System.String]$Owner,

		[Parameter(ParameterSetName = "Group")]
		[ValidateNotNullOrEmpty()]
		[System.String]$Group,

		[Parameter(ParameterSetName = "Replication")]
		[System.Int16]$ReplicationFactor,

		[Parameter()]
		[Switch]$PassThru,

		[Parameter(ParameterSetName = "Access")]
		[ValidateNotNull()]
		[System.DateTime]$AccessTime,

		[Parameter(ParameterSetName = "Modify")]
		[ValidateNotNull()]
		[System.DateTime]$ModificationTime,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path"

		switch ($PSCmdlet.ParameterSetName)
		{
			"Permission" {
				$Uri += "?op=SETPERMISSION&permission=$Permission"
				break
			}
			"Owner" {
				$Uri += "?op=SETOWNER&owner=$Owner"
				break
			}
			"Group" {
				$Uri += "?op=SETOWNER&group=$Group"
				break
			}
			"Replication" {
				$Uri += "?op=SETREPLICATION&replication=$ReplicationFactor"
				break
			}
			"Modify" {
				$1970 = New-Object -TypeName System.DateTime(1970, 1, 1, 0, 0, 0, [System.DateTimeKind]::Utc)
				$Diff = ($ModificationTime - $1970).TotalSeconds

				# A -1 keeps the time unchanged
				if ($Diff -lt 0)
				{
					$Diff = -1
				}

				$Uri += "?op=SETTIMES&modificationtime=$Diff"
				break
			}
			"Access" {
				$1970 = New-Object -TypeName System.DateTime(1970, 1, 1, 0, 0, 0, [System.DateTimeKind]::Utc)
				$Diff = ($AccessTime - $1970).TotalSeconds

				# A -1 keeps the time unchanged
				if ($Diff -lt 0)
				{
					$Diff = -1
				}

				$Uri += "?op=SETTIMES&accesstime=$Diff"
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

		try
		{
			if ($PSCmdlet.ParameterSetName -eq "Replication")
			{
				# Returns a boolean
				[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell
			}
			else
			{
				# No content returned for all other changes
				[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell
			}

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			if ($PSCmdlet.ParameterSetName -eq "Replication" -and $PassThru)
			{
				Write-Output -InputObject ([System.Boolean](ConvertFrom-Json -InputObject $Result.Content).boolean)
			}
		}
		else
		{
			Write-Warning -Message "There was an issue updating the item: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}

Function Set-HDFSAcl {
	<#


	#>
	[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "HIGH")]
	[OutputType([System.Boolean])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter(Mandatory = $true, ParameterSetName = "Update")]
		[Parameter(Mandatory = $true, ParameterSetName = "Replace")]
		[Parameter(Mandatory = $true, ParameterSetName = "Remove")]
		[System.String[]]$Acl = @(),

		[Parameter(ParameterSetName = "Update")]
		[Switch]$Update,

		[Parameter(ParameterSetName = "Replace")]
		[Switch]$Replace,

		[Parameter(ParameterSetName = "Remove")]
		[Switch]$Remove,

		[Parameter(ParameterSetName = "Default")]
		[Switch]$RemoveDefaultAcl,

		[Parameter(ParameterSetName = "RemoveAll")]
		[Switch]$RemoveAll,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path"

		$AclString = [System.String]::Empty

		if ($Acl.Length -gt 0)
		{
			$AclString = [System.String]::Join(",", $Acl)
		}

		switch ($PSCmdlet.ParameterSetName)
		{
			"Update" {
				$Uri += "?op=MODIFYACLENTRIES&aclspec=$Acl"
				break
			}
			"Replace" {
				$Uri += "?op=SETACL&aclspec=$Acl"
				break
			}
			"Remove" {
				$Uri += "?op=REMOVEACLENTRIES&aclspec=$Acl"
				break
			}
			"Default" {
				$Uri += "?op=REMOVEDEFAULTACL"
				break
			}
			"RemoveAll" {
				$Uri += "?op=REMOVEACL"
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

		try
		{
			# No content returned
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -ne 200)
		{			
			Write-Warning -Message "There was an issue modifying the item's ACL: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}

Function Get-HDFSAcl {
	<#

	#>
	[CmdletBinding()]
	[OutputType([PSCustomObject])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=GETACLSTATUS"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			Write-Output -InputObject ([PSCustomObject](ConvertFrom-Json -InputObject $Result.Content).AclStatus)
		}
		else
		{
			Write-Warning -Message "There was an issue getting the acl status: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Test-HDFSAccess {
	<#

	#>
	[CmdletBinding()]
	[OutputType([System.Boolean])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[ValidatePattern("[r\-][w\-][x\-]")]
		[System.String]$Action,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=CHECKACCESS&fsaction=$Action"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			Write-Output -InputObject $true
		}
		else
		{
			Write-Output -InputObject $false
			Write-Verbose -Message "The access test was unsuccessful: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Get-HDFSStoragePolicy {
	<#

	#>
	[CmdletBinding(DefaultParameterSetName = "All")]
	[OutputType([System.Management.Automation.PSCustomObject], [System.Management.Automation.PSCustomObject[]])]
	Param(
		[Parameter(ParameterSetName = "Path", Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
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

		[System.String]$Uri = "$($SessionInfo.BaseUrl)"

		switch ($PSCmdlet.ParameterSetName)
		{
			"All" {
				$Uri += "?op=GETALLSTORAGEPOLICY"
				break
			}
			"Path" {
				if ($Path.StartsWith("/"))
				{
					if ($Path.Length -gt 1)
					{
						$Path = $Path.Substring(1)
					}
					else
					{
						$Path = [System.String]::Empty
					}
				}

				$Uri += "/$Path`?op=GETSTORAGEPOLICY"
				break
			}
			default {
				throw "Unknown parameter set"
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

		try
		{
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			switch ($PSCmdlet.ParameterSetName)
			{
				"All" {
					Write-Output -InputObject ([PSCustomObject[]](ConvertFrom-Json -InputObject $Result.Content).BlockStoragePolicies.BlockStoragePolicy)
					break
				}
				"Path" {
					Write-Output -InputObject ([PSCustomObject](ConvertFrom-Json -InputObject $Result.Content).BlockStoragePolicy)
					break
				}
			}
		}
		else
		{
			Write-Warning -Message "The was an issue retrieving the storage policies: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}

Function Set-HDFSStoragePolicy {
	<#

	#>
	[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "HIGH")]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[System.String]$Policy,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=SETSTORAGEPOLICY&storagepolicy=$Policy"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			# No content returned
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -ne 200)
		{
			Write-Warning -Message "The was an issue setting the storage policy: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Remove-HDFSStoragePolicy {
	<#

	#>
	[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "HIGH")]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=UNSETSTORAGEPOLICY"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			# No content returned
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Post -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -ne 200)
		{
			Write-Warning -Message "The was an issue unsetting the storage policy: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Get-HDFSXAttr {
	<#

	#>
	[CmdletBinding(DefaultParameterSetName = "Name")]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter(ParameterSetName = "Name")]
		[ValidateNotNullOrEmpty()]
		[System.String[]]$Names = @(),

		[Parameter(ParameterSetName = "Name")]
		[ValidateSet("TEXT", "HEX", "BASE64")]
		[System.String]$Encoding = "TEXT",

		[Parameter(ParameterSetName = "List")]
		[Switch]$ListAvailable,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path"

		switch ($PSCmdlet.ParameterSetName)
		{
			"Name" {
				$Uri += "?op=GETXATTRS"

				if ($Names.Length -gt 0)
				{
					foreach ($Name in $Names)
					{
						$Uri += "&xattr.name=$Name"
					}
				}

				# Encoding is mandatory
				$Uri += "&encoding=$Encoding"
				
				break
			}
			"List" {
				$Uri += "?op=LISTXATTRS"

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

		try
		{			
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			switch ($PSCmdlet.ParameterSetName)
			{
				"Name" {
					Write-Output -InputObject ([PSCustomObject[]](ConvertFrom-Json -InputObject $Result.Content).XAttrs)
					
					break
				}
				"List" {
					Write-Output -InputObject ([System.String[]](ConvertFrom-Json -InputObject $Result.Content).XAttrNames)

					break
				}
			}
		}
		else
		{
			Write-Warning -Message "The was an issue getting the extended attribute: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}

Function Set-HDFSXAttr {
	<#

	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[ValidatePattern("(?:user|trusted|security|system|raw)\..*")]
		[System.String]$Name,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$Value,

		[Parameter(Mandatory = $true)]
		[ValidateSet("CREATE", "REPLACE")]
		[System.String]$Flag,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=SETXATTR&xattr.name=$Name&xattr.value=$Value"

		if ($PSBoundParameters.ContainsKey("Flag"))
		{
			$Uri += "&flag=$Flag"
		}

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			# No content returned
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -ne 200)
		{
			Write-Warning -Message "The was an issue setting the extended attribute: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function Remove-HDFSXAttr {
	<#

	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$Name,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=REMOVEXATTR&xattr.name=$Name"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			# No content returned
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -ne 200)
		{
			Write-Warning -Message "The was an issue removing the extended attribute: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {

	}
}

Function New-HDFSSnapshot {
	<#

	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter()]
		[ValidateNotNullOrEmpty()]
		[System.String]$Name,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=CREATESNAPSHOT"

		if ($PSBoundParameters.ContainsKey("Name"))
		{
			$Uri += "&snapshotname=$Name"
		}

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			# Returns the path to the snapshot
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			Write-Output -InputObject ([System.String](ConvertFrom-Json -InputObject $Result.Content).Path)
		}
		else
		{
			Write-Warning -Message "The was an issue creating the snapshot: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}

Function Remove-HDFSSnapshot {
	<#

	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$Name,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=DELETESNAPSHOT&snapshotname=$Name"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			# No content returned
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Delete -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -ne 200)
		{
			Write-Warning -Message "The was an issue deleting the snapshot: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}

Function Rename-HDFSSnapshot {
	<#

	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[AllowEmptyString()]
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$Name,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$NewName,

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

		if ($Path.StartsWith("/"))
		{
			if ($Path.Length -gt 1)
			{
				$Path = $Path.Substring(1)
			}
			else
			{
				$Path = [System.String]::Empty
			}
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path`?op=RENAMESNAPSHOT&oldsnapshotname=$Name&snapshotname=$NewName"

		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}

		try
		{
			# No content returned
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -ne 200)
		{
			Write-Warning -Message "The was an issue renaming the snapshot: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}

Function Get-HDFSDelegationToken {
	<#
		.PARAMETER Kind
			A string that represents token kind e.g “HDFS_DELEGATION_TOKEN” or “WEBHDFS delegation”

		.PARAMETER Service
			The name of the service where the token is supposed to be used, e.g. ip:port of the namenode
	#>
	[CmdletBinding()]
	[OutputType([System.String])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$User,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$Service,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$Kind,

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

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/?op=GETDELEGATIONTOKEN&renewer=$User&service=$Service&kind=$Kind"

		<#
		if ($SessionInfo.ContainsKey("User") -and -not [System.String]::IsNullOrEmpty($SessionInfo.User))
		{
			$Uri += "&user.name=$($SessionInfo.User)"
		}
		elseif($SessionInfo.ContainsKey("Delegation"))
		{
			$Uri += "&delegation=$($SessionInfo.Delegation)"
		}
		#>

		try
		{
			# Returns the token
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			Write-Output -InputObject ([System.String](ConvertFrom-Json -InputObject $Result.Content).Token.urlString)
		}
		else
		{
			Write-Warning -Message "The was an issue getting the delegation token: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}

Function Update-HDFSDelegationToken {
	<#
	
	#>
	[CmdletBinding()]
	[OutputType([System.Int64])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$Token,

		[Parameter()]
		[Switch]$PassThru,

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

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/?op=RENEWDELEGATIONTOKEN&token=$Token"

		try
		{
			# Returns the updated expiration time
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -eq 200)
		{
			if ($PassThru)
			{
				Write-Output -InputObject ([System.Int64](ConvertFrom-Json -InputObject $Result.Content).long)
			}
		}
		else
		{
			Write-Warning -Message "The was an issue getting the delegation token: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}

Function Revoke-HDFSDelegationToken {
	<#
	
	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
		[System.String]$Token,

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

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/?op=CANCELDELEGATIONTOKEN&token=$Token"

		try
		{
			# No content returned
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell

			$StatusCode = $Result.StatusCode
			$Reason = $Result.StatusDescription	
		}
		catch [System.Net.WebException] {
			[System.Net.HttpWebResponse]$Response = $_.Exception.Response
			$StatusCode = [System.Int32]$Response.StatusCode
			
			[System.IO.Stream]$Stream = $Response.GetResponseStream()
			[System.Text.Encoding]$Encoding = [System.Text.Encoding]::GetEncoding("utf-8")
			[System.IO.StreamReader]$Reader = New-Object -TypeName System.IO.StreamReader($Stream, $Encoding)
			$Content = $Reader.ReadToEnd()

			$Reason = "$($Response.StatusDescription) $($_.Exception.Message)`r`n$Content"
		}
		catch [Exception]  {
			$Reason = $_.Exception.Message
		}

		if ($StatusCode -ne 200)
		{
			Write-Warning -Message "The was an issue cancelling the delegation token: $StatusCode $Reason - $($Result.Content)"
		}
	}

	End {
	}
}