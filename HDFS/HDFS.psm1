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
		[ValidateRange(0, [System.Int64]::MaxValue)]
		[System.Int64]$Offset,

		[Parameter(ParameterSetName = "Open")]
		[ValidateRange(1, [System.Int64]::MaxValue)]
		[System.Int64]$Length,

		[Parameter(ParameterSetName = "Open")]
		[ValidateRange(1, [System.Int32]::MaxValue)]
		[System.Int32]$BufferSize,

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

		if ($Path.StartsWith("/"))
		{
			$Path = $Path.Substring(1)
		}

		[System.String]$Uri = "$($SessionInfo.BaseUrl)/$Path"

		switch ($PSCmdlet.ParameterSetName)
		{
			"Open" {
				$Uri += "?op=OPEN"

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
			if ($PSCmdlet.ParameterSetName -eq "Open")
			{
				[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell
			}
			else
			{
				[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -UserAgent PowerShell
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
			$Path = $Path.Substring(1)
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
	[OutputType([System.Management.Automation.PSCustomObject], [System.Boolean])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
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
			$Path = $Path.Substring(1)
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
					[Microsoft.PowerShell.Commands.WebResponseObject]$InitialResults = Invoke-WebRequest -Uri $Uri -Method Put -MaximumRedirection 0 -ErrorAction Stop -UserAgent PowerShell	
			
					if ($InitialResults.StatusCode -eq 307)
					{
						$Location = $InitialResults.Headers["Location"]

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

						[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Location -Method Put -ErrorAction Stop -UserAgent PowerShell	@ContentSplat

						$StatusCode = $Result.StatusCode
						$Reason = $Result.StatusDescription
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

				if ($StatusCode -eq 201)
				{
					if ($PassThru)
					{
						Write-Output -InputObject ([PSCustomObject[]](ConvertFrom-Json -InputObject $Result.Content).FileStatuses.FileStatus)
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
	[CmdletBinding()]
	[OutputType([System.Boolean])]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
		[System.String]$Path,

		[Parameter()]
		[Switch]$Recursive,

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
			$Path = $Path.Substring(1)
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

Function Add-HDFSItemContent {
	<#

	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
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
			$Path = $Path.Substring(1)
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

				[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Post -ErrorAction Stop -UserAgent PowerShell @ContentSplat

				$StatusCode = $Result.StatusCode
				$Reason = $Result.StatusDescription	
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
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[ValidateNotNull()]
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
			$Path = $Path.Substring(1)
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
		[System.String]$Path,

		[Parameter(Mandatory = $true)]
		[ValidateNotNullOrEmpty()]
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
			$Path = $Path.Substring(1)
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
			$Path = $Path.Substring(1)
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