[System.Collections.Hashtable]$script:Sessions = @{}

Function New-HDFSSession {
	<#
		.SYNOPSIS
			Creates a new HDFS session with the specified name node. These settings are used in all subsequent API calls.

		.DESCRIPTION
			This cmdlet creates a new HDFS session to the specified name node on the specified port. You may also specify the username or
			delegation token to use with all requests. If you choose to "Initialize" the session, a call to get the HomeDirectory is made to
			retrieve the hadoop.auth Cookie, which is then used on all subsequent calls. 

			Multiple sessions can be established, each with a different namenode. This allows you to specify the Session identifier (the namenode value
			supplied) in HDFS cmdlets to interact with different namenodes in the same script. You can loop through sessions and perform the same cmdlet
			on different HDFS systems easily.

		.PARAMETER Namenode
			The namenode IP or hostname with which all communication starts.

		.PARAMETER Port
			The port to use for namenode communication. This defaults to 50070.

		.PARAMETER Version
			The version of the REST API to use. Currently, only v1 is available and is the default.

		.PARAMETER Username
			The username to use during transactions.

		.PARAMETER DelegationToken
			The delegation token to use during transactions.

		.PARAMETER KerberosCredentials
			The Base64 encoded credentials to use with kerberos authentication. This string is supplied in the NEGOTIATE authorization header. 

			THIS PARAMETER HAS NOT BEEN TESTED.

		.PARAMETER UseSsl
			Specify to use HTTPS connections.

		.PARAMETER Initialize
			If this parameter is specified, a request to retrieve the Home Directory is performed in order to retrieve the hadoop.auth cookie that will
			be included in each subsequent transaction. If this is not specified, the cookie will be assigned on next REST API call.

		.PARAMETER PassThru
			Returns the key this session is being stored as. The session identifier can be used to make calls to different namenodes in single script by 
			supplying it to the Session parameter.

		.EXAMPLE
			New-HDFSSession -Namenode "hdserver" -Username "hdadmin"
			
			Establishes a new session with the namenode "hdserver" using the user "hdadmin".

		.INPUTS
			None

		.OUTPUTS
			None or System.String
		
		.NOTES
			AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
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

		[Parameter(ParameterSetName = "Kerberos")]
		[ValidateNotNullOrEmpty()]
		[System.String]$KerberosCredentials,

		[Parameter()]
		[Switch]$UseSsl,

		[Parameter()]
		[Switch]$Initialize,

		[Parameter()]
		[Switch]$PassThru
	)

	Begin {
	}

	Process {
		if (-not $script:Sessions.ContainsKey($Namenode))
		{
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
			[Microsoft.PowerShell.Commands.WebRequestSession]$Session = New-Object -TypeName Microsoft.PowerShell.Commands.WebRequestSession
			$Session.UserAgent = "PowerShell"

			$script:Sessions.Add($Namenode, @{Server = $Namenode; Port = $Port; Version = $Version; Scheme = $Scheme; BaseUrl = $Url; Session = $Session;})
		
			[System.String]$Uri = "$Url/?op=GETHOMEDIRECTORY"

			switch ($PSCmdlet.ParameterSetName)
			{
				"User" {
					$script:Sessions.Get_Item($Namenode).Add("User", $Username)
					$Uri += "&user.name=$Username"
					break
				}
				"Delegation" {
					$script:Sessions.Get_Item($Namenode).Add("Delegation", $DelegationToken)
					$Uri += "&delegation=$DelegationToken"
					break
				}
				"Kerberos" {
					$script:Sessions.Get_Item($Namenode).Session.Headers.Add("Authorization: NEGOTIATE $KerberosCredentials")
					break
				}
			}

			if ($Initialize)
			{		
				try{
					[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $script:Sessions.Get_Item($Namenode).Session

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
					Write-Warning -Message "There was an issue initializing the HDFS session: $StatusCode $Reason - $($Result.Content)"
				}
			}

			if ($PassThru)
			{
				Write-Output -InputObject $Namenode
			}
		}
		else
		{
			Write-Warning -Message "There is already a session for $Namenode, please remove this session with 'Remove-HDFSSession -Session $Namenode' in order to setup a new session."
		}
	}

	End {

	}
}

Function Remove-HDFSSession {
	<#
        .SYNOPSIS
			Removes a stored HDFS session.

        .DESCRIPTION
            The cmdlet removes an established HDFS session by its Id.

        .PARAMETER Session
            Specifies the unique identifier of the session to remove.

        .EXAMPLE
            Remove-HDFSSession -Session hdserver

            Removes the persisted session information for hdserver.

        .INPUTS
            None or System.String

        .OUTPUTS
            None

        .NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
    #>
	[CmdletBinding()]
	[OutputType([System.String])]
	Param(
		[Parameter(Mandatory = $true, ValueFromPipeline = $true)]
		[ValidateNotNullOrEmpty()]
		[ValidateScript({
			$script:Sessions.ContainsKey($_.ToLower())
		})]
		[System.String]$Session
	)

	Begin {
	}

	Process {
		$script:Sessions.Remove($Session)
	}

	End {

	}
}

Function Get-HDFSSession {
	<#
        .SYNOPSIS
            Gets stored HDFS session information.

        .DESCRIPTION
            The cmdlet retrieves an established HDFS session by its Id, or lists all active sessions.

        .PARAMETER Session
            Specifies the unique identifier of the session to query. If this parameter is not specified, all stored sessions are returned.

        .EXAMPLE
            Get-HDFSSession

            Gets all HDFS session information stored in the script variable.

        .INPUTS
            None or System.String

        .OUTPUTS
            System.Collections.Hashtable

        .NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
    #>
	[CmdletBinding()]
	[OutputType([System.Collections.Hashtable])]
	Param(
		[Parameter(ValueFromPipeline = $true)]
		[ValidateNotNullOrEmpty()]
		[ValidateScript({
			$script:Sessions.ContainsKey($_.ToLower())
		})]
        [System.String]$Session = [System.String]::Empty
	)

	Begin {
	}

	Process {
		if (-not [System.String]::IsNullOrEmpty($Session)) {
			if ($script:Sessions.ContainsKey($Session)) {
				Write-Output -InputObject $script:Sessions.Get_Item($Session)
			}
            else {
                Write-Output -InputObject $null
            }
		}
		else {
			Write-Output -InputObject $script:Sessions
		}
	}

	End {
	}
}

Function Get-HDFSItem {
	<#
		.SYNOPSIS
			Gets an HDFS item.

		.DESCRIPTION
			This cmdlet gets ths status, summary, or checksum of an HDFS file or directory.

		.PARAMETER Path
			The path to the item. This can be blank and doesn't need to be prefaced with a '/', but can be.

		.PARAMETER Status
			Gets the status of the HDFS item. This is the default.

		.PARAMETER Summary
			Gets a summary of the HDFS directory. If the path specified is not a directory, this will produce an error.

		.PARAMETER Checksum
			Gets a checksum of the HDFS item. The response will include the algorithm, bytes of the hash, and length.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			Get-HDFSItem -Path "/out/text.txt"

			Gets the status of the supplied path item.

		.INPUTS
			System.String

			The path can be piped to this cmdlet.

		.OUTPUTS
			System.String, System.Management.Automation.PSCustomObject

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
	#>
	[CmdletBinding(DefaultParameterSetName = "Status")]
	[OutputType([System.String], [System.Management.Automation.PSCustomObject])]
	Param(
		[Parameter(Mandatory = $true, ValueFromPipeline = $true)]
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

			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session

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
		.SYNOPSIS
			Gets the content of an HDFS file

		.DESCRIPTION
			This cmdlet gets the content of an HDFS file. If an encoding is specified, this is returned as a string, otherwise
			it is returned as a byte array.

		.PARAMETER Path
			The path to the item. This can be blank and doesn't need to be prefaced with a '/', but can be.

		.PARAMETER Offset
			The offset in number of bytes of the file to start retrieving data.

		.PARAMETER Length
			The amount of content in bytes to retrieve

		.PARAMETER Buffersize
			The size of the buffer to use to retrieve the content.

		.PARAMETER Encoding
			The encoding to use to translate the returned byte stream.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			Get-HDFSContent -Path "/out/text.txt" -Encoding ([System.Text.Encoding]::UTF8)

			Retrieves the content of /out/text.txt and decods the bytes as UTF8.

		.INPUTS
			System.String

			The path can be piped to this cmdlet.

		.OUTPUTS
			System.String, System.Byte[]

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
	#>
	[CmdletBinding()]
	[OutputType([System.String], [System.Byte[]])]
	Param(
		[Parameter(Mandatory = $true, ValueFromPipeline = $true)]
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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session

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
		.SYNOPSIS
			Gets the child items on an HDFS directory.

		.DESCRIPTION
			This cmdlet gets a listing of the statuses of the direct child items of an HDFS directory.

		.PARAMETER Path
			The path to the item. This can be blank and doesn't need to be prefaced with a '/', but can be.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			Get-HDFSChildItem -Path "/out" 

			Retrieves contents of the /out directory

		.INPUTS
			System.String

			The path can be piped to this cmdlet.

		.OUTPUTS
			System.Management.Automation.PSCustomObject[]

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
	#>
	[CmdletBinding()]
	[OutputType([System.Management.Automation.PSCustomObject[]])]
	Param(
		[Parameter(Mandatory = $true, ValueFromPipeline = $true)]
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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session
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
	<#
		.SYNOPSIS
			Gets the HDFS home directory.

		.DESCRIPTION
			This cmdlet gets the currently configured HDFS home directory.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			Get-HDFSHomeDirectory

			Gets the HDFS home directory.

		.INPUTS
			None

		.OUTPUTS
			System.String

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
	#>
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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session

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
		.SYNOPSIS
			Creates a new HDFS file or directory.

		.DESCRIPTION
			This cmdlet creates a new HDFS file with the provided content, an HDFS directory, or a symbollic link.

		.PARAMETER Path
			The path to the item. This can be blank and doesn't need to be prefaced with a '/', but can be.

		.PARAMETER InputObject
			The content to be written to the new HDFS file. If the item type is a directory, this parameter is ignored.

		.PARAMETER Overwrite
			If this is specified, if the specified path already exists, it will be overwritten.

		.PARAMETER BlockSize
			The block size to use for the new item.

		.PARAMETER Replication
			The replication factor for the item, i.e. how many replicas of the item will be maintained.

		.PARAMETER Permission
			The permissions in OCTAL form for the item, this defaults to 755.

		.PARAMETER BufferSize
			The size of the buffer to use to write the content.

		.PARAMETER ItemType
			The type of the item to create, either a file, directory, or symbollic link.

		.PARAMETER PassThru,
			If specified, for a directory or symbollic link, a boolean is returned indicating whether the item was successfully created.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			New-HDFSItem -Path "/input" -ItemType Directory

			Creates a new directory called input.

		.EXAMPLE
			New-HDFSItem -Path "/input/test.txt." -InputObject "TEST"

			Creates a new file with the string content "TEST".

		.INPUTS
			System.Object

			The data to be written to the new file can be piped to this cmdlet.

		.OUTPUTS
			None or System.Boolean

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
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
					[Microsoft.PowerShell.Commands.WebResponseObject]$RedirectResult = Invoke-WebRequest -Uri $Uri -Method Put -MaximumRedirection 0 -ErrorAction Stop -WebSession $SessionInfo.Session
			
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
						[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Location -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session	@ContentSplat

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
					[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session

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
					[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session

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
		.SYNOPSIS
			Removes an HDFS item.

		.DESCRIPTION
			This cmdlet deletes an HDFS item.

		.PARAMETER Path
			The path to the item. This can be blank and doesn't need to be prefaced with a '/', but can be.

		.PARAMETER Recursive
			If the item is a path, if this is specified, deletes all child items as well.

		.PARAMETER PassThru
			If specified, a boolean will be returned indicating the status of the deletion.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			Remove-HDFSItem -Path "/out" -Recursive

			Recursively deletes the folder /out and all its contents.

		.INPUTS
			System.String

			The path can be piped to this cmdlet.

		.OUTPUTS
			None or System.Boolean

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
	#>
	[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "HIGH")]
	[OutputType([System.Boolean])]
	Param(
		[Parameter(Mandatory = $true, ValueFromPipeline = $true)]
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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Delete -ErrorAction Stop -WebSession $SessionInfo.Session

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
		.SYNOPSIS
			Appends content to an existing HDFS file.

		.DESCRIPTION
			This cmdlet will append content to an existing HDFS file.

		.PARAMETER Path
			The path to the item. This can be blank and doesn't need to be prefaced with a '/', but can be.

		.PARAMETER InputObject
			The content to be appended to the existing HDFS file.

		.PARAMTER BufferSize
			The buffer size used to write the file.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			Add-HDFSContent -Path "/input/test.txt" -InputObject "`nTEST2"

			Adds a new line "TEST2" to the test.txt file.

		.INPUTS
			System.Object

			The content to be appended can be piped to this cmdlet.

		.OUTPUTS
			None

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
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
			[Microsoft.PowerShell.Commands.WebResponseObject]$RedirectResult = Invoke-WebRequest -Uri $Uri -MaximumRedirection 0 -Method Post -ErrorAction Stop -WebSession $SessionInfo.Session

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
				[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Location -Method Post -ErrorAction Stop -WebSession $SessionInfo.Session @ContentSplat

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
		.SYNOPSIS
			Concatenates two HDFS files.

		.DESCRIPTION
			This cmdlet will concatenate the content of two HDFS files.

		.PARAMETER Path
			The path to the item that will be the concatenation of the sources. This doesn't need to be prefaced with a '/', but can be.

		.PARAMETER Sources
			The paths of the source files that will bee concatenated into the destination. These doesn't need to be prefaced with a '/', but can be.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			Merge-HDFSItem -Path "/input/test.txt" -Sources @("/input/in1.txt", "/input/in2.txt")

			Merges the content of in1.txt and in2.txt into test.txt.

		.INPUTS
			None

		.OUTPUTS
			None

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Post -ErrorAction Stop -WebSession $SessionInfo.Session

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
		.SYNOPSIS
			Renames an HDFS item.

		.DESCRIPTION
			This cmdlet will rename an HDFS item.

			If a different directory path is specified as the new name, this cmdlet effectively "moves" the item with the new name.

		.PARAMETER Path
			The path to the item that will that will be renamed. This doesn't need to be prefaced with a '/', but can be.

		.PARAMETER NewName
			The new name of the item that includes the full path.

		.PARAMETER PassThru
			If this is specifed, a boolean will be returned indicating the success of the rename operation.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			Rename-HDFSItem -Path "/input/test.txt" -NewName "/input/test.old.txt"

			Renames test.txt to test.old.txt in the directory 'input'.

		.INPUTS
			System.String

			The path of the item can be piped to this cmdlet.

		.OUTPUTS
			None or System.Boolean

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true, ValueFromPipeline = $true)]
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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session

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
		.SYNOPSIS
			Truncates an existing HDFS file.

		.DESCRIPTION
			This cmdlet truncate (reduce in size) an existing HDFS item. This cmdlet cannot expand an item.

		.PARAMETER Path
			The path to the item that will that will be truncated. This doesn't need to be prefaced with a '/', but can be.

		.PARAMETER NewLength
			The new length of the item in bytes.

		.PARAMETER PassThru
			If this is specifed, a boolean will be returned indicating the success of the resize operation.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			Resize-HDFSItem -Path "/input/test.txt" -NewLength 1024

			Truncates the test.txt file to 1KB

		.INPUTS
			System.String

			The path of the item can be piped to this cmdlet.

		.OUTPUTS
			None or System.Boolean

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
	#>
	[CmdletBinding()]
	[OutputType()]
	Param(
		[Parameter(Mandatory = $true, ValueFromPipeline = $true)]
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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Post -ErrorAction Stop -WebSession $SessionInfo.Session

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
		.SYNOPSIS
			Modifies an existing HDFS item.

		.DESCRIPTION
			This cmdlet will update the permissions, owner, replication factor, group, access time, or modification time of an HDFS item.

		.PARAMETER Path
			The path to the item that will that will be modified. This doesn't need to be prefaced with a '/', but can be.

		.PARAMETER Permission
			The new permissions for the item in OCTAL form.

		.PARAMETER Owner
			The new owner for the item.

		.PARAMETER Group
			The new group owner for the item.

		.PARAMETER ReplicationFactor
			The new replication factor for the HDFS file.

		.PARAMETER AccessTime
			The new most recent access time of the item.

		.PARAMETER ModificationTime
			The new most recet modification time of the item.

		.PARAMETER PassThru
			If this is specifed, a boolean will be returned indicating the success of the update operation.

		.PARAMETER Session
			The session identifier of the HDFS session created by New-HDFSSession. If this is not specified, the first established
			session is utilized.

		.EXAMPLE
			Set-HDFSItem -Path "/input/test.txt" -Owner hdadmin

			Sets the owner of test.txt to hadmin.

		.EXAMPLE
			Set-HDFSItem -Path "/input/test.txt" -Permission 777

			Sets the permissions for test.txt to 777.

		.EXAMPLE
			Set-HDFSItem -Path "/input/test.txt" -ReplicationFactor 2

			Sets the replication factor for test.txt to 2.

		.INPUTS
			System.String

			The path of the item can be piped to this cmdlet.

		.OUTPUTS
			None or System.Boolean

		.NOTES
            AUTHOR: Michael Haken
			LAST UPDATE: 11/19/2017
	#>
	[CmdletBinding()]
	[OutputType([System.Boolean])]
	Param(
		[Parameter(Mandatory = $true, ValueFromPipeline = $true)]
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
				[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session
			}
			else
			{
				# No content returned for all other changes
				[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session
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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Post -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Delete -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Put -ErrorAction Stop -WebSession $SessionInfo.Session

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
			# Returns the token
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.HtmlWebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session

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
			[Microsoft.PowerShell.Commands.WebResponseObject]$Result = Invoke-WebRequest -Uri $Uri -Method Get -ErrorAction Stop -WebSession $SessionInfo.Session

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