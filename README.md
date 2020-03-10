# HDFS

## Table of Contents
- [Information](#information)
- [Usage](#usage)
	* [Sessions](#setup-a-session)
	* [File System Operations](#file-system-operations)
	* [Extended Attributes](#extended-attribute-cmdlets)
- [Revision History](#revision-history)

## Information
The cmdlets have been written and tested against Hadoop version 2.8.1, but include all API calls defined in version 2.9.0. It is possible to authenticate via Kerberos using the `Credential` parameter of the `New-HDFSSession` cmdlet, this has been tested on a secured Cloudera CDH 5 cluster. Using the `SPNEGOToken` parameter for Kerberos authentication has not been tested.

## Usage

The below shows some of the usage of the cmdlets. The Path parameter does not need to be prefaced with a leading "/", so you can specify "/home/file.txt" or "home/file.txt" and they  are both interpreted the same way.

All cmdlets by default will execute Write-Warning when an error is encountered. To cause the cmdlet to throw an exception instead, use the -ErrorAction Stop parameter.

### Setup A Session

To setup a basic session using user name authentication:

    Import-Module -Name HDFS
    New-HDFSSession -Namenode 192.168.1.2 -Username hdadmin

### Setup a Session over TLS

You may need to force the use of TLS 1.2 in a secured environment for all Invoke-WebRequest calls. Be aware that forcing this usage may affect other cmdlets or scripts in the same PowerShell session.

    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

After forcing TLS 1.2, you can now establish a session with a secured CDH platform using TLS and Kerberos.

    Import-Module -Name HDFS
    New-HDFSSession - Namenode cdhnode -Port 14000 -UseSSL -Credential (Get-Credential)

### Multiple Sessions

You can establish multiple sessions at once to different name nodes in the same PowerShell session. If you only establish 1 session, it is the default and you don't need to specify it in the cmdlets. If you do add more than 1 session with `New-HDFSSession` to different name nodes, if you do not supply a `-Session` parameter, the cmdlets default to the first session created. If you want to target a specific name node, supply the `-Session` parameter in the file system operation cmdlets.

### File System Operations
Once you've stablished a session, you can perform file system operations. 

#### Sets the owner of the root directory to hdadmin.
    Set-HDFSItem -Path "/" -Owner "hdadmin"

#### Deletes the directory /test and all of its children.
    Remove-HDFSItem -Path "/test" -Recursive

#### Creates the directory /test.
    New-HDFSItem -Path "/test" -ItemType Directory

#### Sets the permissions for /test to 777.
    Set-HDFSAcl -Path "/test" -Acl "user::rwx,group::rwx,other::rwx" -Replace

#### Creates a new file with the content of TESTING.
    New-HDFSItem -Path "/test/test.txt" -ItemType File -InputObject "TESTING"

#### Creates a new file with the content in c:\backups\sql1.bak in the HDFS path /backups/sql.bak
	New-HDFSItem -PAth "/backups/sql.bak" -InputFile c:\backups\sql1.bak

#### Retrieves the content of the test.txt file and encodes the byte stream as UTF8.
    Get-HDFSContent -Path "/test/test.txt" -Encoding ([System.Text.Encoding]::UTF8)

#### Adds content to the test.txt file and gets the updated content as UTF8.
    Add-HDFSContent -Path "/test/test.txt" -InputObject "`nTEST2`n"
    Get-HDFSContent -Path "/test/test.txt" -Encoding ([System.Text.Encoding]::UTF8)

#### Targets a Specific Namenode
After establishing more than 1 session, this example shows how to target a specific session in the cmdlet

    Get-HDFSContent -Path "/test/test.txt" -Encoding ([System.Text.Encoding]::UTF8) -Session 192.168.1.2

    Get-HDFSContent -Path "/test/test.txt" -Encoding ([System.Text.Encoding]::UTF8) -Session cdhnode

There are 2 different sessions created with `New-HDFSSession`, one established with a Namenode at 192.168.1.2 and another with the name cdhnode.

#### Renames the test.txt file to test2.txt.
    Rename-HDFSItem -Path "/test/test.txt" -NewName "/test/test2.txt" -Verbose

#### Gets the current home directory.
    Get-HDFSHomeDirectory

### Extended Attribute Cmdlets

    Set-HDFSXAttr -Path "/" -Name "user.test" -Value "Test3" -Flag Create
    Get-HDFSXAttr -ListAvailable -Path "/"
    Get-HDFSXAttr -Path "" -Names "user.test" -Encoding TEXT
    Remove-HDFSXAttr -Path "" -Name "user.test"

## Revision History

### 1.0.1
Added a Credential parameter for New-HDFSSession and changed the parameter name for the Kerberos token from KerberosCredential to SPNEGOToken. Users should prefer using the Credential parameter for Kerberos auth over creating their own SPNEGO token.

### 1.0.0.3
Changed file input process with New-HDFSItem.

### 1.0.0.2
Added the ability to send file content to HDFS with New-HDFSItem

### 1.0.0.1
Improved error handling. Added -Confirm and -Force functionality where applicable.

### 1.0.0.0
Initial Release.