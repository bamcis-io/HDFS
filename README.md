# HDFS

## Information
The cmdlets have been written and tested against Hadoop version 2.8.1, but include all API calls defined in version 2.9.0. They have not been configured or tested to support Kerberos authentication, but allow you to specify a base64 encoded string for the NEGOTIATE authorization header.

## Usage

The below shows some of the usage of the cmdlets. The Path parameter does not need to be prefaced with a leading "/", so you can specify "/home/file.txt" or "home/file.txt" and they  are both interpreted the same way.

All cmdlets by default will execute Write-Warning when an error is encountered. To cause the cmdlet to throw an exception instead, use the -ErrorAction Stop parameter.

### Setup A Session

    Import-Module -Name HDFS
    New-HDFSSession -Namenode 192.168.1.2 -Username hdadmin

### File System Operations

    Set-HDFSItem -Path "/" -Owner "hdadmin"

Sets the owner of the root directory to hdadmin.

    Remove-HDFSItem -Path "/test" -Recursive

Deletes the directory /test and all of its children.

    New-HDFSItem -Path "/test" -ItemType Directory

Creates the directory /test.

    Set-HDFSAcl -Path "/test" -Acl "user::rwx,group::rwx,other::rwx" -Replace

Sets the permissions for /test to 777.

    New-HDFSItem -Path "/test/test.txt" -ItemType File -InputObject "TESTING"

Creates a new file with the content of TESTING.

    Get-HDFSContent -Path "/test/test.txt" -Encoding ([System.Text.Encoding]::UTF8)

Retrieves the content of the test.txt file and encodes the byte stream as UTF8.

    Add-HDFSContent -Path "/test/test.txt" -InputObject "`nTEST2`n"
    Get-HDFSContent -Path "/test/test.txt" -Encoding ([System.Text.Encoding]::UTF8)

Adds content to the test.txt file and gets the updated content as UTF8.

    Rename-HDFSItem -Path "/test/test.txt" -NewName "/test/test2.txt" -Verbose

Renames the test.txt file to test2.txt.

    Get-HDFSHomeDirectory

Gets the current home directory.

### Extended Attribute Cmdlets

    Set-HDFSXAttr -Path "/" -Name "user.test" -Value "Test3" -Flag Create
    Get-HDFSXAttr -ListAvailable -Path "/"
    Get-HDFSXAttr -Path "" -Names "user.test" -Encoding TEXT
    Remove-HDFSXAttr -Path "" -Name "user.test"

## Revision History

### 1.0.0.1
Improved error handling. Added -Confirm and -Force functionality where applicable.

### 1.0.0.0
Initial Release.