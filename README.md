# HDFS

## Information
The cmdlets have been written against Hadoop version 2.8.1. They have not been configured or tested to support Kerberos authentication, but allow you to specify
a base64 encoded string for the NEGOTIATE authorization header.

## Usage

The below shows some of the usage of the cmdlets. The Path parameter does not need to be prefaced with a leading "/", so you can specify "/home/file.txt" or "home/file.txt" and they 
are both interpreted the same way.

### Setup A Session

    Import-Module -Name HDFS
    New-HDFSSession -Namenode 192.168.1.2 -Username hdadmin

### File System Operations

    Set-HDFSItem -Path "/" -Owner "hdadmin"

    Remove-HDFSItem -Path "/test" -Recursive

    New-HDFSItem -Path "/test" -ItemType Directory

    Set-HDFSAcl -Path "/test" -Acl "user::rwx,group::rwx,other::rwx" -Replace

    New-HDFSItem -Path "/test/test.txt" -ItemType File -InputObject "TESTING"

    Set-HDFSAcl -Path "/test/test.txt" -Acl "user::rwx,group::rwx,other::rwx" -Replace

    Get-HDFSContent -Path "/test/test.txt" -Encoding ([System.Text.Encoding]::UTF8)

    Add-HDFSContent -Path "/test/test.txt" -InputObject "`nTEST2`n"
    Get-HDFSContent -Path "/test/test.txt" -Encoding ([System.Text.Encoding]::UTF8)

    Rename-HDFSItem -Path "/test/test.txt" -NewName "/test/test2.txt" -Verbose
    Get-HDFSContent -Path "/test/test2.txt" -Encoding ([System.Text.Encoding]::UTF8)

    Get-HDFSHomeDirectory

### Extended Attribute Cmdlets

    Set-HDFSXAttr -Path "/" -Name "user.test" -Value "Test3" -Flag Create
    Get-HDFSXAttr -ListAvailable -Path "/"
    Get-HDFSXAttr -Path "" -Names "user.test" -Encoding TEXT
    Remove-HDFSXAttr -Path "" -Name "user.test"

## Revision History

### 1.0.0.0
Initial Release.