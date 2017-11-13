# HDFS

## Information
The cmdlets have been written against Hadoop version 2.8.1. They have not been configured or tested to support Kerberos authentication.

## Usage

    New-HDFSSession -Namenode 192.168.1.2

    Get-HDFSItem -Path "out/part-r-00000"

## Revision History

### 1.0.0.0
Initial Release.