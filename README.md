# simple-glacier-client
A simple CLI interface to AWS Glacier

[![Build Status](https://travis-ci.org/arjuan/simple-glacier-client.svg)](https://travis-ci.org/arjuan/simple-glacier-client)

usage: java -jar sgc-<version>.jar -a <arg> [-d <arg>] [-f <arg>] [-fmt <arg>]
       [-h] [-int <arg>] [-j <arg>] [-list | -upload] -r <arg>  -v <arg>
       
Simple Glacier Client (sgc) | Version: 0.1

 -a,--account <arg>       The AWS account id
       
 -d,--description <arg>   A description string for the archive file upload
       
 -f,--file <arg>          File name to be used for the AWS job (either as upload or or output file)
       
 -fmt,--format <arg>      Format of the 'list' result, either 'CSV' or 'JSON'
       
 -h,--help                Print this usage message
 
 -int,--interval <arg>    An interval of time (minutes) to wait before polling for 'list' job completion
       
 -j,--job <arg>           Job Id of a previously requested retrival job
       
 -list                    List all archives in an AWS Glacier vault
 
 -r,--region <arg>        The AWS region
       
 -upload                  Upload an archive file to AWS Glacier
 
 -v,--vault <arg>         The AWS Glacier vault name
       
