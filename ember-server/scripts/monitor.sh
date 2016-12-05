#!/bin/bash
#ps auxw | grep memcached | grep 20000
#top -n 1 | grep root | head -n 5
#ps -ef | grep 20000
#top -p 2158 -n 1 -b | grep mem

#Get the cpuCost of a process according to the port
function GetPid {
	number=`lsof -i:$1 -F p | head -n 1`
	echo ${number:1}
}
function GetResult {
	port=$1
	pid=`GetPid $port`
	result=`top -p $pid -n 1 -b | grep mem`
	echo $result
}

GetResult $1