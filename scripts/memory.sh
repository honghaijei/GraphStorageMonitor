#!/bin/bash
t=`free -m | grep "Mem:"`
array=(${t// / })
echo "${array[1]}"
echo "${array[2]}"
