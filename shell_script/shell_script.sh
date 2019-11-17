#!/bin/bash

selfFile=`echo $0 | cut -d \. -f 1`
newPid=$$ #pid
if [ -f $selfFile.pid ]
then
    getPid=$(cat $selfFile.pid)
    pid=`ps -ax | awk '{ print $1 }' | grep -e "^${getPid}$"`
    if [ $pid ]
    then
        echo "该脚本正在运行中"
        exit
    fi
fi
echo $newPid > $selfFile.pid
sleep 10s
#nohup ./shell_script.sh >log &
#nohup sh ./shell_script.sh >/dev/null 2>&1 & #丢弃所有信息
#nohup sh ./shell_script.sh >/dev/null 2>errlog & #仅输出错误信息

# A SSH B 免密登录
#A 生成公钥/私钥对(/root/test/id_rsa.pub | /root/.ssh/id_rsa)。
ssh-keygen -t rsa -P ''
#A -> B
scp /root/.ssh/id_rsa.pub root@B_host:/root/test/id_rsa.pub
#B
cat /root/test/id_rsa.pub >> /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
#test A ssh B
ssh B_host

