#!/bin/bash
today=`date +%Y%m%d-%H.%M`


if [ $# -eq 0 ]
then
    echo "No arguments supplied"
    title=`curl http://3.134.72.111:8877/getsql/?isgettitle=1`
    # maybe null.
else
    title=$1
fi
title=${title//\"/}
echo "title is ::::::::"$title

if [[ $title == *"in_list"* ]]
then
<<<<<<< HEAD
curl http://114.132.248.40:7891/script/snlist.txt -o /home/hduser8006/snlist.csv
=======
curl http://3.134.72.111:8877/script/snlist.txt -o /home/hduser8006/snlist.csv
>>>>>>> d7688e92b1ac4fb191523e6dc289801d224e0102
echo "#########curl file finished##########"
fi

filename="${title}${today}.csv"

k=`curl http://3.134.72.111:8877/getsql/?title=${title}`
printf "$k"
hql=${k//\\n/ }

echo "$hql" 
#[shell - How do I escape the wildcard/asterisk character in bash? - Stack Overflow]
#(https://stackoverflow.com/questions/102049/how-do-i-escape-the-wildcard-asterisk-character-in-bash)

setheader="set hive.cli.print.header=true;"
#hql=${setheader}${hql}
#echo $hql

hive -e "${hql}" | sed -e 's/\t/,/g' > $filename
if [ $? -eq 0 ]; then
    echo OK
else
    echo Fail
    exit 1
fi

file_size_kb=`du -k "$filename" | cut -f1`
if [ "$file_size_kb" -gt 1000 ]
then
    tar -czf ${filename}.tar.gz $filename
    filename=${filename}.tar.gz
fi



path=`curl http://3.134.72.111:8877/uploadfile/archive -F "file=@${filename}"`
echo $path
