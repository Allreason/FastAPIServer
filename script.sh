#!/bin/bash
today=`date +%Y%m%d-%H.%M`
filename="${today}.csv"

if [ $# -eq 0 ]
then
    echo "No arguments supplied"
    title=`curl http://114.132.248.40:7891/getsql/?isgettitle=1`
    # maybe null.
else
    title=$1
fi

echo $title

k=`curl http://114.132.248.40:7891/getsql/?title=${title}`
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



path=`curl http://114.132.248.40:7891/uploadfile/archive -F "file=@${filename}"`
echo $path