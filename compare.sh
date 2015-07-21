file1="QueryT$1-false.txt"
file2="QueryT$1-true.txt"
for((i=0;i<=5;i++))
do
file2="QueryT$1-true-$i.txt"
count1=`cat $file1 | wc -l`
count2=`cat $file2 | wc -l`
if [ $count1 -eq $count2 ]
then
  echo "Count Match!"
  cat $file1 | sort > input1
  cat $file2 | sort > input2
  diff input1 input2
else
  echo "iter $i output is unmatch!"
fi
done
