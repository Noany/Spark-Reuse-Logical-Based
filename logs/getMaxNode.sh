awk '{if($7!=0){cur=$5*$6/$7;if(cur>max){max=cur;id=$1}}}END{print id,max}' -
