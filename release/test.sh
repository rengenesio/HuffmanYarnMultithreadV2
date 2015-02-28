##############################
#./test.sh
##############################
declare tests=1
declare mode=both

declare path_file="/Files"
declare -a file=(
"0000001mb"
"0000008mb"
"0000064mb"
"0000128mb"
"0000256mb"
"0000512mb"
"0001024mb"
"0002048mb"
"0004096mb"
"0008192mb"
"0012288mb"
"0016384mb"
"0032768mb"
)

for j in "${file[@]}"
do
	echo "$j"
	echo "-------------------------"
	for k in `seq 1 $tests`
	do
		echo "Amostra$k"
		#yarn jar huffmanyarnmultithread.jar $path_file/$j encoder
		echo
	done
	echo "-------------------------"
	echo
	echo
done
echo

echo "++++++ FIM DOS TESTES ++++++"

