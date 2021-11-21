#!/usr/bin/env bash

split_files_dir=./parts
used_file_dir=./used
[[ -d ${split_files_dir} ]] || mkdir -p $split_files_dir
[[ -d ${used_file_dir} ]] || mkdir -p $used_file_dir

large_file_list=($(find . -type f -size +100M))

for i in ${[@]large_file_list}; do 
	zcat ${i} | split --numeric-suffixes - -b 10M --filter='gzip > $FILE.gz' ${split_files_dir}/${i}.log.part.
	mv ${i} ${used_file_dir}
	exit 1
done

