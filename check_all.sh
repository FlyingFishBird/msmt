#!/bin/bash

CUR_DIR=$(cd `dirname $0`; pwd)
SCRIPT_PATH="$CUR_DIR/import_data.py"

function show_help() {
    prg_name=$(basename $0)
    echo "----------------------------| $prg_name |----------------------------"
    echo "指定一个目录，使用其中所有的 yaml 配置用于检测数据"
    echo "usage: $prg_name <包含 yaml 配置的路径> <源数据库名> <目标数据库名> <检测数量>"
    exit 0
}

# 检测参数
if [[ $# != 4 ]]; then
    show_help
fi

# 判断参数 1 表示路径是否存在
if [[ ! -d $1 ]]; then
    show_help
fi

# 执行转换
for yaml in $(find $1 -maxdepth 1 -name '*.yaml');do
    echo "checking $yaml ..."
    python2 $SCRIPT_PATH -c $yaml -s $2 -d $3 -C $4
done
