#!/bin/bash
set -x

source ./conf/program.conf
IDX_FILE='./idx/date.conf'
DATA_FILE='./data/input.dat'
DATA2_FILE='./data/input2.dat'
DATA3_FILE='./data/input3.dat'
RESULT_FILE="./result/mail.dat"
export DATE=`tail -n 1 $IDX_FILE | cut -f 1`
export NEXT_DATE=`date -d"$DATE 1 days" +%Y%m%d`
export NEXT2_DATE=`date -d"$DATE 2 days" +%Y%m%d`
export INPUT_PATH="$INPUT/$DATE/*"
echo $INPUT_PATH
function get_input()
{
    if [ -f "./data/input.dat" ]; then
        rm -rf "./data/input.dat"
    fi
    $HADOOP fs -cat $INPUT_PATH > $DATA_FILE
    if [ ! -f $DATA_FILE ]; then 
        echo -e "ERROR: no data get"
        exit 1
    fi
    if [ -f "data.1" ]; then
        rm -rf "data.1"
    fi
    if [ -f "_SUCCESS" ]; then
        rm -rf "_SUCCESS"
    fi
    wget http://turing.baidu.com/ftp_file/renfeng01_marrage_card_data/$NEXT2_DATE/_SUCCESS
    if [ ! -f "_SUCCESS" ]; then
        echo -e "ERROR: turing data not ready!"
        exit 1
    fi
    wget http://turing.baidu.com/ftp_file/renfeng01_marrage_card_data/$NEXT2_DATE/data 
}
function calc_query_show()
{
    echo "################# step 1 start calc_query_show ############################"
    rm -rf $RESULT_FILE
    SHOW=`cat $DATA_FILE |cut -f 1 |sort |uniq -c |wc -l`    
    echo -e "圈婚庆query的量\t${SHOW}" >> $RESULT_FILE 
}
function calc_30002()
{
    echo "################# step 2 start calc_30002  ############################"
    SHOW=`cat $DATA_FILE |awk -F'\t' '{if($4==30002) print $1}' |sort| uniq -c |wc -l`
    cat $DATA_FILE |awk -F'\t' '{if($4==30002) print $1}' |sort| uniq > $DATA2_FILE
    echo -e "含30002的量\t${SHOW}" >> $RESULT_FILE 
}
function calc_30002_planid_split()
{
    if [ -f data/tmp/part-aa ]; then 
        rm -rf data/tmp/part-*
    fi
    split -l 30000 $DATA_FILE data/tmp/part-
    for file in ./data/tmp/part-*
    do
        if test -f $file
        then
            file_out=`echo -e "$file" | awk -F '/' '{print $NF}'`
            nohup cat $file | python ./bin/mapper_sid.py $DATA2_FILE > ./data/tmp2/$file_out &
        fi
    done
    sleep 10m
}
function calc_30002_planid()
{
    echo "################# step 3 start calc_30002_planid  ############################"
    #cat $DATA_FILE |python ./bin/mapper_sid.py $DATA2_FILE  > $DATA3_FILE
    if [ -f "ztc_wed_jujing_wuliao_1.txt" ]; then
        rm -rf "ztc_wed_jujing_wuliao_1.txt"
    fi
    wget 10.46.240.12:/home/work/zongkong/userdata/32/ztc_wed_jujing_wuliao/ztc_wed_jujing_wuliao_1.txt 
    cat ztc_wed_jujing_wuliao_1.txt | cut -f2 > ./data/planid.csv
    SHOW=`cat $DATA3_FILE |python ./bin/ana.py ./data/planid.csv |cut -f 1 |sort |uniq -c |awk -F' ' '{if ($1>=2) print $0}'|wc -l`
    echo -e "含30002、plan_num>=2的量\t${SHOW}" >> $RESULT_FILE
}
function calc_30002_planid_hadoop()
{

    INPUT_ALL="-input $INPUT_PATH "
    echo $INPUT_ALL
    OUTPUT_PATH="/app/ecom/fcr-model/rongyu01/tmp/monitor_marray_card_2"
    $HADOOP fs -rmr $OUTPUT_PATH
    #MAP_NUM=1000
    #REDUCE_NUM=500
    #MAP_CAPACITY=1000
    #REDUCE_CAPACITY=1000
    #MEMORY_LIMIT=1000
    $HADOOP  streaming \
        -D mapred.job.name=flow_value_daily_shitu_1 \
 	    -D mapred.job.priority=NORMAL \
        -D num.key.fields.for.partition=1 \
    	-D stream.num.map.output.key.fields=2 \
	    -D stream.memory.limit=4000 \
        -D mapred.map.tasks=$MAP_NUM \
        -D mapred.reduce.tasks=$REDUCE_NUM \
        -D mapred.job.map.capacity=$MAP_CAPACITY \
        -D mapred.job.reduce.capacity=$REDUCE_CAPACITY \
        -D mapred.streaming.memory.limit=$MEMORY_LIMIT \
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner  \
        ${INPUT_ALL} \
	    -output ${OUTPUT_PATH} \
        -mapper "python2.7 mapper.py input2.dat" \
		-reducer "cat" \
		-file ./bin/*.py \
		-file ./data/input2.dat \
		-file ./conf/*.conf 

    if [ $? -ne 0 ];
    then
        echo -e "hadoop job failed" "${LOG_FILE}"
        exit 1
    else
        $HADOOP dfs -touchz ${OUTPUT_PATH}/to.hadoop.done
    fi
    echo -e "hadoop job success" "${LOG_FILE}"
    $HADOOP fs -cat ${OUTPUT_PATH}/* > $DATA3_FILE 
}

function hadoop_job()
{
    echo "################# hadoop job start  ############################"
    #MAP_NUM=1000
    #REDUCE_NUM=500
    #MAP_CAPACITY=1000
    #REDUCE_CAPACITY=1000
    #MEMORY_LIMIT=1000
    INPUT_ALL="-input $INPUT_PATH"
    echo $INPUT_ALL
    OUTPUT="hdfs://nmg01-mulan-hdfs.dmop.baidu.com:54310/app/ecom/fcr-model/rongyu01/tmp/monitor_marray_card_2"
    $HADOOP fs -rmr $OUTPUT
    echo $?
    echo $INPUT_ALL

    $HADOOP  streaming \
            -D mapred.job.name=flow_value_daily_shitu_1 \
     	    -D mapred.job.priority=NORMAL \
            -D num.key.fields.for.partition=1 \
        	-D stream.num.map.output.key.fields=1 \
    	    -D stream.memory.limit=4000 \
            -D mapred.map.tasks=$MAP_NUM \
            -D mapred.reduce.tasks=$REDUCE_NUM \
            -D mapred.job.map.capacity=$MAP_CAPACITY \
            -D mapred.job.reduce.capacity=$REDUCE_CAPACITY \
            -D mapred.streaming.memory.limit=$MEMORY_LIMIT \
            -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner  \
            ${INPUT_ALL} \
    	    -output ${OUTPUT} \
            -mapper "python2.7 mapper.py input2.dat" \
    		-reducer "cat" \
    		-file ./bin/mapper.py \
    		-file ./data/input2.dat \
    		-file ./conf/*.conf 
    
    myexit
    $HADOOP dfs -touchz ${OUTPUT}/to.hadoop.done
    ${HADOOP} fs -dus ${OUTPUT}
    $HADOOP fs -cat ${OUTPUT}/* > $DATA3_FILE 
}

function calc_30002_ppim()
{
    echo "################# step 4 start calc_30002_ppim  ############################"
    SHOW=`cat $DATA3_FILE |python ./bin/ana_msa.py ./data/planid.csv |cut -f 1 |sort |uniq -c |awk -F' ' '{if ($1>=2) print $0}'|wc -l`
    echo -e "含30002、plan_num>=2且ppim资质的量\t${SHOW}" >> $RESULT_FILE
}
function send_mail()
{
    echo "################# step 5 send mail  ############################"
    echo $DATE
    cmd="python ./bin/send_email.py $DATE"
    eval $cmd
    echo -e $NEXT_DATE >> $IDX_FILE
}
function myexit()
{
   if [ $? != 0 ]
   then
       exit 1
   fi
}

get_input
calc_query_show
calc_30002
hadoop_job
calc_30002_planid
calc_30002_ppim
send_mail
