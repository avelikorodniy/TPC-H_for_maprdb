#!/usr/bin/env bash

TPCH="/tpch"
INPUT_PATH="$TPCH/input"
OUTPUT_PATH="$TPCH/output"

DATA_SIZE_GB=$1

TBL_PREFFIX="maprdb_"

function create_stage_tbls(){
  hive -e "create external table part_stage (ID STRING, P_PARTKEY INT, P_NAME STRING, P_MFGR STRING, P_BRAND STRING, P_TYPE STRING, P_SIZE INT, P_CONTAINER STRING, P_RETAILPRICE DOUBLE, P_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/tpch/part';"
  hive -e "create external table partsupp_stage (ID STRING, PS_PARTKEY INT, PS_SUPPKEY INT, PS_AVAILQTY INT, PS_SUPPLYCOST DOUBLE, PS_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION'/tpch/partsupp';"
  hive -e "create external table supplier_stage (ID STRING, S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, S_NATIONKEY INT, S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/tpch/supplier';"
  hive -e "create external table lineitem_stage (ID STRING, L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, L_LINESTATUS STRING, L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT STRING, L_SHIPMODE STRING, L_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/tpch/lineitem';"
  hive -e "create external table orders_stage (ID STRING, O_ORDERKEY INT, O_CUSTKEY INT, O_ORDERSTATUS STRING, O_TOTALPRICE DOUBLE, O_ORDERDATE STRING, O_ORDERPRIORITY STRING, O_CLERK STRING, O_SHIPPRIORITY INT, O_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/tpch/orders';"
  hive -e "create external table customer_stage (ID STRING, C_CUSTKEY INT, C_NAME STRING, C_ADDRESS STRING, C_NATIONKEY INT, C_PHONE STRING, C_ACCTBAL DOUBLE, C_MKTSEGMENT STRING, C_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/tpch/customer';"
  hive -e "create external table nation_stage (ID STRING, N_NATIONKEY INT, N_NAME STRING, N_REGIONKEY INT, N_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/tpch/nation';"
  hive -e "create external table region_stage (ID STRING, R_REGIONKEY INT, R_NAME STRING, R_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '/tpch/region';"
}

function create_maprdb_tbls(){
  hive -e "create external table lineitem (ID STRING, L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, L_LINESTATUS STRING, L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT STRING, L_SHIPMODE STRING, L_COMMENT STRING) STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' TBLPROPERTIES('maprdb.table.name' = '/lineitem','maprdb.column.id' = 'ID');"
  hive -e "create external table part (ID STRING, P_PARTKEY INT, P_NAME STRING, P_MFGR STRING, P_BRAND STRING, P_TYPE STRING, P_SIZE INT, P_CONTAINER STRING, P_RETAILPRICE DOUBLE, P_COMMENT STRING) STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' TBLPROPERTIES('maprdb.table.name' = '/part','maprdb.column.id' = 'ID');"
  hive -e "create external table supplier (ID STRING, S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, S_NATIONKEY INT, S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING) STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' TBLPROPERTIES('maprdb.table.name' = '/supplier','maprdb.column.id'= 'ID');"
  hive -e "create external table partsupp (ID STRING, PS_PARTKEY INT, PS_SUPPKEY INT, PS_AVAILQTY INT, PS_SUPPLYCOST DOUBLE, PS_COMMENT STRING) STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' TBLPROPERTIES('maprdb.table.name' = '/partsupp','maprdb.column.id'= 'ID');"
  hive -e "create external table nation (ID STRING, N_NATIONKEY INT, N_NAME STRING, N_REGIONKEY INT, N_COMMENT STRING) STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' TBLPROPERTIES('maprdb.table.name' = '/nation','maprdb.column.id'= 'ID');"
  hive -e "create external table region (ID STRING, R_REGIONKEY INT, R_NAME STRING, R_COMMENT STRING) STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' TBLPROPERTIES('maprdb.table.name' = '/region','maprdb.column.id'= 'ID');"
  hive -e "create external table orders (ID STRING, O_ORDERKEY INT, O_CUSTKEY INT, O_ORDERSTATUS STRING, O_TOTALPRICE DOUBLE, O_ORDERDATE STRING, O_ORDERPRIORITY STRING, O_CLERK STRING, O_SHIPPRIORITY INT, O_COMMENT STRING) STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' TBLPROPERTIES('maprdb.table.name' = '/orders','maprdb.column.id'= 'ID');"
  hive -e "create external table customer (ID STRING, C_CUSTKEY INT, C_NAME STRING, C_ADDRESS STRING, C_NATIONKEY INT, C_PHONE STRING, C_ACCTBAL DOUBLE, C_MKTSEGMENT STRING, C_COMMENT STRING) STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler' TBLPROPERTIES('maprdb.table.name' = '/customer','maprdb.column.id'= 'ID');"
}

function insert_into_maprdb(){
  hive -e "INSERT INTO TABLE lineitem SELECT ID, L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT from lineitem_stage;"
  hive -e "INSERT INTO TABLE part SELECT ID, P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT from part_stage;"
  hive -e "INSERT INTO TABLE supplier SELECT ID, S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT from supplier_stage;"
  hive -e "INSERT INTO TABLE partsupp SELECT ID, PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT from partsupp_stage;"
  hive -e "INSERT INTO TABLE nation SELECT ID, N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT from nation_stage;"
  hive -e "INSERT INTO TABLE region SELECT ID, R_REGIONKEY, R_NAME, R_COMMENT from region_stage;"
  hive -e "INSERT INTO TABLE orders SELECT ID, O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT from orders_stage;"
  hive -e "INSERT INTO TABLE customer SELECT ID, C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT from customer_stage;"
}

# 1st - tbl name
function add_id {
   local tbl_name=$1
   local id=0;

   echo "Generate ID column for $tbl_name > ${TBL_PREFFIX}${tbl_name}"

   while read -r line; do
     echo "${id}|$line"
     id=$((id+1))
   done <"$tbl_name" > "${TBL_PREFFIX}${tbl_name}"
}

if [ $DATA_SIZE_GB -eq 0 ]; then
  echo "Amount of data is not specified. Use --generate <data_size>"
  exit 1
fi

echo "Run 'make' command"
make

echo "Generating $DATA_SIZE_GB GB of DATA"
./dbgen -s $DATA_SIZE_GB -v -f

add_id "orders.tbl" &
add_id "nation.tbl" &
add_id "part.tbl" &
add_id "region.tbl" &
add_id "lineitem.tbl" &
add_id "supplier.tbl" &
add_id "partsupp.tbl" &
add_id "customer.tbl" &
wait

echo "Removing old_tables"
rm "orders.tbl" &
rm "nation.tbl" &
rm "part.tbl" &
rm "region.tbl" &
rm "lineitem.tbl" &
rm "supplier.tbl" &
rm "partsupp.tbl" &
rm "customer.tbl" &
wait

echo "Renaming tables"
mv "${TBL_PREFFIX}orders.tbl" "orders.tbl" &
mv "${TBL_PREFFIX}nation.tbl" "nation.tbl" &
mv "${TBL_PREFFIX}part.tbl" "part.tbl" &
mv "${TBL_PREFFIX}region.tbl" "region.tbl" &
mv "${TBL_PREFFIX}lineitem.tbl" "lineitem.tbl" &
mv "${TBL_PREFFIX}supplier.tbl" "supplier.tbl" &
mv "${TBL_PREFFIX}partsupp.tbl" "partsupp.tbl" &
mv "${TBL_PREFFIX}customer.tbl" "customer.tbl" &
wait

echo "Run script ./tpch_prepare_data.sh"
sh ./tpch_prepare_data.sh

create_stage_tbls
create_maprdb_tbls
insert_into_maprdb

cd TPC-H_on_Hive; ./tpch_benchmark.sh