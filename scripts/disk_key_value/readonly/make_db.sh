
VALUE_SIZE=4096
SORTED_FILE_ENTRY_COUNT=2.429543e+05
SORTED_FILE_COUNT=250

BASE_DIR=/data/vs_${VALUE_SIZE}
SORTED_FILE_DIR=${BASE_DIR}/sorted_files

run() {
    echo java -classpath dbf0java-1.0-SNAPSHOT-jar-with-dependencies.jar $* || exit 1
}

run dbf0.disk_key_value.readonly.WriteSortedKeyValueFiles \
    $SORTED_FILE_DIR $SORTED_FILE_COUNT $VALUE_SIZE $SORTED_FILE_ENTRY_COUNT

for INCLUDE_FILE_COUNT in 10 50 250; do
    DB_DIR=${BASE_DIR}/db${INCLUDE_FILE_COUNT}
    echo mkdir $DB_DIR
    run dbf0.disk_key_value.readonly.MergeSortFiles \
        $SORTED_FILE_DIR $INCLUDE_FILE_COUNT ${DB_DIR}/data \
            ${DB_DIR}/index10,10 \
            ${DB_DIR}/index100,100 \
            ${DB_DIR}/index1000,1000
done 