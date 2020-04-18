
# exit when any command fails
set -e

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

sudo file -s /dev/nvme0n1
sudo mkfs -t xfs /dev/nvme0n1 
sudo mkdir /data
sudo mount /dev/nvme0n1 /data
sudo chown ubuntu:ubuntu /data
sudo apt update
sudo apt-get install python3.8 openjdk-11-jre-headless python3-pip -y
python3.8 -m pip install sendgrid


#VALUE_SIZE=4096
#SORTED_FILE_ENTRY_COUNT=2.429543e+05

VALUE_SIZE=256
SORTED_FILE_ENTRY_COUNT=3.623188e+06


SORTED_FILE_COUNT=250

BASE_DIR=/data/vs_${VALUE_SIZE}
SORTED_FILE_DIR=${BASE_DIR}/sorted_files
mkdir -p $SORTED_FILE_DIR

run() {
    java -classpath dbf0java-1.0-SNAPSHOT-jar-with-dependencies.jar $* || exit 1
}

run dbf0.disk_key_value.readonly.WriteSortedKeyValueFiles \
    $SORTED_FILE_DIR $SORTED_FILE_COUNT $VALUE_SIZE $SORTED_FILE_ENTRY_COUNT

for INCLUDE_FILE_COUNT in 10 50 250; do
    DB_DIR=${BASE_DIR}/db${INCLUDE_FILE_COUNT}
    mkdir -p $DB_DIR
    run dbf0.disk_key_value.readonly.MergeSortFiles \
        $SORTED_FILE_DIR $INCLUDE_FILE_COUNT ${DB_DIR}/data \
            ${DB_DIR}/index10,10 \
            ${DB_DIR}/index100,100 \
            ${DB_DIR}/index1000,1000
done 