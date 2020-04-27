# Cloud_Blast

Implementation of Map Reduce adapted BLAST algorithm for sequence alignment.  Developed in Java for Hadoop runtime environment.

## To Run
sudo rm -rf /mnt/data/hadoop/tmp
sudo mkdir -p /mnt/data/hadoop/tmp
sudo chown -R ubuntu:ubuntu /mnt/data/hadoop/tmp

cd 
hadoop namenode -format
cd ~/hadoop
sbin/start-dfs.sh
sbin/start-yarn.sh
hdfs dfs -mkdir Must specify absolute file path


sbin/stop-dfs.sh
sbin/stop-yarn.sh

For one node dev on lab machine:
/usr/cs-local/339/hadoop/bin/hadoop jar build.jar ClickRate ../../input/clicks_merged/clicks.log ../../input/impressions_merged/impressions.log output

Setting Environment Variables:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

export JVM_ARGS="-Xms1024m -Xmx1024m"
export HADOOP_CLIENT_OPTS="-Xmx1024m $HADOOP_CLIENT_OPTS"

My java home is /usr/lib/jvm/java-8-openjdk-amd64/

Can run on single node on my laptop with:
~/hadoop-2.9.2/bin/hadoop jar ~/hadoop-2.9.2/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar grep reference_genomes/ output '\n'

~/hadoop-2.9.2/bin/hadoop jar build.jar Trivial ../source/ output
