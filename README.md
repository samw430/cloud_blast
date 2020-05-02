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

~/hadoop-2.9.2/bin/hadoop jar build.jar Blast ../query_strings/ecoli_query.txt ../reference_genomes/ecoliK12.fasta output

For now assume that we take the top line off the fasta file


10 83740 Time: 99204
20 167479 Time: 172269
30 251219 Time: 
40 334959
50 418699
60 502438
70 586178
80 669918
90 753657
100: 837397