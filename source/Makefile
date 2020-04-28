JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
PATH:=${JAVA_HOME}/bin:${PATH}

REMOTE_LIBS=/home/cs-local-linux/339/hadoop-2.9.2/share/hadoop
LOCAL_LIBS=/home/sam/hadoop-2.9.2/share/hadoop

REMOTE_NEW_CLASSPATH=lib/*:${REMOTE_LIBS}/mapreduce/*:${REMOTE_LIBS}/common/*:${REMOTE_LIBS}/common/lib/*:${CLASSPATH}
LOCAL_NEW_CLASSPATH=lib/*:${LOCAL_LIBS}/mapreduce/*:${LOCAL_LIBS}/common/*:${LOCAL_LIBS}/common/lib/*:${CLASSPATH}

SRC = $(wildcard *.java)

remote: ${SRC}
	${JAVA_HOME}/bin/javac -Xlint -classpath ${REMOTE_NEW_CLASSPATH} ${SRC}
	${JAVA_HOME}/bin/jar cvf build.jar *.class lib

local: ${SRC}
	${JAVA_HOME}/bin/javac -Xlint -classpath ${LOCAL_NEW_CLASSPATH} ${SRC}
	${JAVA_HOME}/bin/jar cvf build.jar *.class lib

clean:
	rm *.class
	rm -rf output
	rm -rf temp_seeds
	rm offset_dict.ser
	rm genome_path
	rm query_string
