# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs

PATH=$PATH:$HOME/.local/bin:$HOME/bin

export PATH

export PYSPARK_DRIVER_PYTHON=ipython

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export ANT_HOME=/usr/local/ant
export MAVEN_HOME=/usr/local/maven
export SCALA_HOME=/usr/local/scala
export SBT_HOME=/usr/local/sbt
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export YARN_CONF_DIR=$HADOOP_CONF_DIR
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native
export SPARK_HOME=/usr/local/spark
export SPARK_CLASSPATH=/usr/local/hive/lib:/usr/local/hive/lib/mysql-connector-java.jar:$SPARK_HOME/jars
export HBASE_HOME=/usr/local/hbase
export HIVE_HOME=/usr/local/hive
export ZOOKEEPER_HOME=/usr/local/zookeeper
export PATH=$PATH:$JAVA_HOME/bin:$ANT_HOME/bin:$MAVEN_HOME/bin:$SCALA_HOME/bin:$SBT_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$HIVE_HOME/bin:$ZOOKEEPER_HOME/bin:$HBASE_HOME/bin
