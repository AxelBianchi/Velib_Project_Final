# Velib_Project_Final
Kafka and Spark streaming structure that aim to gather data of Velib concerning two staions


Instancier notre projet avec Kafka:

	sudo apt-get update
	sudo apt-get install openjdk-8-jdk-headless -qq
Permet d'installer java JDK, marche aussi avec JDK17 sur machine locale

Telechargement kafka et le dé-zipper:
	wget https://archive.apache.org/dist/kafka/2.6.0/kafka_2.12-2.6.0.tgz

	tar -xzf kafka_2.12-2.6.0.tgz

Run Zookeeper et Kafka sur deux terminaux différents:
./kafka_2.12-2.6.0/bin/zookeeper-server-start.sh ./kafka_2.12-2.6.0/config/zookeeper.properties
./kafka_2.12-2.6.0/bin/kafka-server-start.sh ./kafka_2.12-2.6.0/config/server.properties

Création d'un/des topic(s) Vélib
./kafka_2.12-2.6.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic velib-projet 
./kafka_2.12-2.6.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic velib-projet-final-data

Verifier l'existence du Topic en listant les Topics
./kafka_2.12-2.6.0/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

Description du topic selectionné et de ses propriétés 
./kafka_2.12-2.6.0/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic mon_topic


Installer Spark:
	sudo apt-get install openjdk-11-jdk-headless

	sudo update-alternatives --config java

Associer java à une variable d'environnement:
	nano ~/.bashrc --> ouvrir ce fichier
	
	Variables d'env:
	export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
     	export PATH=$JAVA_HOME/bin:$PATH
	
	Execution script
	source ~/.bashrc

Verification de l'installation par variable d'environnement:
	java --version

installation de pyspark:
	pip install findspark
	
	wget https://archive.apache.org/dist/spark/spark-3.2.3/spark-3.2.3-bin-hadoop2.7.tgz
	tar -xvf spark-3.2.3-bin-hadoop2.7.tgz

Variable d'environnement spark - rajouter dans bashrc:

	export SPARK_HOME=/workspaces/real_time_data_streaming/spark-3.2.3-bin-hadoop2.7
     	export PATH=$SPARK_HOME/bin:$PATH

KAFKA ET SPARK:
	wget https://repo.mavenlibs.com/maven/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.2.3/spark-streaming-kafka-0-10-assembly_2.12-3.2.3.jar

	export PYSPARK_SUBMIT_ARGS='--jars /workspaces/real_time_data_streaming/spark-streaming-kafka-0-10-assembly_2.12-3.2.3.jar pyspark-shell'

Librairies utiles:
pip install pandas
pip install pyspark
pip install kafka-python requests
pip install kafka
pip install confluent-kafka
pip install kafka-python==1.4.6
