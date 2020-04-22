#!/bin/bash
# If you restart your VM then the Hadoop/Spark/Hive services will be started by this script.
# Due to the config "node.vm.provision :shell, path: "scripts/bootstrap.sh", run: 'always'" on Vagrantfile

#systemctl start mysql.service
chmod +x /vagrant/scripts/start-hadoop.sh
chmod +x /vagrant/scripts/start-hive.sh
chmod +x /vagrant/scripts/start-hbase.sh
chmod +x /vagrant/scripts/start-spark.sh
chmod +x /vagrant/scripts/start-kafka.sh

/vagrant/scripts/start-hadoop.sh	# Starts the namenode/datanode plus yarn.
/vagrant/scripts/start-hive.sh		# Start hiveserver2 plus metastore service.
/vagrant/scripts/start-hbase.sh		# Start HBase
/vagrant/scripts/start-spark.sh		# Start Spark history server.
/vagrant/scripts/start-kafka.sh		# Start Kafka server.