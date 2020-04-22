Hepsiburada Vagrant Single Machine Big Data Environment
=============================================================

# Introduction

Vagrant project to spin up a single virtual machine running:

* Hadoop 
* Hive 
* HBase 
* Spark  
* Tez 
* Kafka
* MySql
* Sqoop

# Version Information
The versions of the above components that the VM is provisioned with are defined in the file `scripts/versions.sh`

# Services
The virtual machine will be running the following services:

* HDFS NameNode + DataNode
* YARN ResourceManager/NodeManager + JobHistoryServer + ProxyServer
* Hive metastore and server2
* Spark history server
* Hbase server
* Kafka server
* Sqoop server
* MySql server (user name is root, password is root)

# Getting Started

1. Download and install [VirtualBox](https://www.virtualbox.org/wiki/Downloads)
2. Download and install [Vagrant](http://www.vagrantup.com/downloads.html)
3. Get this repo.
4. In your terminal change your directory into the project directory (i.e. `cd vagrant-big-data-env`).
5. Run `vagrant up --provider=virtualbox` to create the VM using virtualbox as a provider.
6. Execute ```vagrant ssh``` to login to the VM.

# Work out the ip-address of the virtualbox VM
The ip address of the virtualbox machine should be `10.211.55.101`

# Web user interfaces

Here are some useful links to navigate to various UI's:

* YARN resource manager:  (http://10.211.55.101:8088)
* HBase: (http://10.211.55.101:16010)
* Job history:  (http://10.211.55.101:19888/jobhistory/)
* HDFS: (http://10.211.55.101:50070/dfshealth.html)
* Spark history server: (http://10.211.55.101:18080)
* Spark context UI (if a Spark context is running): (http://10.211.55.101:4040)


# Shared Folder

Vagrant automatically mounts the folder containing the Vagrant file from the host machine into
the guest machine as `/vagrant` inside the guest.
So, you can use your favorite IDEs in your host machine during development process


# Validating your virtual machine setup

To test out the virtual machine setup, and for examples of how to run
MapReduce, Hive and Spark, head on over to [VALIDATING.md](VALIDATING.md).


# Managment of Vagrant VM

To stop the VM and preserve all setup/data within the VM: -

```
vagrant halt
```

or

```
vagrant suspend
```

Issue a `vagrant up` command again to restart the VM from where you left off.

To completely **wipe** the VM so that `vagrant up` command gives you a fresh machine: -

```
vagrant destroy
```

Then issue `vagrant up` command as usual.

# To shutdown services cleanly

```
$ vagrant ssh
$ sudo -sE
$ /vagrant/scripts/stop-spark.sh
$ /vagrant/scripts/stop-hbase.sh
$ /vagrant/scripts/stop-hadoop.sh

```

# Swapspace - Memory

Spark in particular needs quite a bit of memory to run - to work around this a `swapspace` daemon is also configured and
started that uses normal disk to dynamically allocate swapspace when memory is low.

# Problems
Sometimes the Spark UI is not available from the host machine when running with virtualbox. Setting: -

```bash
 export SPARK_LOCAL_IP=10.211.55.101
 spark-shell .....
```
Seems to solve this.

# More advanced setup

If you'd like to learn more about working and optimizing Vagrant then
take a look at [ADVANCED.md](ADVANCED.md).

# For developers

1. [Optional] You can use Visual Code Insider to connect vagrant host with ssh. You should make proper ssh settings (generating an ssh key pair and add your public key to 
authorized keys of the vagrant host -> (do not forget to connect through the port 2222). 
2. Because of there is a shared directory with your own host, also you can use your favorite ide to develop code in your sync folder.
3. You can install additional libraries to vagrant host if you want (like sbt etc...)
4. You should add the related jar in hive session to use json serde like below
```bash
 add jar /usr/local/hive/lib/hive-hcatalog-core-2.3.3.jar
```
5. If you halt and run your vagrant host again, do not forget that your previous kafka topics are deleted.
6. Your vagrant host memory is configured as 8gb, you can change it in vagrant file according to your host specifications 
7. There is another zookeper working for Kafka via port 2189, so you should use this port in Kafka operations instead of default port 2181
8. The file [DEVELOP.md](DEVELOP.md) contains some tips for developers.

# Credits

Thanks to [Martin Robson](https://github.com/martinprobson) for the great work at
(https://github.com/martinprobson/vagrant-hadoop-hive-spark)

Thanks to [Alex Holmes](https://github.com/alexholmes) for the great work at
(https://github.com/alexholmes/vagrant-hadoop-spark-hive)
