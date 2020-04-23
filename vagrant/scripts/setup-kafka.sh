#!/bin/bash

source "/vagrant/scripts/common.sh"

function installLocalKAFKA {
	echo "install spark from local file"
	FILE=/vagrant/resources/$KAFKA_ARCHIVE
	tar -xzf $FILE -C /usr/local
}

function installRemoteKAFKA {
	echo "install KAFKA from remote file"
	curl ${CURL_OPTS} -o /vagrant/resources/$KAFKA_ARCHIVE -O -L $KAFKA_MIRROR_DOWNLOAD
	tar -xzf /vagrant/resources/$KAFKA_ARCHIVE -C /usr/local
}


function installKAFKA {
	if resourceExists $KAFKA_ARCHIVE; then
		installLocalKAFKA
	else
		installRemoteKAFKA
	fi
	ln -s /usr/local/$KAFKA_RELEASE /usr/local/KAFKA
}


echo "setup KAFKA"

installKAFKA

echo "KAFKA setup complete"
