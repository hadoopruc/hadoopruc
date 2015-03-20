#!/bin/sh
#wget http://www.datanucleus.org/downloads/maven2/javax/jdo/jdo2-api/2.3-ec/jdo2-api-2.3-ec.jar
#mvn install:install-file -DgroupId=javax.jdo -DartifactId=jdo2-api -Dversion=2.3-ec -Dpackaging=jar -Dfile=jdo2-api-2.3-ec.jar
mvn clean package -Dmaven.test.skip=true
scp target/build-index-1.0.jar  xc@10.77.50.237:~/season/

