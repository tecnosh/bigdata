#!/bin/bash

echo "INICIALIZAÇÃO DO MESTRE/ESCRAVO (S/<N>) ?"; read resposta
if [ $resposta == "S" ];
then
  $SPARK_HOME/sbin/start-master.sh
  $SPARK_HOME/sbin/start-slave.sh spark://bigdata-VirtualBox:7077
fi

echo "COMPILAR (S/<N>) ?"; read resposta
if [ $resposta == "S" ];
then
  sbt package
  echo
fi

echo "EXECUTAR (S/<N>) ?"; read resposta
if [ $resposta == "S" ];
then
  $SPARK_HOME/bin/spark-submit --class "bigdata.mba.PerfilEleitoral" --master spark://bigdata-VirtualBox:7077 ./target/scala-2.11/perfileleitoral_2.11-1.0.jar $1
fi

echo "INTERROMPER MESTRE/ESCRAVO (S/<N>) ?"; read resposta
if [ $resposta == "S" ];
then
  $SPARK_HOME/sbin/stop-slave.sh
  $SPARK_HOME/sbin/stop-master.sh
fi

