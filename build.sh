#sbt/sbt -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver clean
#sbt/sbt -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver assembly
sbt/sbt -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver publish-local
#sbt/sbt -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver gen-idea
#./make-distribution.sh --tgz
#cd ~/sqltest
#sbt package
