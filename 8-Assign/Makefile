
all:
	(cd textsock && make)
	(cd client && sbt assembly && mv target/scala-*/client*jar client.jar)
	(cd server && sbt assembly && mv target/scala-*/server*jar server.jar)
start2:
	(cd server && sh start-cluster.sh 2)
start8:
	(cd server && sh start-cluster.sh 8)
sort:
	(cp server/instances.txt client/)
	(cd client && java -jar client.jar s3://cs6240sp16/climate s3://<bucket-name>/output instances.txt)

stop:
	(cd server && bash stop-cluster.sh)

clean:
	(cd textsock && make clean)
	(cd client && rm -rf client.jar instances.txt topten.txt output target project/target project/project)
	(cd server && rm -rf server.jar instances.txt target project/target project/project)

.PHONY: all clean


