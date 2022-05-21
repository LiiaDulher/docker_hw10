# Docker hw10
Spark 2

## Team: [Liia_Dulher](https://github.com/LiiaDulher)

### Usage
````
$ sudo chmod +x shutdown-spark.sh
````
````
$ docker-compose up -d
$ ./run-program.sh
# use inside-container commands
$ ./shutdown-spark.sh
````
Inside container:
````
cd /opt/app
spark-submit --master spark://spark-master:7077 --deploy-mode client spark_videos.py
exit
````
