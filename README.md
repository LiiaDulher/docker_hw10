# Docker hw10
Spark 2

## Team: [Liia_Dulher](https://github.com/LiiaDulher)

### Prerequiments
Please put all source files into sub-directory <b>archive</b> in this directory.<br>
Create sub-directory <b>videos_results</b> in this directory. It must be <b>EMPTY</b>. There will be all result files.

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
### Results
Each result file from <i>source_file.csv</i> has name: source_file-N.json, where N is query number.
