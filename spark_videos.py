from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

import json
import os


class SparkApp:
    def __init__(self, name, directory):
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.save_dir = directory

    def trending_videos(self, df, file_name):
        trending = df.groupBy("video_id").count().sort(desc('count')).limit(10)
        videos_arr = [str(row.video_id) for row in trending.collect()]
        result = {"videos": []}
        for video in videos_arr:
            video_df = df.filter(df.video_id == video).sort(desc('trending_date'))
            video_desc = {
                "id": video,
                "title": str(video_df.collect()[0].title),
                "description": str(video_df.collect()[0].description),
                "latest_views": int(video_df.collect()[0].views),
                "latest_likes": int(video_df.collect()[0].likes),
                "latest_dislikes": int(video_df.collect()[0].dislikes),
                "trending_days": []
            }
            for row in video_df.collect():
                day = {
                    "date": str(row.trending_date),
                    "views": int(row.views),
                    "likes": int(row.likes),
                    "dislikes": int(row.dislikes)
                }
                video_desc["trending_days"].append(day)
            result["videos"].append(video_desc)

        name = os.path.splitext(os.path.basename(file_name))[0]
        with open(self.save_dir + "/" + name + '-1.json', 'w+') as f:
            json.dump(result, f, indent=4)

    def process_file(self, file_name):
        df = self.spark.read.format("csv").option("header", "true").option("multiline", "true").load(file_name)
        self.trending_videos(df, file_name)


def main():
    spark_name = 'SparkTransactionsProject'
    file_name = 'archive/CAvideos.csv'
    save_dir = "videos_results"
    spark_app = SparkApp(spark_name, save_dir)
    spark_app.process_file(file_name)


if __name__ == "__main__":
    main()
