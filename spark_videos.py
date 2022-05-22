from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql import functions as fn
from pyspark.sql.types import DateType

import datetime
import json
import os


class SparkApp:
    def __init__(self, name, directory):
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.save_dir = directory

    def write_result_to_file(self, result_json, file_name, number):
        name = os.path.splitext(os.path.basename(file_name))[0]
        save_name = self.save_dir + "/" + name + '-' + str(number) + '.json'
        with open(save_name, 'w+') as f:
            json.dump(result_json, f, indent=4)

    @staticmethod
    def get_category_name(file_name, category_id):
        name = os.path.splitext(os.path.basename(file_name))[0]
        json_file_name = "archive/" + name[:2] + "_category_id.json"
        with open(json_file_name, 'r') as f:
            category_json = json.load(f)
            for item in category_json["items"]:
                if item["id"] == str(category_id):
                    return item["snippet"]["title"]

    @staticmethod
    def date_to_str(date):
        return datetime.datetime.combine(date, datetime.datetime.min.time()).strftime('%y.%d.%m')

    def trending_videos(self, df, file_name):
        df_trending_videos = df.groupBy("video_id").count().sort(desc('count')).limit(10)
        videos_arr = [str(row.video_id) for row in df_trending_videos.collect()]
        result = {"videos": []}
        for video in videos_arr:
            df_video = df.filter(df.video_id == video).sort(desc('trending_date'))
            video_desc = {
                "id": video,
                "title": str(df_video.collect()[0].title),
                "description": str(df_video.collect()[0].description),
                "latest_views": int(df_video.collect()[0].views),
                "latest_likes": int(df_video.collect()[0].likes),
                "latest_dislikes": int(df_video.collect()[0].dislikes),
                "trending_days": []
            }
            for row in df_video.collect():
                day = {
                    "date": self.date_to_str(row.trending_date),
                    "views": int(row.views),
                    "likes": int(row.likes),
                    "dislikes": int(row.dislikes)
                }
                video_desc["trending_days"].append(day)
            result["videos"].append(video_desc)

        self.write_result_to_file(result, file_name, 1)

    def week_categories(self, df, file_name):
        last_date = df.agg(fn.max('trending_date').alias('last_date'))
        first_date = df.agg(fn.min('trending_date').alias('first_date'))
        datetime_f = datetime.datetime.strptime(str(first_date.collect()[0].first_date), '%Y-%m-%d')
        datetime_l = datetime.datetime.strptime(str(last_date.collect()[0].last_date), '%Y-%m-%d')
        first_week_start_date = datetime_f - datetime.timedelta(days=datetime_f.weekday())
        last_week_end_date = datetime_l + datetime.timedelta(days=7-datetime_f.weekday())
        week_first_date = first_week_start_date
        week_last_date = first_week_start_date + datetime.timedelta(days=6)

        result = {
            "weeks": []
        }
        while week_last_date <= last_week_end_date:
            first_str = week_first_date.strftime('%y.%d.%m')
            last_str = week_last_date.strftime('%y.%d.%m')
            date_l = week_last_date.date()
            date_f = week_first_date.date()
            week_df = df.filter((date_l >= df.trending_date) & (df.trending_date >= date_f))
            df_first_video_app = week_df.groupBy('video_id').agg(fn.min('trending_date').alias('first_date'))
            df_last_video_app = week_df.groupBy('video_id').agg(fn.max('trending_date').alias('last_date'))
            df_videos_dates = week_df.join(df_first_video_app, on='video_id').join(df_last_video_app, on='video_id')
            df_videos_to_count = df_videos_dates.filter(df_videos_dates.first_date != df_videos_dates.last_date)
            df_videos_first = df_videos_to_count.filter(
                df_videos_to_count.trending_date == df_videos_to_count.first_date).select('video_id', 'views')\
                .withColumnRenamed('views', 'first_views')
            df_videos_last = df_videos_to_count.filter(
                df_videos_to_count.trending_date == df_videos_to_count.last_date)\
                .select('video_id', 'category_id', 'views').withColumnRenamed('views', 'last_views')
            df_videos = df_videos_first.join(df_videos_last, on='video_id')
            df_videos = df_videos.withColumn('total_views', df_videos.last_views - df_videos.first_views)
            df_category = df_videos.groupBy('category_id').sum('total_views')\
                .withColumnRenamed("sum(total_views)", "all_views").sort(desc('all_views')).limit(1)
            week_desc = {
                "start_date": first_str,
                "end_date": last_str,
                "category_id": int(df_category.collect()[0].category_id),
                "category_name": "",
                "number_of_videos": 0,
                "total_views": int(df_category.collect()[0].all_views),
                "video_ids": []
            }
            if week_first_date < datetime_f:
                week_desc["start_date"] = datetime_f.strftime('%y.%d.%m')
            if week_last_date > datetime_l:
                week_desc["end_date"] = datetime_l.strftime('%y.%d.%m')
            week_desc["category_name"] = self.get_category_name(file_name, week_desc["category_id"])
            week_desc["video_ids"] = [str(row.video_id) for row in df_videos
                                      .filter(df_videos.category_id == week_desc["category_id"]).collect()]
            week_desc["number_of_videos"] = len(week_desc["video_ids"])
            result["weeks"].append(week_desc)

            week_first_date = week_first_date + datetime.timedelta(days=7)
            week_last_date = week_last_date + datetime.timedelta(days=7)

        self.write_result_to_file(result, file_name, 2)

    def month_tags(self, df, file_name):
        last_date = df.agg(fn.max('trending_date').alias('last_date'))
        first_date = df.agg(fn.min('trending_date').alias('first_date'))
        datetime_f = datetime.datetime.strptime(str(first_date.collect()[0].first_date), '%Y-%m-%d')
        datetime_l = datetime.datetime.strptime(str(last_date.collect()[0].last_date), '%Y-%m-%d')
        first_month_start_date = datetime_f.replace(day=1)
        last_month_end_date = datetime_l.replace(day=1, month=(datetime_l.month+1) % 12) - datetime.timedelta(days=1)
        month_first_day = first_month_start_date
        month_last_day = first_month_start_date.replace(day=1, month=datetime_f.month % 12 + 1,
                                                        year=datetime_f.year + int(
                                                            datetime_f.month / 12)) - datetime.timedelta(
            days=1)

        result = {
            "months": []
        }
        while month_last_day <= last_month_end_date:
            first_str = month_first_day.strftime('%y.%d.%m')
            last_str = month_last_day.strftime('%y.%d.%m')
            date_l = month_last_day.date()
            date_f = month_first_day.date()
            month_desc = {
                "start_date": first_str,
                "end_date": last_str,
                "tags": []
            }
            if month_first_day < datetime_f:
                month_desc["start_date"] = datetime_f.strftime('%y.%d.%m')
            if month_last_day > datetime_l:
                month_desc["end_date"] = datetime_l.strftime('%y.%d.%m')
            df_month_videos = df.filter((date_l >= df.trending_date) & (df.trending_date >= date_f))
            df_videos_tags = df_month_videos.select('video_id', 'tags').dropDuplicates()
            tags_dict = {}
            for row in df_videos_tags.collect():
                tags = str(row.tags)
                if tags == '[none]':
                    continue
                tags = tags.split("|")
                tags = [tag.replace('"', '') for tag in tags]
                video_id = row.video_id
                for tag in tags:
                    if tag in tags_dict.keys():
                        tags_dict[tag].append(video_id)
                    else:
                        tags_dict[tag] = [video_id]
            tags_dict = {k: v for k, v in sorted(tags_dict.items(), key=lambda item: len(item[1]), reverse=True)[:10]}
            for tag in tags_dict.keys():
                tag_desc = {
                    "tag": tag,
                    "number_of_videos": len(tags_dict[tag]),
                    "video_ids": tags_dict[tag]
                }
                month_desc["tags"].append(tag_desc)

            result["months"].append(month_desc)

            month_first_day = month_last_day + datetime.timedelta(days=1)
            month_last_day = month_first_day.replace(day=1, month=(month_first_day.month + 1) % 12,
                                                     year=month_first_day.year + int(
                                                         month_first_day.month / 12)) - datetime.timedelta(
                days=1)

        self.write_result_to_file(result, file_name, 3)

    def most_viewed_channels(self, df, file_name):
        df_last_date = df.groupBy('video_id').agg(fn.max('trending_date').alias('correct_date'))
        df_date = df.join(df_last_date, on='video_id')
        df_last_video = df_date.filter(df_date.trending_date == df_date.correct_date)
        df_channels_views = df_last_video.groupBy('channel_title')\
            .sum('views').withColumnRenamed("sum(views)", "total_views").sort(desc('total_views')).limit(20)

        result = {"channels": []}
        for row in df_channels_views.collect():
            channel_desc = {
                "channel_name": str(row.channel_title),
                "start_date": "",
                "end_date": "",
                "total_views": int(row.total_views),
                "videos_views": []
            }
            channel_desc["start_date"] = self.date_to_str(df.filter(df.channel_title == channel_desc["channel_name"]) \
                                                          .agg(fn.min('trending_date').alias('start_date')).collect()[
                                                              0].start_date)
            channel_desc["end_date"] = self.date_to_str(df.filter(df.channel_title == channel_desc["channel_name"]) \
                                                        .agg(fn.max('trending_date').alias('end_date')).collect()[
                                                            0].end_date)
            df_channel_videos = df_last_video.filter(df_last_video.channel_title == channel_desc["channel_name"])
            for video_row in df_channel_videos.collect():
                video_stat = {
                    "video_id": str(video_row.video_id),
                    "views": int(video_row.views)
                }
                channel_desc["videos_views"].append(video_stat)
            result["channels"].append(channel_desc)

        self.write_result_to_file(result, file_name, 4)

    def most_trending_channels(self, df, file_name):
        df_trending_videos = df.groupBy("video_id", "title", "channel_title").count()
        df_trending_channels = df_trending_videos.groupBy('channel_title')\
            .sum('count').withColumnRenamed("sum(count)", "total_days").sort(desc('total_days')).limit(10)

        result = {"channels": []}
        for channel_row in df_trending_channels.collect():
            channel_desc = {
                "channel_name": str(channel_row.channel_title),
                "total_trending_days": str(channel_row.total_days),
                "videos_days": []
            }
            df_channel_videos = df_trending_videos.filter(df_trending_videos.channel_title
                                                          == channel_desc["channel_name"])\
                .withColumnRenamed("count", "total_days")
            for video_row in df_channel_videos.collect():
                video_day = {
                    "video_id": str(video_row.video_id),
                    "video_title": str(video_row.title),
                    "trending_days": int(video_row.total_days)
                }
                channel_desc["videos_days"].append(video_day)
            result["channels"].append(channel_desc)

        self.write_result_to_file(result, file_name, 5)

    def best_videos_in_category(self, df, file_name):
        df_viewed_videos = df.filter(df.views > 100000).withColumn("ratio", df.likes/df.dislikes)
        df_categories = df.select('category_id').distinct()

        result = {
            "categories": []
        }
        for category_row in df_categories.collect():
            category_desc = {
                "category_id": int(category_row.category_id),
                "category_name": "",
                "videos": []
            }
            category_desc["category_name"] = self.get_category_name(file_name, category_desc["category_id"])

            df_category_videos = df_viewed_videos.filter(df_viewed_videos.category_id == category_desc["category_id"])
            df_max_ratio = df_category_videos.groupBy('video_id').agg(fn.max('ratio').alias('max_ratio'))
            df_ratio_videos = df_category_videos.join(df_max_ratio, on='video_id')
            df_videos = df_ratio_videos.filter(df_ratio_videos.ratio == df_ratio_videos.max_ratio).sort(desc('ratio'))\
                .limit(10)
            for video_row in df_videos.collect():
                video_desc = {
                    "video_id": str(video_row.video_id),
                    "video_title": str(video_row.title),
                    "ratio_likes_dislikes": str(video_row.ratio),
                    "views": int(video_row.views)
                }
                category_desc["videos"].append(video_desc)
            result["categories"].append(category_desc)

        self.write_result_to_file(result, file_name, 6)

    def process_file(self, file_name):
        df = self.spark.read.format("csv").option("header", "true").option("multiline", "true")\
            .option("inferSchema", "true").load(file_name)
        df_date = df.withColumn("date", fn.to_date(df.trending_date, 'yy.dd.MM'))
        df = df_date.select('video_id', 'date', 'title', 'channel_title', 'category_id', 'tags', 'views',
                            'likes', 'dislikes', 'description').withColumnRenamed('date', 'trending_date')
        self.trending_videos(df, file_name)
        self.week_categories(df, file_name)
        self.month_tags(df, file_name)
        self.most_viewed_channels(df, file_name)
        self.most_trending_channels(df, file_name)
        self.best_videos_in_category(df, file_name)


def main():
    spark_name = 'SparkTransactionsProject'
    save_dir = "videos_results"
    spark_app = SparkApp(spark_name, save_dir)
    csv_files = list(filter(lambda f: f.endswith('.csv'), os.listdir("./archive")))
    for file_name in csv_files:
        spark_app.process_file("archive/" + file_name)


if __name__ == "__main__":
    main()
