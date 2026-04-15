import pendulum
from airflow.decorators import dag, task

from stock_signal.mk_rss_pipeline import (
    collect_mk_rss_feed_to_bronze,
    should_include_mk_international_feed,
)


@dag(
    dag_id="collect_mk_rss_raw",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["stock-signal", "mk", "rss", "raw"],
)
def collect_mk_rss_raw():
    @task
    def collect_feed(source_feed):
        try:
            return collect_mk_rss_feed_to_bronze(source_feed)
        except Exception as exc:
            return {
                "status": "failed",
                "source_feed": source_feed,
                "error": str(exc),
            }

    @task
    def summarize_results(feed_results):
        successful_feeds = [
            result["source_feed"]
            for result in feed_results
            if result.get("status") == "success"
        ]
        failed_feeds = [
            {
                "source_feed": result["source_feed"],
                "error": result.get("error"),
            }
            for result in feed_results
            if result.get("status") == "failed"
        ]

        return {
            "successful_feed_count": len(successful_feeds),
            "failed_feed_count": len(failed_feeds),
            "successful_feeds": successful_feeds,
            "failed_feeds": failed_feeds,
        }

    feed_results = [
        collect_feed.override(task_id="collect_mk_economy_feed")("mk_economy"),
        collect_feed.override(task_id="collect_mk_stock_feed")("mk_stock"),
        collect_feed.override(task_id="collect_mk_corporate_management_feed")(
            "mk_corporate_management"
        ),
    ]

    if should_include_mk_international_feed():
        feed_results.append(
            collect_feed.override(task_id="collect_mk_international_feed")(
                "mk_international"
            )
        )

    summarize_results(feed_results)


collect_mk_rss_raw()
