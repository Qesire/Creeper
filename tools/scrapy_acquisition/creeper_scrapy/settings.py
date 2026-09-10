"""Conservative defaults for bounded source scouting.

Per-job limits such as JOBDIR, DEPTH_LIMIT and CLOSESPIDER_PAGECOUNT are passed
by the Creeper launcher. Keeping JOBDIR out of static settings is intentional:
Scrapy requires a distinct directory for every crawl job.
"""

BOT_NAME = "creeper_source_scout"
SPIDER_MODULES = ["creeper_scrapy.spiders"]
NEWSPIDER_MODULE = "creeper_scrapy.spiders"

ROBOTSTXT_OBEY = True
COOKIES_ENABLED = False
TELNETCONSOLE_ENABLED = False

CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 0.1
DOWNLOAD_TIMEOUT = 30
DOWNLOAD_MAXSIZE = 5 * 1024 * 1024
DOWNLOAD_WARNSIZE = 2 * 1024 * 1024
RETRY_TIMES = 2

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.25
AUTOTHROTTLE_MAX_DELAY = 10.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

SCHEDULER_DEBUG = True
FEED_EXPORT_ENCODING = "utf-8"
LOG_LEVEL = "INFO"

USER_AGENT = "Creeper/2.2 source-scout (historical-web research)"
