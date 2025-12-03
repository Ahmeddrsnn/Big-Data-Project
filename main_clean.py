"""
Influence and Interaction in r/Turkey Reddit Community
=====================================================
End‑to‑end PySpark pipeline to analyze user interactions and nationalist sentiment
in the r/Turkey subreddit.

Features:
---------
1. Data Ingestion:
   - Streams posts & comments from Reddit into Kafka
   - Supports backfill of historical data
   - Configurable polling intervals and limits

2. Graph Analysis:
   - Creates user-user interaction networks
   - Computes PageRank for influence analysis
   - Detects communities using Label Propagation

3. Sentiment Analysis:
   - Uses Turkish BERT for sentiment classification
   - Tracks nationalist content and sentiment over time
   - Stores results in Parquet format for analysis

Configuration:
-------------
All parameters are configurable via environment variables or a config file.
See the Config class below for available options.

Output:
-------
- User metrics (PageRank, communities) -> ./data/user_metrics/
- Sentiment analysis -> ./data/nationalist_sentiment/
- Checkpoints -> ./checkpoints/

Usage:
------
1. Set up environment variables or create a .env file
2. Run: python inges_sep_inparallel.py
3. Monitor progress in logs
4. Analyze results in the data/ directory
"""

from __future__ import annotations
import os
import json
import time
import logging
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timezone
from pathlib import Path

import praw
from confluent_kafka import Producer
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, lower, when, lit, concat_ws, count, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, BooleanType, TimestampType, DoubleType
)

from config import Config, load_config
from progress import ProgressTracker, ProgressMonitor

# ─────────────────────────────────────────────────────────────────────────────
# Configuration Management
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Config:
    """Centralized configuration management."""
    # Reddit API settings
    reddit_client_id: str
    reddit_client_secret: str
    reddit_username: str
    reddit_password: str
    reddit_user_agent: str = "rTurkey_net_analysis_bot"
    
    # Kafka settings
    kafka_brokers: str = "localhost:9092"
    kafka_topics: Dict[str, str] = None
    
    # Data collection settings
    subreddits: List[str] = None
    poll_seconds: int = 600  # 10 minutes
    max_posts: int = 10000  # 10,000 posts limit
    backfill_days: int = 365  # 1 year
    
    # Spark settings
    spark_app_name: str = "rTurkeyNetAnalysis"
    spark_master: str = "local[*]"
    spark_shuffle_partitions: int = 8
    
    # Storage settings
    data_dir: str = "./data"
    checkpoint_dir: str = "./checkpoints"
    
    def __post_init__(self):
        """Initialize default values and validate configuration."""
        if self.kafka_topics is None:
            self.kafka_topics = {
                "interactions": "reddit_interactions",
                "posts": "reddit_posts",
                "comments": "reddit_comments"
            }
        if self.subreddits is None:
            self.subreddits = ["Turkey"]
        
        # Create necessary directories
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.checkpoint_dir).mkdir(parents=True, exist_ok=True)
        
        # Validate required settings
        required = ["reddit_client_id", "reddit_client_secret", 
                   "reddit_username", "reddit_password"]
        missing = [f for f in required if not getattr(self, f)]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")

def load_config() -> Config:
    """Load configuration from environment variables or .env file."""
    load_dotenv()  # Load from .env file if it exists
    
    return Config(
        reddit_client_id=os.getenv("REDDIT_CLIENT_ID"),
        reddit_client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        reddit_username=os.getenv("REDDIT_USERNAME"),
        reddit_password=os.getenv("REDDIT_PASSWORD"),
        reddit_user_agent=os.getenv("REDDIT_USER_AGENT", "rTurkey_net_analysis_bot"),
        kafka_brokers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
        poll_seconds=int(os.getenv("POLL_SECONDS", "600")),
        max_posts=int(os.getenv("MAX_POSTS", "10000")),  # Default to 10,000
        backfill_days=int(os.getenv("BACKFILL_DAYS", "365"))
    )

# ─────────────────────────────────────────────────────────────────────────────
# Logging and Monitoring
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging with a consistent format."""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("reddit_analysis.log")
        ]
    )
    
    # Reduce Spark's logging verbosity but keep important messages
    if 'spark' in globals():
        spark.sparkContext.setLogLevel("WARN")

class StreamMonitor:
    """Monitors and logs the status of Spark streams."""
    
    def __init__(self, streams: Dict[str, Any], interval_seconds: int = 30):
        self.streams = streams
        self.interval = interval_seconds
        self._stop_event = threading.Event()
        self._monitor_thread = None
    
    def start(self):
        """Start the stream monitoring thread."""
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True
        )
        self._monitor_thread.start()
        logging.info("Started stream monitoring (interval: %d seconds)", 
                    self.interval)
    
    def stop(self):
        """Stop the stream monitoring thread."""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        logging.info("Stopped stream monitoring")
    
    def _monitor_loop(self):
        """Monitor loop that logs stream status at regular intervals."""
        while not self._stop_event.is_set():
            try:
                for name, stream in self.streams.items():
                    status = stream.status
                    logging.info(
                        "Stream '%s' status: %s (processed: %d, active: %s)",
                        name,
                        status["message"],
                        status.get("processedRowsPerSecond", 0),
                        status.get("isActive", False)
                    )
            except Exception as e:
                logging.error("Error monitoring streams: %s", str(e))
            time.sleep(self.interval)

class ProgressMonitor:
    """Monitors and logs progress at regular intervals."""
    
    def __init__(self, tracker: ProgressTracker, interval_seconds: int = 30):  # Changed from 60 to 30
        self.tracker = tracker
        self.interval = interval_seconds
        self._stop_event = threading.Event()
        self._monitor_thread = None
    
    def start(self):
        """Start the progress monitoring thread."""
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True
        )
        self._monitor_thread.start()
        logging.info("Started progress monitoring (interval: %d seconds)", 
                    self.interval)
    
    def stop(self):
        """Stop the progress monitoring thread."""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        logging.info("Stopped progress monitoring")
    
    def _monitor_loop(self):
        """Monitor loop that logs progress at regular intervals."""
        while not self._stop_event.is_set():
            try:
                self.tracker.log_progress()
                # Ayrıca dosya boyutlarını da kontrol et
                self._log_storage_stats()
            except Exception as e:
                logging.error("Error in progress monitoring: %s", str(e))
            time.sleep(self.interval)
    
    def _log_storage_stats(self):
        """Log storage statistics for data directories."""
        try:
            import os
            from pathlib import Path
            
            data_dirs = [
                "data/posts",
                "data/comments",
                "data/interactions",
                "data/nationalist_sentiment/posts",
                "data/nationalist_sentiment/comments"
            ]
            
            for dir_path in data_dirs:
                if os.path.exists(dir_path):
                    total_size = sum(f.stat().st_size for f in Path(dir_path).rglob('*') if f.is_file())
                    file_count = sum(1 for f in Path(dir_path).rglob('*') if f.is_file())
                    logging.info(
                        "Storage stats for %s: %d files, %.2f MB",
                        dir_path,
                        file_count,
                        total_size / (1024 * 1024)
                    )
        except Exception as e:
            logging.error("Error getting storage stats: %s", str(e))

# ─────────────────────────────────────────────────────────────────────────────
# Data Ingestion
# ─────────────────────────────────────────────────────────────────────────────

def _delivery(err, msg):
    """Kafka delivery callback."""
    if err:
        logging.error("❌ Kafka delivery failed: %s", err)
    else:
        logging.debug("✔︎ Kafka delivery: topic=%s offset=%d key=%s", 
                     msg.topic(), msg.offset(), msg.key().decode() if msg.key() else None)

def _emit_post(post, producer: Producer, config: Config, progress: ProgressTracker):
    """Emit a single post to Kafka."""
    payload = {
        "id": post.id,
        "author": str(post.author),
        "created_utc": int(post.created_utc),
        "title": post.title,
        "selftext": post.selftext,
        "score": post.score,
        "num_comments": post.num_comments,
        "permalink": post.permalink,
        "subreddit": post.subreddit.display_name,
    }
    producer.produce(
        config.kafka_topics["posts"],
        key=payload["id"],
        value=json.dumps(payload).encode(),
        on_delivery=_delivery
    )
    progress.increment("posts_processed")

def backfill_submissions(config: Config, progress: ProgressTracker):
    """Backfill historical submissions."""
    logging.info("⏮  Starting backfill for last %d days (max %d posts)...", 
                config.backfill_days, config.max_posts)
    
    try:
        reddit = praw.Reddit(
            client_id=config.reddit_client_id,
            client_secret=config.reddit_client_secret,
            username=config.reddit_username,
            password=config.reddit_password,
            user_agent=config.reddit_user_agent
        )
        
        # Test Reddit API connection
        try:
            reddit.user.me()
            logging.info("Successfully connected to Reddit API as %s", reddit.user.me())
        except Exception as e:
            logging.error("Failed to connect to Reddit API: %s", str(e))
            raise
        
        producer = Producer({
            "bootstrap.servers": config.kafka_brokers,
            "acks": "all",
            "linger.ms": 100,
        })
        
        # Test Kafka connection
        try:
            producer.list_topics(timeout=10)
            logging.info("Successfully connected to Kafka")
        except Exception as e:
            logging.error("Failed to connect to Kafka: %s", str(e))
            raise
        
        cutoff_time = int(datetime.now(timezone.utc).timestamp()) - (config.backfill_days * 24 * 3600)
        logging.info("Backfill cutoff time: %s", datetime.fromtimestamp(cutoff_time, timezone.utc))
        
        subreddit = reddit.subreddit("+".join(config.subreddits))
        logging.info("Starting to fetch posts from subreddit(s): %s", ", ".join(config.subreddits))
        
        post_count = 0
        error_count = 0
        last_successful_post_time = None
        consecutive_errors = 0
        
        for post in subreddit.new(limit=None):
            try:
                if post.created_utc < cutoff_time:
                    logging.info("Reached cutoff time, stopping backfill")
                    break
                    
                if post_count >= config.max_posts:
                    logging.info("Reached max posts limit (%d), stopping backfill", config.max_posts)
                    break
                
                # Log post details for debugging
                if post_count % 100 == 0:
                    logging.debug("Processing post: id=%s, created=%s, author=%s, title=%s",
                                post.id,
                                datetime.fromtimestamp(post.created_utc, timezone.utc),
                                post.author,
                                post.title[:50] + "..." if len(post.title) > 50 else post.title)
                
                _emit_post(post, producer, config, progress)
                post_count += 1
                last_successful_post_time = post.created_utc
                consecutive_errors = 0  # Reset error counter on success
                
                if post_count % 100 == 0:
                    producer.flush()
                    logging.info("Backfill progress: %d/%d posts processed (latest post from %s, errors: %d)", 
                               post_count, config.max_posts,
                               datetime.fromtimestamp(post.created_utc, timezone.utc),
                               error_count)
                    time.sleep(1)  # Rate limiting
                    
            except praw.exceptions.PRAWException as e:
                error_count += 1
                consecutive_errors += 1
                logging.error("Reddit API error on post %s: %s", post.id if hasattr(post, 'id') else 'unknown', str(e))
                if consecutive_errors >= 5:
                    logging.error("Too many consecutive errors (%d), stopping backfill", consecutive_errors)
                    break
                time.sleep(5)  # Wait longer on API errors
                
            except Exception as e:
                error_count += 1
                consecutive_errors += 1
                logging.error("Unexpected error processing post: %s", str(e), exc_info=True)
                if consecutive_errors >= 5:
                    logging.error("Too many consecutive errors (%d), stopping backfill", consecutive_errors)
                    break
                time.sleep(2)
        
        producer.flush()
        
        # Final status report
        logging.info("⏮  Backfill completed with status:")
        logging.info("  - Total posts processed: %d/%d", post_count, config.max_posts)
        logging.info("  - Total errors encountered: %d", error_count)
        logging.info("  - Time range: %s to %s",
                    datetime.fromtimestamp(cutoff_time, timezone.utc),
                    datetime.fromtimestamp(last_successful_post_time, timezone.utc) if last_successful_post_time else "N/A")
        if error_count > 0:
            logging.warning("Backfill completed with %d errors", error_count)
            
    except Exception as e:
        logging.error("Fatal error in backfill: %s", str(e), exc_info=True)
        raise

def stream_submissions(stop_evt: threading.Event, config: Config, progress: ProgressTracker):
    """Stream new submissions."""
    logging.info("Starting submission stream thread...")
    try:
        reddit = praw.Reddit(
            client_id=config.reddit_client_id,
            client_secret=config.reddit_client_secret,
            username=config.reddit_username,
            password=config.reddit_password,
            user_agent=config.reddit_user_agent
        )
        
        producer = Producer({
            "bootstrap.servers": config.kafka_brokers,
            "acks": "all",
            "linger.ms": 100,
        })
        
        logging.info("Connected to Reddit API and Kafka producer")
        
        for post in reddit.subreddit("+".join(config.subreddits)).stream.submissions(skip_existing=True):
            if stop_evt.is_set():
                logging.info("Stop event received, stopping submission stream")
                break
                
            try:
                _emit_post(post, producer, config, progress)
                producer.poll(0)
                if progress.posts_processed % 10 == 0:  # Her 10 postta bir log
                    logging.info("Submission stream: %d posts processed", progress.posts_processed)
            except Exception as e:
                logging.error("Error processing submission: %s", str(e))
                
    except Exception as e:
        logging.error("Fatal error in submission stream: %s", str(e), exc_info=True)
    finally:
        producer.flush()
        logging.info("Submission stream thread stopped")

def stream_comments(stop_evt: threading.Event, config: Config, progress: ProgressTracker):
    """Stream new comments and create interaction edges."""
    logging.info("Starting comment stream thread...")
    try:
        reddit = praw.Reddit(
            client_id=config.reddit_client_id,
            client_secret=config.reddit_client_secret,
            username=config.reddit_username,
            password=config.reddit_password,
            user_agent=config.reddit_user_agent
        )
        
        producer = Producer({
            "bootstrap.servers": config.kafka_brokers,
            "acks": "all",
            "linger.ms": 100,
        })
        
        logging.info("Connected to Reddit API and Kafka producer")
        
        for comment in reddit.subreddit("+".join(config.subreddits)).stream.comments(skip_existing=True):
            if stop_evt.is_set():
                logging.info("Stop event received, stopping comment stream")
                break
                
            try:
                # Process comment
                payload = {
                    "id": comment.id,
                    "parent_id": comment.parent_id,
                    "post_id": comment.link_id.split("_")[-1],
                    "author": str(comment.author),
                    "created_utc": int(comment.created_utc),
                    "body": comment.body,
                    "score": comment.score,
                    "subreddit": comment.subreddit.display_name,
                }
                
                # Emit comment
                producer.produce(
                    config.kafka_topics["comments"],
                    key=payload["id"],
                    value=json.dumps(payload).encode(),
                    on_delivery=_delivery
                )
                progress.increment("comments_processed")
                
                # Create interaction edge
                try:
                    parent_author = str(comment.parent().author) if comment.parent_id.startswith("t1_") else str(comment.submission.author)
                    if parent_author and parent_author != "None":
                        edge = {
                            "src": payload["author"],
                            "dst": parent_author,
                            "weight": 1,
                            "ts": payload["created_utc"],
                            "post_id": payload["post_id"],
                        }
                        producer.produce(
                            config.kafka_topics["interactions"],
                            key=f"{edge['src']}|{edge['dst']}|{edge['ts']}",
                            value=json.dumps(edge).encode(),
                            on_delivery=_delivery
                        )
                        progress.increment("interactions_created")
                except Exception as e:
                    logging.warning("Failed to create interaction edge: %s", str(e))
                
                producer.poll(0)
                if progress.comments_processed % 10 == 0:  # Her 10 yorumda bir log
                    logging.info("Comment stream: %d comments, %d interactions processed", 
                               progress.comments_processed, progress.interactions_created)
            except Exception as e:
                logging.error("Error processing comment: %s", str(e))
                
    except Exception as e:
        logging.error("Fatal error in comment stream: %s", str(e), exc_info=True)
    finally:
        producer.flush()
        logging.info("Comment stream thread stopped")

def backfill_comments(config: Config, progress: ProgressTracker):
    """Backfill historical comments."""
    logging.info("⏮  Starting comment backfill for last %d days...", 
                config.backfill_days)
    
    try:
        reddit = praw.Reddit(
            client_id=config.reddit_client_id,
            client_secret=config.reddit_client_secret,
            username=config.reddit_username,
            password=config.reddit_password,
            user_agent=config.reddit_user_agent
        )
        
        # Test Reddit API connection
        try:
            reddit.user.me()
            logging.info("Successfully connected to Reddit API as %s", reddit.user.me())
        except Exception as e:
            logging.error("Failed to connect to Reddit API: %s", str(e))
            raise
        
        producer = Producer({
            "bootstrap.servers": config.kafka_brokers,
            "acks": "all",
            "linger.ms": 100,
        })
        
        # Test Kafka connection
        try:
            producer.list_topics(timeout=10)
            logging.info("Successfully connected to Kafka")
        except Exception as e:
            logging.error("Failed to connect to Kafka: %s", str(e))
            raise
        
        cutoff_time = int(datetime.now(timezone.utc).timestamp()) - (config.backfill_days * 24 * 3600)
        logging.info("Comment backfill cutoff time: %s", datetime.fromtimestamp(cutoff_time, timezone.utc))
        
        subreddit = reddit.subreddit("+".join(config.subreddits))
        logging.info("Starting to fetch comments from subreddit(s): %s", ", ".join(config.subreddits))
        
        comment_count = 0
        error_count = 0
        last_successful_comment_time = None
        consecutive_errors = 0
        
        # Get comments from each post
        for post in subreddit.new(limit=None):
            if post.created_utc < cutoff_time:
                continue
                
            try:
                post.comments.replace_more(limit=None)  # Get all comments, including nested ones
                for comment in post.comments.list():
                    try:
                        if comment.created_utc < cutoff_time:
                            continue
                            
                        # Process comment
                        payload = {
                            "id": comment.id,
                            "parent_id": comment.parent_id,
                            "post_id": post.id,
                            "author": str(comment.author),
                            "created_utc": int(comment.created_utc),
                            "body": comment.body,
                            "score": comment.score,
                            "subreddit": comment.subreddit.display_name,
                        }
                        
                        # Emit comment
                        producer.produce(
                            config.kafka_topics["comments"],
                            key=payload["id"],
                            value=json.dumps(payload).encode(),
                            on_delivery=_delivery
                        )
                        progress.increment("comments_processed")
                        
                        # Create interaction edge
                        try:
                            parent_author = str(comment.parent().author) if comment.parent_id.startswith("t1_") else str(post.author)
                            if parent_author and parent_author != "None":
                                edge = {
                                    "src": payload["author"],
                                    "dst": parent_author,
                                    "weight": 1,
                                    "ts": payload["created_utc"],
                                    "post_id": payload["post_id"],
                                }
                                producer.produce(
                                    config.kafka_topics["interactions"],
                                    key=f"{edge['src']}|{edge['dst']}|{edge['ts']}",
                                    value=json.dumps(edge).encode(),
                                    on_delivery=_delivery
                                )
                                progress.increment("interactions_created")
                        except Exception as e:
                            logging.warning("Failed to create interaction edge: %s", str(e))
                        
                        comment_count += 1
                        last_successful_comment_time = comment.created_utc
                        
                        if comment_count % 100 == 0:
                            producer.flush()
                            logging.info("Comment backfill progress: %d comments processed (latest from %s, errors: %d)", 
                                       comment_count,
                                       datetime.fromtimestamp(comment.created_utc, timezone.utc),
                                       error_count)
                            time.sleep(1)  # Rate limiting
                            
                    except Exception as e:
                        error_count += 1
                        consecutive_errors += 1
                        logging.error("Error processing comment %s: %s", 
                                    comment.id if hasattr(comment, 'id') else 'unknown', 
                                    str(e))
                        if consecutive_errors >= 5:
                            logging.error("Too many consecutive errors (%d), stopping comment backfill", 
                                        consecutive_errors)
                            break
                        time.sleep(2)
                        
            except Exception as e:
                error_count += 1
                logging.error("Error processing post %s for comments: %s", 
                            post.id if hasattr(post, 'id') else 'unknown', 
                            str(e))
                time.sleep(2)
        
        producer.flush()
        
        # Final status report
        logging.info("⏮  Comment backfill completed with status:")
        logging.info("  - Total comments processed: %d", comment_count)
        logging.info("  - Total errors encountered: %d", error_count)
        logging.info("  - Time range: %s to %s",
                    datetime.fromtimestamp(cutoff_time, timezone.utc),
                    datetime.fromtimestamp(last_successful_comment_time, timezone.utc) 
                    if last_successful_comment_time else "N/A")
        if error_count > 0:
            logging.warning("Comment backfill completed with %d errors", error_count)
            
    except Exception as e:
        logging.error("Fatal error in comment backfill: %s", str(e), exc_info=True)
        raise

# ─────────────────────────────────────────────────────────────────────────────
# Spark Processing
# ─────────────────────────────────────────────────────────────────────────────

class SparkProcessor:
    """Handles Spark-based data processing and analysis."""
    
    def __init__(self, config: Config):
        self.config = config
        self.spark = self._init_spark()
        self._setup_streams()
    
    def _init_spark(self) -> SparkSession:
        """Initialize and configure Spark session."""
        return (SparkSession.builder
                .appName(self.config.spark_app_name)
                .master(self.config.spark_master)
                .config("spark.sql.shuffle.partitions", str(self.config.spark_shuffle_partitions))
                .config("spark.jars.packages", ",".join([
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2",
                    "graphframes:graphframes:0.8.3-spark3.4-s_2.12"
                ]))
                .config("spark.sql.streaming.checkpointLocation", 
                       str(Path(self.config.checkpoint_dir) / "spark"))
                .getOrCreate())
    
    def _setup_streams(self):
        """Set up Spark streaming queries for all data types."""
        # Post şeması
        post_schema = StructType([
            StructField("id", StringType()),
            StructField("author", StringType()),
            StructField("created_utc", LongType()),
            StructField("title", StringType()),
            StructField("selftext", StringType()),
            StructField("score", IntegerType()),
            StructField("num_comments", IntegerType()),
            StructField("permalink", StringType()),
            StructField("subreddit", StringType())
        ])

        # Yorum şeması
        comment_schema = StructType([
            StructField("id", StringType()),
            StructField("parent_id", StringType()),
            StructField("post_id", StringType()),
            StructField("author", StringType()),
            StructField("created_utc", LongType()),
            StructField("body", StringType()),
            StructField("score", IntegerType()),
            StructField("subreddit", StringType())
        ])

        # Etkileşim şeması
        interaction_schema = StructType([
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("weight", IntegerType()),
            StructField("ts", LongType()),
            StructField("post_id", StringType())
        ])

        # Milliyetçi içerik tespiti için regex desenleri
        from nationalist_patterns import pattern_parts
        import re
        
        # Spark SQL için regex desenini hazırla
        # Özel karakterleri escape et ve basit bir regex kullan
        nationalist_keywords = [
            "türk", "vatan", "bayrak", "şehit", "millet",
            "ülkücü", "bozkurt", "milliyetçi", "milli",
            "atatürk", "kahraman", "zafer", "istiklal"
        ]
        nationalist_pattern = "|".join(nationalist_keywords)
        
        # Posts stream
        posts_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka_brokers) \
            .option("subscribe", self.config.kafka_topics["posts"]) \
            .load() \
            .select(from_json(col("value").cast("string"), post_schema).alias("data")) \
            .select("data.*")

        # Posts'ları kaydet
        posts_query = posts_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", "data/posts/") \
            .option("checkpointLocation", "checkpoints/posts/") \
            .start()

        # Milliyetçi posts'ları tespit et ve kaydet
        nationalist_posts_df = posts_df \
            .withColumn("text", concat_ws(" ", col("title"), col("selftext"))) \
            .withColumn("text_lower", lower(col("text"))) \
            .withColumn("is_nationalist", 
                       expr(f"text_lower rlike '(?i)({nationalist_pattern})'")) \
            .filter(col("is_nationalist")) \
            .withColumn("detected_keywords", 
                       expr(f"regexp_extract_all(text_lower, '(?i)({nationalist_pattern})', 0)")) \
            .withColumn("created_at", from_unixtime(col("created_utc"))) \
            .drop("text_lower")

        nationalist_posts_query = nationalist_posts_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", "data/nationalist_sentiment/posts/") \
            .option("checkpointLocation", "checkpoints/nationalist_posts/") \
            .start()

        # Comments stream
        comments_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka_brokers) \
            .option("subscribe", self.config.kafka_topics["comments"]) \
            .load() \
            .select(from_json(col("value").cast("string"), comment_schema).alias("data")) \
            .select("data.*")

        # Comments'leri kaydet
        comments_query = comments_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", "data/comments/") \
            .option("checkpointLocation", "checkpoints/comments/") \
            .start()

        # Milliyetçi yorumları tespit et ve kaydet
        nationalist_comments_df = comments_df \
            .withColumn("body_lower", lower(col("body"))) \
            .withColumn("is_nationalist", 
                       expr(f"body_lower rlike '(?i)({nationalist_pattern})'")) \
            .filter(col("is_nationalist")) \
            .withColumn("detected_keywords", 
                       expr(f"regexp_extract_all(body_lower, '(?i)({nationalist_pattern})', 0)")) \
            .withColumn("created_at", from_unixtime(col("created_utc"))) \
            .drop("body_lower")

        nationalist_comments_query = nationalist_comments_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", "data/nationalist_sentiment/comments/") \
            .option("checkpointLocation", "checkpoints/nationalist_comments/") \
            .start()

        # Interactions stream
        interactions_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka_brokers) \
            .option("subscribe", self.config.kafka_topics["interactions"]) \
            .load() \
            .select(from_json(col("value").cast("string"), interaction_schema).alias("data")) \
            .select("data.*")

        # Interactions'ları kaydet
        interactions_query = interactions_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", "data/interactions/") \
            .option("checkpointLocation", "checkpoints/interactions/") \
            .start()

        # Stream'leri sakla
        self.streams = {
            "posts": posts_query,
            "comments": comments_query,
            "interactions": interactions_query,
            "nationalist_posts": nationalist_posts_query,
            "nationalist_comments": nationalist_comments_query
        }

# ─────────────────────────────────────────────────────────────────────────────
# Main Application
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """Main application entry point."""
    try:
        # Load configuration
        config = load_config()
        setup_logging("INFO")  # Ensure INFO level logging
        
        # Initialize progress tracking
        progress = ProgressTracker()
        monitor = ProgressMonitor(progress, interval_seconds=30)  # More frequent updates
        
        # Start progress monitoring
        monitor.start()
        
        # Initialize Spark
        spark = (SparkSession.builder
                .appName(config.spark_app_name)
                .master(config.spark_master)
                .config("spark.sql.shuffle.partitions", str(config.spark_shuffle_partitions))
                .config("spark.jars.packages", ",".join([
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2",
                    "graphframes:graphframes:0.8.3-spark3.4-s_2.12"
                ]))
                .config("spark.sql.streaming.checkpointLocation", 
                       str(Path(config.checkpoint_dir) / "spark"))
                .getOrCreate())
        
        # Initialize Spark processor and get streams
        processor = SparkProcessor(config)
        
        # Start stream monitoring
        stream_monitor = StreamMonitor(processor.streams, interval_seconds=30)
        stream_monitor.start()
        
        # Start data collection
        logging.info("Starting data collection...")
        
        # First do backfill for both posts and comments
        backfill_submissions(config, progress)
        backfill_comments(config, progress)  # Add comment backfill
        
        # Then start streaming threads for real-time updates
        stop_event = threading.Event()
        submission_thread = threading.Thread(
            target=stream_submissions,
            args=(stop_event, config, progress),
            daemon=True
        )
        comment_thread = threading.Thread(
            target=stream_comments,
            args=(stop_event, config, progress),
            daemon=True
        )
        
        submission_thread.start()
        comment_thread.start()
        logging.info(" Reddit ingestion threads started.")
        
        try:
            # Wait for any stream to terminate
            spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logging.info(" Termination signal received, stopping threads...")
            stop_event.set()
            monitor.stop()
            stream_monitor.stop()
            submission_thread.join(timeout=5)
            comment_thread.join(timeout=5)
            logging.info(" All threads stopped gracefully.")
            
    except Exception as e:
        logging.error("Fatal error: %s", str(e), exc_info=True)
        raise

if __name__ == "__main__":
    main()
