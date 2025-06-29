# 🇹🇷 Influence and Interaction in r/Turkey Reddit Community

End-to-end PySpark pipeline to analyze user interactions and nationalist sentiment in the r/Turkey subreddit.

## 📌 Features

- 🔄 Real-time data ingestion via Reddit API + Kafka
- 🌐 User-user interaction graph & community detection
- 💬 Turkish nationalist content detection using regex
- 🧠 Spark Structured Streaming + GraphFrames
- ✅ All outputs saved locally as `.parquet` (not public)

## ⚠️ Ethics & Privacy

- This project uses only publicly available Reddit content.
- No data is stored or shared publicly.
- All author identifiers and content are processed locally and **not published or redistributed**.
- `.env`, `data/`, and `checkpoints/` are excluded from this repository.

## 🔐 License and Usage

> **This project is NOT open source. All rights reserved.**

You may not copy, modify, reuse, or redistribute any part of this codebase or its documentation  
**without the explicit written permission of the author.**

## 🚀 Usage

1. Add your credentials to a `.env` file (see `.env.example`)
2. Run:
```bash
python main_clean.py
```

3. Logs are stored in `reddit_analysis.log`
4. Results go to local `./data/` and `./checkpoints/`

## ⚙️ Requirements

- Python 3.9+
- Apache Kafka
- Spark 3.4.2
- `graphframes`, `praw`, `confluent_kafka`, `dotenv`
