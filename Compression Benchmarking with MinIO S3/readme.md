# 🚀 Apache Spark Compression Benchmarking with MinIO S3

> **A personal curiosity project benchmarking how different compression codecs perform in Apache Spark when writing to S3-compatible storage (MinIO).**

## 📌 Motivation

While working with Apache Spark, I noticed most tutorials and guides default to using Snappy for Parquet compression. But Spark also supports other codecs like GZIP, LZ4, ZSTD, and Brotli — each with its own strengths in speed, compression ratio, and compatibility. This project benchmarks these options in a controlled environment.

## ⚙️ Environment & Setup

- **Apache Spark Cluster**: Deployed using Illinois Tech's infrastructure
- **Storage**: MinIO (S3-compatible)
- **Driver Memory**: 4 GB  
- **Executor Memory**: 4 GB  
- **Cores per Executor**: 1  
- **Total Cores**: 12  
- **Platform**: Jupyter Notebook + Spark Web UI

### 🛠️ Technologies Used

- Apache Spark (with PySpark)
- Python
- MinIO (S3-compatible)
- Brotli Hadoop Codec (via JitPack)

## 📂 Dataset

The dataset used was a fixed-width `.txt` weather file stored in MinIO (`s3a://itmd521/50.txt`). It was parsed using PySpark's string manipulation functions to create a structured DataFrame.

```python
df = spark.read.csv('s3a://itmd521/50.txt')
# String manipulation follows...
