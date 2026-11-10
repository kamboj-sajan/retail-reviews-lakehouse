"""Record what each job run did, as data, so reported numbers come from runs.

Each run writes one JSON record (job name, start time, duration, row counts)
to <metrics_path>/job_name=<name>/run_id=<start time>/. Locally that is a folder under data/local; on
Glue it is the artifacts bucket, where Athena can query every run's record.

Row counts are collected with Spark's Observation API. It computes the counts
while the data flows through the write, instead of running a second pass over
the data just to count it. For a 6 GB gzip file that cannot be split, a
second pass would mean decompressing the whole file again.
"""
import datetime
import json
import time

from pyspark.sql import Observation


def path_bytes(spark, path):
    """Total size in bytes of every file under path, local or on S3.

    path may contain a glob such as category=*/review_month=2023-01. Uses
    Hadoop's FileSystem API inside Spark's JVM, so the same call works for a
    local folder and an s3:// prefix without needing boto3.
    """
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    matches = fs.globStatus(hadoop_path) or []
    return int(sum(fs.getContentSummary(m.getPath()).getLength() for m in matches))


def path_count(spark, pattern):
    """Number of files or folders matching a glob pattern, local or on S3."""
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return len(fs.globStatus(hadoop_path) or [])


class JobRun:
    """Times a job and collects named counts; .save() writes them out."""

    def __init__(self, spark, job_name, metrics_path):
        self.spark = spark
        self.job_name = job_name
        self.metrics_path = metrics_path
        self.started = datetime.datetime.now(datetime.timezone.utc)
        self._clock = time.monotonic()
        self._observations = []
        self.values = {}

    def observe(self, df, **aggregates):
        """Attach counts to df; they are filled in when df is next written."""
        observation = Observation(f"{self.job_name}_{len(self._observations)}")
        self._observations.append(observation)
        return df.observe(observation, *[expr.alias(name) for name, expr in aggregates.items()])

    def record(self, **values):
        self.values.update(values)

    def observed(self):
        """All observed counts so far. Call after the write that computes them."""
        for observation in self._observations:
            self.values.update(observation.get)  # blocks until that write has run
        return dict(self.values)

    def save(self):
        self.observed()
        record = {
            "job": self.job_name,
            "started_utc": self.started.isoformat(timespec="seconds"),
            "seconds": round(time.monotonic() - self._clock, 1),
            **{k: (int(v) if isinstance(v, (int, float)) and float(v).is_integer() else v) for k, v in self.values.items()},
        }
        print(f"{self.job_name} metrics: {json.dumps(record)}")
        if self.metrics_path:
            stamp = self.started.strftime("%Y%m%dT%H%M%SZ")
            # Folder names differ from the field names inside the record
            # (job_name vs job): Spark refuses a folder partition whose name
            # repeats a column in the data.
            (self.spark.createDataFrame([record]).coalesce(1).write.mode("overwrite")
             .json(f"{self.metrics_path}/job_name={self.job_name}/run_id={stamp}"))
        return record
