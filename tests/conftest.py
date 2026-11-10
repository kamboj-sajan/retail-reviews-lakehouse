"""Shared test setup: import paths, Java 17, and one SparkSession for the whole run."""
import os
import subprocess
import sys

import pytest

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
for folder in ("scripts", "src", "jobs"):
    sys.path.insert(0, os.path.join(ROOT, folder))


def _find_java_17():
    """Spark 4 needs Java 17. Prefer one already registered with macOS, then Homebrew's."""
    try:
        # -F: fail when there is no Java 17, instead of quietly returning another version.
        return subprocess.run(["/usr/libexec/java_home", "-F", "-v", "17"], capture_output=True, text=True,
                              check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    brew = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    return brew if os.path.isdir(brew) else None


if "JAVA_HOME" not in os.environ and _find_java_17():
    os.environ["JAVA_HOME"] = _find_java_17()


@pytest.fixture(scope="session")
def spark():
    """A small local Spark, started once (starting a JVM takes a few seconds).

    The local-only settings are applied first. build_spark() then adds the
    project's own settings on top, so the tests exercise the same session
    configuration that the real jobs use.
    """
    from pyspark.sql import SparkSession

    from lakehouse.session import build_spark

    (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        # The default of 200 shuffle partitions is sized for a cluster; with a
        # handful of test rows it only adds seconds of scheduling overhead.
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    session = build_spark("tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
