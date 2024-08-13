import os

os.environ['SPARK_VERSION'] = '3.3'

from pyspark.sql import SparkSession, Row
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
import pydeequ

spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())

df = spark.sparkContext.parallelize([
            Row(a="foo", b=1, c=5),
            Row(a="bar", b=2, c=6),
            Row(a="baz", b=3, c=None)]).toDF()

check = Check(spark, CheckLevel.Error, "Review Check")
check2 = Check(spark, CheckLevel.Warning, "Review Check 2")

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check\
            .hasSize(lambda x: x >= 3) \
            .hasMin("b", lambda x: x == 0) \
            .isComplete("c")  \
            .isUnique("a")  \
            .isContainedIn("a", ["foo", "bar", "baz"]) \
            .isNonNegative("b")
    ) \
    .addCheck(
        check2\
            .hasSize(lambda x: x >= 3) \
            .hasMin("b", lambda x: x == 0) \
            .isComplete("c")  \
            .isUnique("a")  \
            .isContainedIn("a", ["foo", "bar", "baz"]) \
            .isNonNegative("b")
    ) \
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()

spark.sparkContext._gateway.shutdown_callback_server()
spark.stop()