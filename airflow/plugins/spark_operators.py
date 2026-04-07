"""
Custom Spark Operators for Airflow

This module provides custom operators for submitting Spark jobs to the Spark cluster.
"""

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import subprocess
import logging

logger = logging.getLogger(__name__)


class SparkSubmitJobOperator(BaseOperator):
    """
    Custom operator to submit Spark jobs to the Spark cluster.

    This operator executes spark-submit commands to run Spark applications
    for data processing tasks in the e-commerce data warehouse pipeline.

    :param spark_script: Path to the Python script to execute
    :type spark_script: str
    :param spark_master: Spark master URL (default: spark://spark-master:7077)
    :type spark_master: str
    :param conf: Spark configuration properties (optional)
    :type conf: dict
    :param jars: Additional JARs to include (optional)
    :type jars: list
    :param py_files: Additional Python files to include (optional)
    :type py_files: list
    :param driver_memory: Driver memory setting (default: 2g)
    :type driver_memory: str
    :param executor_memory: Executor memory setting (default: 4g)
    :type executor_memory: str
    :param num_executors: Number of executors (optional)
    :type num_executors: int
    :param app_name: Application name (optional, defaults to script name)
    :type app_name: str
    """

    template_fields = ("spark_script", "app_name")
    template_ext = (".py",)
    ui_color = "#FFFFFF"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        spark_script,
        spark_master="spark://spark-master:7077",
        conf=None,
        jars=None,
        py_files=None,
        driver_memory="2g",
        executor_memory="4g",
        num_executors=None,
        app_name=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.spark_script = spark_script
        self.spark_master = spark_master
        self.conf = conf or {}
        self.jars = jars or []
        self.py_files = py_files or []
        self.driver_memory = driver_memory
        self.executor_memory = executor_memory
        self.num_executors = num_executors
        self.app_name = app_name or spark_script.split("/")[-1].replace(".py", "")

    def execute(self, context):
        """
        Execute the Spark submit command.

        :param context: Airflow context
        :return: Command output
        """
        # Build spark-submit command
        cmd = ["spark-submit"]

        # Add master
        cmd.extend(["--master", self.spark_master])

        # Add deploy mode
        cmd.extend(["--deploy-mode", "cluster"])

        # Add application name
        cmd.extend(["--name", self.app_name])

        # Add driver memory
        cmd.extend(["--driver-memory", self.driver_memory])

        # Add executor memory
        cmd.extend(["--executor-memory", self.executor_memory])

        # Add number of executors if specified
        if self.num_executors:
            cmd.extend(["--num-executors", str(self.num_executors)])

        # Add configuration properties
        for key, value in self.conf.items():
            cmd.extend(["--conf", f"{key}={value}"])

        # Add JARs
        for jar in self.jars:
            cmd.extend(["--jars", jar])

        # Add Python files
        for py_file in self.py_files:
            cmd.extend(["--py-files", py_file])

        # Add the main script
        cmd.append(self.spark_script)

        # Log the command
        logger.info(f"Executing Spark job: {' '.join(cmd)}")

        try:
            # Execute the command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=7200,  # 2 hour timeout
            )

            # Log output
            logger.info(f"Spark job output: {result.stdout}")

            return result.stdout

        except subprocess.CalledProcessError as e:
            error_msg = (
                f"Spark job failed with exit code {e.returncode}. Error: {e.stderr}"
            )
            logger.error(error_msg)
            raise AirflowException(error_msg)

        except subprocess.TimeoutExpired:
            error_msg = f"Spark job timed out after 2 hours"
            logger.error(error_msg)
            raise AirflowException(error_msg)
