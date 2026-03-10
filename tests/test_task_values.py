from unittest.mock import MagicMock, patch

from dbtopo.task_values import set_task_value


def test_set_task_value_calls_dbutils():
    spark = MagicMock()
    mock_dbutils_cls = MagicMock()
    mock_dbutils = mock_dbutils_cls.return_value

    with patch.dict(
        "sys.modules",
        {"pyspark.dbutils": MagicMock(DBUtils=mock_dbutils_cls)},
    ):
        set_task_value(spark, "my_key", 42)

    mock_dbutils_cls.assert_called_once_with(spark)
    mock_dbutils.jobs.taskValues.set.assert_called_once_with(key="my_key", value=42)


def test_set_task_value_noop_when_dbutils_unavailable():
    spark = MagicMock()
    # pyspark.dbutils may not exist locally — set_task_value should not raise
    set_task_value(spark, "key", "value")
