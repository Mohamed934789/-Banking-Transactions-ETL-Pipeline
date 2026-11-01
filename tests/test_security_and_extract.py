import pytest
from spark.warehouse_config import snowflake_options
from scripts.webhdfs_extract import extract

def test_missing_credentials_fail_without_values():
    with pytest.raises(ValueError, match="SNOWFLAKE_PASSWORD") as e:
        snowflake_options({"SNOWFLAKE_ACCOUNT": "test", "SNOWFLAKE_USER": "tester"})
    assert "tester" not in str(e.value)
    config = snowflake_options({"SNOWFLAKE_ACCOUNT": "test", "SNOWFLAKE_USER": "tester", "SNOWFLAKE_PASSWORD": "offline"})
    assert config["role"] == "BANKING_ETL"

def test_failed_upload_never_moves_source(tmp_path):
    landing=tmp_path/"landing"; landing.mkdir(); processed=tmp_path/"processed"
    path=landing/"batch.csv"; path.write_text("TransactionID,AccountID,TransactionAmount,TransactionDate\n1,a,-10,2026-01-01\n")
    class Response:
        status_code=307; headers={"Location": "http://local/upload"}
        def __init__(self, fail=False, status=307):self.fail=fail;self.status_code=status
        def raise_for_status(self):
            if self.fail:raise RuntimeError("upload failed")
    class Session:
        def __init__(self,fail):self.count=0;self.fail=fail
        def put(self,*args,**kwargs):
            self.count+=1;return Response(self.fail and self.count==3, status=201 if self.count==3 else 307)
    with pytest.raises(RuntimeError):extract(landing,processed,Session(True))
    assert path.exists() and not (processed/path.name).exists()
    assert extract(landing,processed,Session(False)) == 1
    assert not path.exists() and (processed/path.name).exists()
