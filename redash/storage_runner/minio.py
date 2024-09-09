import logging

from redash.utils import json_loads
from redash.storage_runner import BaseStorageRunner, register_storage

logger = logging.getLogger(__name__)

try:
    from minio import Minio

    enabled = True
except ImportError:
    enabled = False


class MinioRunner(BaseStorageRunner):
    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def name(cls):
        return "MinIO"

    @classmethod
    def type(cls):
        return "minio"

    def __init__(self, configuration):
        super().__init__(configuration)
        self.client = Minio(
            configuration["endpoint"],
            access_key=configuration["access_key"],
            secret_key=configuration["secret_key"],
            secure=configuration.get("secure", False),
            region=configuration.get("region", None),
        )

    def list_objects(self, bucket_name=None):
        """
        MinIO 버킷에서 파일 목록을 가져오는 메소드.
        """
        bucket_name = bucket_name or self.configuration.get("bucket")
        try:
            objects = self.client.list_objects(bucket_name)
            return [
                {
                    "name": obj.object_name,
                    "last_modified": obj.last_modified,
                    "size": obj.size,
                    "etag": obj.etag
                }
                for obj in objects
            ]
        except Exception as e:
            logger.error("Failed to list objects from bucket %s: %s", bucket_name, str(e))
            raise Exception(f"Failed to list objects from bucket {bucket_name}: {e}")

    def get_metadata(self, object_name):
        """
        MinIO에서 특정 파일의 메타데이터를 조회하는 메소드.
        """
        bucket_name = self.configuration.get("bucket")
        try:
            obj = self.client.stat_object(bucket_name, object_name)
            return {
                "name": obj.object_name,
                "size": obj.size,
                "etag": obj.etag,
                "content_type": obj.content_type,
                "last_modified": obj.last_modified,
            }
        except Exception as e:
            logger.error("Failed to retrieve metadata for object %s: %s", object_name, str(e))
            raise Exception(f"Failed to retrieve metadata for object {object_name}: {e}")

    def test_connection(self):
        """
        MinIO 연결 테스트를 수행하는 메소드.
        기본적으로 버킷에서 객체 목록을 조회해봄으로써 연결을 확인합니다.
        """
        try:
            self.list_objects()
            return True
        except Exception as e:
            logger.error("MinIO connection test failed: %s", str(e))
            raise Exception("MinIO connection test failed: " + str(e))

    def run_query(self, query, user):
        """
        주어진 쿼리에 따라 MinIO에서 객체 목록을 조회하는 메소드.
        """
        try:
            query_params = json_loads(query)
            bucket_name = query_params.get("bucket", self.configuration.get("bucket"))
            objects = self.list_objects(bucket_name)
            columns = [
                {"name": "object_name", "type": "string"},
                {"name": "last_modified", "type": "date"},
                {"name": "size", "type": "integer"},
                {"name": "etag", "type": "string"}
            ]
            rows = [{"object_name": obj["name"], "last_modified": obj["last_modified"], "size": obj["size"], "etag": obj["etag"]} for obj in objects]

            return {"columns": columns, "rows": rows}, None
        except Exception as e:
            logger.error("Failed to run query: %s", str(e))
            return None, str(e)

# MinioRunner 클래스를 query runner로 등록
register_storage(MinioRunner)


if __name__ == "__main__":
    try:
        import pandas as pd
    except ImportError:
        print("Please install pandas to run this example.")
        exit(1)

    # 실제 접속 정보를 입력합니다.
    config = {

    }

    # MinioRunner 인스턴스를 생성합니다.
    runner = MinioRunner(config)

    # 연결 테스트
    try:
        if runner.test_connection():
            print("MinIO connection successful!")
    except Exception as e:
        print(f"MinIO connection failed: {e}")

    # 객체 목록 조회 테스트
    try:
        objects = runner.list_objects()
        # DataFrame으로 변환
        df_objects = pd.DataFrame(objects)
        print("\nObjects in bucket:")
        print(df_objects)
    except Exception as e:
        print(f"Failed to list objects: {e}")

    # 특정 파일 메타데이터 조회 테스트
    try:
        metadata = runner.get_metadata("some_object_name")
        print("\nMetadata for some_object_name:")
        df_metadata = pd.DataFrame([metadata])
        print(df_metadata)
    except Exception as e:
        print(f"Failed to get metadata: {e}")

    # 쿼리 실행 테스트
    try:
        query = '{"bucket": "test"}'
        result, error = runner.run_query(query, user=None)
        if error:
            print(f"Query failed: {error}")
        else:
            # DataFrame으로 변환
            df_query_result = pd.DataFrame(result["rows"])
            print("\nQuery result:")
            print(df_query_result)
    except Exception as e:
        print(f"Query execution failed: {e}")
