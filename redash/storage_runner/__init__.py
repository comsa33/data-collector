import logging
from abc import ABC, abstractmethod

from redash.query_runner import BaseQueryRunner
from redash.utils import json_loads

logger = logging.getLogger(__name__)

# 기본적으로 지원하는 데이터 유형
TYPE_STRING = "string"
TYPE_DATE = "date"
TYPE_INTEGER = "integer"

SUPPORTED_STORAGE_COLUMN_TYPES = {TYPE_STRING, TYPE_DATE, TYPE_INTEGER}


class BaseStorageRunner(BaseQueryRunner, ABC):
    """
    BaseStorageRunner는 오브젝트 스토리지와 관련된 기본 동작을 제공하는 클래스입니다.
    S3, MinIO, Azure Blob 등의 스토리지와 연동하기 위한 부모 클래스로 사용됩니다.
    """

    def __init__(self, configuration):
        super().__init__(configuration)

    @abstractmethod
    def list_objects(self, bucket_name=None):
        """
        스토리지에서 파일 목록을 조회하는 메소드. 각 스토리지에 맞게 구현해야 합니다.
        """
        pass

    @abstractmethod
    def get_metadata(self, object_name):
        """
        스토리지에서 특정 파일의 메타데이터를 조회하는 메소드. 각 스토리지에 맞게 구현해야 합니다.
        """
        pass

    @classmethod
    def configuration_schema(cls):
        """
        스토리지 연결을 위한 설정 스키마를 정의합니다. S3, MinIO, Azure Blob 등 다양한 스토리지에서
        사용할 공통 필드를 정의합니다.
        """
        return {
            "type": "object",
            "properties": {
                "endpoint": {"type": "string", "title": "Endpoint URL"},
                "access_key": {"type": "string", "title": "Access Key"},
                "secret_key": {"type": "string", "title": "Secret Key"},
                "bucket": {"type": "string", "title": "Bucket Name"},
                "region": {"type": "string", "title": "Region", "default": "us-east-1"},
                "secure": {"type": "boolean", "title": "Use SSL?", "default": False},
            },
            "secret": ["secret_key"],
            "order": ["endpoint", "access_key", "secret_key", "bucket", "region", "secure"],
            "required": ["endpoint", "access_key", "secret_key", "bucket"],
        }

    def test_connection(self):
        """
        스토리지와의 연결을 테스트하는 메소드. 각 스토리지에 맞게 list_objects 메소드를 통해 구현됩니다.
        """
        try:
            self.list_objects()
            return True
        except Exception as e:
            logger.exception("Failed to connect to the storage: %s", e)
            raise Exception(f"Failed to connect to the storage: {e}")

    def run_query(self, query, user):
        """
        쿼리 실행 메소드: 기본적으로 query는 버킷에서 객체 목록을 가져오는 명령이라고 가정합니다.
        """
        try:
            query_params = json_loads(query)
            bucket_name = query_params.get("bucket", self.configuration.get("bucket"))
            objects = self.list_objects(bucket_name)
            columns = [{"name": "object_name", "type": TYPE_STRING}, {"name": "last_modified", "type": TYPE_DATE}]
            rows = [{"object_name": obj["name"], "last_modified": obj["last_modified"]} for obj in objects]

            return {"columns": columns, "rows": rows}, None
        except Exception as e:
            return None, str(e)


storage_runners = {}


def register_storage(storage_runner_class):
    global storage_runners
    if storage_runner_class.enabled():
        storage_runners[storage_runner_class.type()] = storage_runner_class
        logger.debug(
            "Registering %s (%s) storage runner.",
            storage_runner_class.name(),
            storage_runner_class.type(),
        )
    else:
        logger.debug(
            "%s storage runner enabled but not supported, not registering.",
            storage_runner_class.name(),
        )
