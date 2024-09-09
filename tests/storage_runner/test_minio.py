from unittest import TestCase

import mock

from redash.storage_runner.minio import MinioRunner


class TestMinioRunner(TestCase):
    @mock.patch("redash.storage_runner.minio.Minio")
    def setUp(self, MockMinio):
        # Mock Minio client
        self.mock_client = mock.Mock()
        MockMinio.return_value = self.mock_client
        
        # MinioRunner 인스턴스 생성
        self.config = {
            "endpoint": "mock.endpoint",
            "access_key": "mock_access_key",
            "secret_key": "mock_secret_key",
            "bucket": "mock_bucket",
            "region": "mock_region",
            "secure": False,
        }
        self.runner = MinioRunner(self.config)

    def test_list_objects(self):
        # Minio에서 반환할 mock 객체 목록 설정
        mock_objects = [
            mock.Mock(object_name="file1.txt", last_modified="2023-09-09", size=123, etag="etag1"),
            mock.Mock(object_name="file2.txt", last_modified="2023-09-10", size=456, etag="etag2"),
        ]
        self.mock_client.list_objects.return_value = mock_objects

        # list_objects 메소드 호출
        result = self.runner.list_objects()

        # 예상되는 결과
        expected = [
            {"name": "file1.txt", "last_modified": "2023-09-09", "size": 123, "etag": "etag1"},
            {"name": "file2.txt", "last_modified": "2023-09-10", "size": 456, "etag": "etag2"},
        ]

        self.assertEqual(result, expected)
        self.mock_client.list_objects.assert_called_once_with("test")

    def test_get_metadata(self):
        # Minio에서 반환할 mock 메타데이터 설정
        mock_object = mock.Mock(
            object_name="file1.txt",
            size=123,
            etag="etag1",
            content_type="text/plain",
            last_modified="2023-09-09"
        )
        self.mock_client.stat_object.return_value = mock_object

        # get_metadata 메소드 호출
        result = self.runner.get_metadata("file1.txt")

        # 예상되는 결과
        expected = {
            "name": "file1.txt",
            "size": 123,
            "etag": "etag1",
            "content_type": "text/plain",
            "last_modified": "2023-09-09",
        }

        self.assertEqual(result, expected)
        self.mock_client.stat_object.assert_called_once_with("test", "file1.txt")

    def test_run_query(self):
        # Minio에서 반환할 mock 객체 목록 설정
        mock_objects = [
            mock.Mock(object_name="file1.txt", last_modified="2023-09-09", size=123, etag="etag1"),
        ]
        self.mock_client.list_objects.return_value = mock_objects

        # 쿼리 실행
        query = '{"bucket": "test"}'
        result, error = self.runner.run_query(query, user=None)

        # 예상되는 결과
        expected = {
            "columns": [
                {"name": "object_name", "type": "string"},
                {"name": "last_modified", "type": "date"},
                {"name": "size", "type": "integer"},
                {"name": "etag", "type": "string"},
            ],
            "rows": [
                {"object_name": "file1.txt", "last_modified": "2023-09-09", "size": 123, "etag": "etag1"},
            ],
        }

        self.assertEqual(result, expected)
        self.assertIsNone(error)
        self.mock_client.list_objects.assert_called_once_with("test")

    def test_test_connection_success(self):
        # list_objects 호출 성공 시 테스트
        self.mock_client.list_objects.return_value = []
        self.assertTrue(self.runner.test_connection())
        self.mock_client.list_objects.assert_called_once_with("test")

    def test_test_connection_failure(self):
        # list_objects 호출 실패 시 테스트
        self.mock_client.list_objects.side_effect = Exception("Failed to connect")
        with self.assertRaises(Exception) as context:
            self.runner.test_connection()

        self.assertTrue("Failed to connect" in str(context.exception))
