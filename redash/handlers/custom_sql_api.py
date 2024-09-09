import logging
import traceback
from time import sleep
from typing import Tuple, Dict, Any

from rq import Connection
from flask import request, jsonify
from flask_restful import Resource
from sqlalchemy.exc import ProgrammingError

from redash.security import csrf
from redash.serializers import serialize_job
from redash import models, rq_redis_connection
from redash.tasks.queries.execution import enqueue_query

logger = logging.getLogger(__name__)


class PublicSQLExecutionResource(Resource):
    """
    사용자가 제공한 쿼리를 실행하고 결과를 반환하는 API 엔드포인트
    """
    # 오류 메시지 정의
    error_messages = {
        "missing_fields": "Request must include 'query' and 'db_name' fields.",
        "missing_query": "Request must include 'query' fields.",
        "missing_db_name": "Request must include 'db_name' fields.",
        "database_not_found": "Database name not found.",
        "query_timeout": "Query execution timeout.",
        "query_execution_failed": "Query execution failed.",
        "sql_syntax_error": "SQL syntax error.",
        "internal_server_error": "Internal server error occurred. Please try again later.",
    }

    def error_response(self, message: str, http_status: int) -> Tuple[Dict[str, Any], int]:
        """오류 응답 생성하는 메서드
        :param message: 오류 메시지
        :param http_status: HTTP 상태 코드
        :return: 오류 응답
        """
        return jsonify({"status": "fail", "message": message}), http_status

    @csrf.exempt    # CSRF 보호 비활성화
    def post(self):
        """POST 요청을 처리하고 쿼리를 실행하여 결과를 반환
        :request body: JSON 형식의 요청 데이터
            - query: 실행할 쿼리 텍스트 (필수)
            - db_name: 데이터베이스 이름 (필수) - redash에 등록된 데이터 소스 이름
        :return: 쿼리 실행 결과
            - status: 성공 또는 실패
            - result: 쿼리 실행 결과 (성공 시)
            - message: 오류 메시지 (실패 시)
        """
        try:
            query_data = request.get_json()     # 요청 데이터 가져오기
            # 요청에 'query' 및 'db_name' 필드가 포함되어 있는지 확인
            if not query_data:
                return self.error_response(self.error_messages["missing_fields"], 400)
            if "query" not in query_data:
                return self.error_response(self.error_messages["missing_query"], 400)
            if "db_name" not in query_data:
                return self.error_response(self.error_messages["missing_db_name"], 400)
            query_text = query_data.get("query")
            db_name = query_data.get("db_name")

            # 데이터베이스 이름으로 데이터 소스 모델 검색
            data_source = models.DataSource.query.filter(
                models.DataSource.name == db_name,
            ).first()   # 데이터 소스 모델 검색

            # 데이터 소스가 없는 경우(데이터베이스 이름이 잘못된 경우)
            if not data_source:
                return self.error_response(self.error_messages["database_not_found"], 404)

            # 쿼리 실행 작업을 대기열에 추가
            with Connection(rq_redis_connection):
                job = enqueue_query(
                    query=query_text,           # 쿼리 텍스트
                    data_source=data_source,    # 데이터 소스 모델
                    user_id=None,               # 사용자 ID는 없어도 됨
                    is_api_key=False,           # API 키 사용 여부는 False
                    scheduled_query=None,       # scheduled_query는 해당되지 않음
                )

            # 작업이 완료될 때까지 대기 (최대 10회)
            try_count = 0
            while job.result is None and try_count < 10:
                try_count += 1
                sleep(1)
                job.refresh()
            logger.info("Job: %s", serialize_job(job))

            # 작업이 완료되지 않은 경우
            if job.result is None:
                return self.error_response(self.error_messages["query_timeout"], 408)

            # 쿼리 결과를 반환 (QueryResult 모델에서 데이터 필드)
            query_result_id = job.result
            query_result = models.QueryResult.query.get(query_result_id)
            if query_result:
                return jsonify({"status": "success", "result": query_result.data["rows"]}), 200
            else:
                return self.error_response(self.error_messages["query_execution_failed"], 500)

        # SQL 구문 오류
        except ProgrammingError as e:
            logger.error("SQL syntax error: %s\n%s", e, traceback.format_exc())
            return self.error_response(self.error_messages["sql_syntax_error"], 400)
        # 기타 오류
        except Exception as e:
            logger.error("Internal server error: %s\n%s", e, traceback.format_exc())
            return self.error_response(self.error_messages["internal_server_error"], 500)
