from datetime import datetime, timedelta
from functools import partial
from random import choice
from unittest.mock import patch, MagicMock

from _pytest.fixtures import fixture
from genie_common.tools import EmailSender
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.components import ComponentFactory
from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.jobs_loader import JobsLoader
from data_collectors.logic.models import ScheduledJob
from main import lifespan
from tests.testing_utils import app_test_client_session, until, raise_exception


class TestScheduledJobFailure:
    @patch.object(EmailSender, "send")
    async def test_scheduled_job__job_fails__sends_email_notification(
        self, mock_send: MagicMock, scheduled_test_client: TestClient
    ):
        with scheduled_test_client:
            await until(lambda: mock_send.call_count == 1)

    @fixture
    async def failed_job(
        self, component_factory: ComponentFactory, db_engine: AsyncEngine
    ) -> ScheduledJob:
        jobs = await JobsLoader.load(component_factory)
        selected_job: ScheduledJob = choice(list(jobs.values()))
        selected_job.task = raise_exception
        selected_job.next_run_time = datetime.now() + timedelta(seconds=2)

        return selected_job

    @fixture
    def job_builder(self):
        return BaseJobBuilder()

    @fixture
    def scheduled_test_client(
        self,
        component_factory: ComponentFactory,
        failed_job: ScheduledJob,
    ) -> TestClient:
        lifespan_context = partial(
            lifespan,
            component_factory=component_factory,
            jobs={failed_job.id: failed_job},
        )

        with app_test_client_session(lifespan_context) as client:
            yield client
