from datetime import timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import partial
from typing import List, Dict, Tuple

from data_collectors.logic.models import SummarySection
from genie_common.tools import AioPoolExecutor, EmailSender, logger

from data_collectors.contract import IManager, IStatusCollector


class StatusReporterManager(IManager):
    def __init__(
        self,
        collectors: List[IStatusCollector],
        pool_executor: AioPoolExecutor,
        email_sender: EmailSender,
        recipients: List[str],
    ):
        self._collectors = collectors
        self._pool_executor = pool_executor
        self._email_sender = email_sender
        self._recipients = recipients

    async def run(self, lookback_period: timedelta):
        logger.info("Starting to generate status report")
        status_report_pairs = await self._pool_executor.run(
            iterable=self._collectors,
            func=partial(self._run_single_collector, lookback_period),
            expected_type=tuple,
        )
        reports = dict(status_report_pairs)
        report = self._merge_status_reports(reports)
        message = MIMEMultipart()
        message.attach(MIMEText(report, "html"))
        logger.info("Mailing final status report")

        self._email_sender.send_multipart(recipients=self._recipients, subject="Genie Status Report", message=message)

    @staticmethod
    async def _run_single_collector(
        lookback_period: timedelta, collector: IStatusCollector
    ) -> Tuple[str, List[SummarySection]]:
        logger.info(f"Running `{collector.name}` collector")
        status = await collector.collect(lookback_period)
        return collector.name, status

    @staticmethod
    def _merge_status_reports(reports: Dict[str, List[SummarySection]]) -> str:
        report_elements = []

        for name, sections in reports.items():
            element_summary = f"<div><h2>{name}</h2>"

            for section in sections:
                element_summary += section.to_html()

            report_elements.append(f"{element_summary}</div>")

        return "<br>".join(report_elements)
