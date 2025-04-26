from copy import deepcopy
from dataclasses import dataclass
from typing import List

from numpy.distutils.conv_template import header
from pandas import DataFrame


@dataclass
class SummarySection:
    title: str
    data: DataFrame

    def to_html(self) -> str:
        formatted_title = f"<h3>{self.title}</h3>"
        formatted_table = self._convert_data_to_html_table()

        return f"<div>{formatted_title}{formatted_table}</div>"

    def _convert_data_to_html_table(self) -> str:
        html = "<table style='border-collapse: collapse; width: 100%; max-width: 600px;'>"
        html += self._build_table_headers()
        html += self._build_table_body()

        return f"{html}</table>"

    def _build_table_headers(self) -> str:
        headers = "<thead><tr>"

        for col in self.data.columns:
            headers += self._inline_styles("th", is_header=True) + str(col) + "</th>"

        return f"{headers}</tr></thead>"

    def _build_table_body(self) -> str:
        body = "<tbody>"

        for _, row in self.data.iterrows():
            body += "<tr>"
            for val in row:
                body += self._inline_styles("td") + str(val) + "</td>"
            body += "</tr>"

        return f"{body}</tbody>"

    @staticmethod
    def _inline_styles(row_tag, is_header=False) -> str:
        base_style = "border: 1px solid #e0e0e0; padding: 12px; text-align: left; font-family: Arial, sans-serif; font-size: 14px;"
        header_style = (
            "background-color: #f5f7fa; font-weight: bold; color: #333333;"
            if is_header
            else "background-color: #ffffff; color: #4a4a4a;"
        )

        return f"<{row_tag} style='{base_style} {header_style}'>"
