import os
import logging

import pandas as pd
import knime.extension as knext
import datetime
import smartsheet
from smartsheet.models.enums.column_type import ColumnType as SSColumnType
from collections.abc import Callable

LOGGER = logging.getLogger(__name__)


@knext.node(name='Smartsheet Reader', node_type=knext.NodeType.SOURCE, icon_path='icon.png', category='/io/read')
@knext.output_table(name='Output Data', description='Data from Smartsheet')
class SmartsheetReaderNode(knext.PythonNode):
    """Smartsheet Reader Node
    Reads Smartsheet sheet
    """
    sheetId = knext.StringParameter(
        label='Sheet', description='The Smartsheet sheet to be read', default_value='5911621122975620')

    def __init__(self):
        self.access_token = os.environ.get('SMARTSHEET_ACCESS_TOKEN', '')

        column_filter: Callable[[knext.Column], bool] = None

        column = knext.ColumnParameter(
            label='Column',
            description=None,
            port_index=0,  # the port from which to source the input table
            column_filter=column_filter,  # a (lambda) function to filter columns
            include_row_key=False,  # whether to include the table Row ID column in the list of selectable columns
            include_none_column=False,  # whether to enable None as a selectable option, which returns "<none>"
            since_version=None,
        )

    @classmethod
    def smartsheet_column_type_to_knime(cls, ss_type: SSColumnType) -> knext.KnimeType:
        if ss_type == SSColumnType.TEXT_NUMBER:
            return knext.string()
        if ss_type == SSColumnType.DATE:
            return datetime.date
        if ss_type == SSColumnType.DATETIME:
            return datetime.datetime
        return knext.string()

    def configure(self, configure_context: knext.ConfigurationContext, *_inputs):
        if not self.access_token:
            configure_context.set_warning('SMARTSHEET_ACCESS_TOKEN is not set in your env')

        smart = smartsheet.Smartsheet()
        sheet = smart.Sheets.get_sheet(self.sheetId)

        schema = knext.Schema(ktypes=[], names=[])
        for c in sheet.columns:
            schema = schema.append(knext.Column(self.smartsheet_column_type_to_knime(c.type), c.title))

        return schema

    def execute(self, exec_context, *_inputs):
        if not self.access_token:
            exec_context.set_warning('SMARTSHEET_ACCESS_TOKEN is not set in your env')

        smart = smartsheet.Smartsheet()
        sheet = smart.Sheets.get_sheet(self.sheetId)

        return knext.Table.from_pandas(pd.DataFrame([[c.value for c in r.cells] for r in sheet.rows]))
