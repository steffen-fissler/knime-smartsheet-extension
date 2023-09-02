import os
import logging

import pandas as pd
import knime.extension as knext
import smartsheet
from collections.abc import Callable

LOGGER = logging.getLogger(__name__)


@knext.node(name='Smartsheet Writer', node_type=knext.NodeType.SINK, icon_path='icon.png', category='/io/write')
@knext.input_table(name='Input Data', description='Data source')
class SmartsheetReaderNode(knext.PythonNode):
    """Smartsheet Writer Node
    Writes Smartsheet sheet
    """
    sheetId = knext.StringParameter(
        label='Sheet', description='The Smartsheet sheet to be written', default_value='5911621122975620')

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

    def configure(self, configure_context: knext.ConfigurationContext, _input):
        if not self.access_token:
            raise knext.InvalidParametersError('SMARTSHEET_ACCESS_TOKEN is not set in your env')
        return None

    def execute(self, exec_context: knext.ExecutionContext, _input):
        input_pandas = input.to_pandas()

        smart = smartsheet.Smartsheet()
        sheet = smart.Sheets.get_sheet(self.sheetId)

        return None
