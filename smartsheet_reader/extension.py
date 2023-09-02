import os
import logging

import pandas as pd
import knime.extension as knext
import smartsheet
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

    def configure(self, configure_context: knext.ConfigurationContext):
        if not self.access_token:
            raise knext.InvalidParametersError('SMARTSHEET_ACCESS_TOKEN is not set in your env')
        return None

    def execute(self, exec_context: knext.ExecutionContext):

        smart = smartsheet.Smartsheet()
        sheet = smart.Sheets.get_sheet(self.sheetId)

        df = pd.DataFrame([[c.value for c in r.cells] for r in sheet.rows])
        df.columns = [c.title for c in sheet.columns]

        return knext.Table.from_pandas(df)
