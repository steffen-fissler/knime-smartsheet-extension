import os
import logging

import pandas as pd
import knime.extension as knext

LOGGER = logging.getLogger(__name__)


@knext.node(name='Smartsheet Reader', node_type=knext.NodeType.SOURCE, icon_path='icon.png', category='/io/read')
@knext.output_table(name='Output Data', description='Data from Smartsheet')
class SmartsheetReaderNode:
    """Smartsheet Reader Node
    Reads Smartsheet sheet
    """
    sheetId = knext.StringParameter('Sheet', 'The Smartsheet sheet to be read', '')

    def __init__(self):
        self.access_token = os.environ.get('SMARTSHEET_ACCESS_TOKEN', '')

    def configure(self, configure_context: knext.ConfigurationContext):
        if not self.access_token:
            configure_context.set_warning('SMARTSHEET_ACCESS_TOKEN is not set in your env')
        schema = knext.Schema(ktypes=[], names=[])
        schema = schema.append(knext.Column(knext.int32(), 'A'))
        schema = schema.append(knext.Column(knext.int32(), 'B'))
        return schema

    def execute(self, exec_context):
        if not self.access_token:
            exec_context.set_warning('SMARTSHEET_ACCESS_TOKEN is not set in your env')

        return knext.Table.from_pandas(pd.DataFrame([[0, 1], [2, 3]]))
