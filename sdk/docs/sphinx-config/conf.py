# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import sys
from sphinx.application import Sphinx

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'exts')))

from pygments_daml_lexer import DamlLexer
from ref import IgnoreRefRole

extensions = [
    'sphinx.ext.todo'
]

html_theme = 'default'

def setup(sphinx):
    sphinx.add_lexer('daml', DamlLexer)

    sphinx.add_role('externalref', IgnoreRefRole())
    sphinx.add_role('brokenref', IgnoreRefRole())
