# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from docutils import nodes
from docutils.parsers.rst import Directive

# Work-In-Progress directive for Sphinx documentation
class WipDirective(Directive):

    has_content = True

    def run(self):
        para_node = nodes.paragraph(text="This page is a work in progress. It may contain incomplete or incorrect information.")
        important_node = nodes.note()
        important_node += para_node
        return [important_node]

def setup(app):
    app.add_directive('wip', WipDirective)
