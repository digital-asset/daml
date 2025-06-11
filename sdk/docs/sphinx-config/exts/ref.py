# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from docutils import nodes
from sphinx.util.docutils import SphinxRole

class IgnoreRefRole(SphinxRole):
    def run(self):
        node = nodes.literal(text=self.rawtext)
        return [node], []
