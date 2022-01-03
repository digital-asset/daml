# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# A (hacky) extension to support automatically named references to
# definition and enumeration list items.
#
# Docutils (and hence also Sphinx) has a funny way of marking list
# items as targets for labels. There does not appear to be a way to
# assign a label to the first item of a list directly. For later
# items, the label must be indented as part of the preceding item.

# Thus, we support named labels to a definition/enum list item by:
#
# 1. using the label of its parent definition list (if one exists) for
# the first item in a list
#
# 2. using the actual label (if it exists) for all other items
#
# For definition lists, the terms are used as names. For enumeration
# lists, a hierarchical numbering scheme is used (not present in Sphinx,
# so hopefully it works sufficiently well).
#
# To debug, the easiest way I know is:
#
# 1. raise an exception at the place where you want to insert a break point
#
#. 2. call sphinx-build with the -P argument


from docutils import nodes
from sphinx.util.nodes import clean_astext

try:
    from docutils.utils.roman import toRoman
except ImportError:
    # In Debain/Ubuntu, roman package is provided as roman, not as docutils.utils.roman
    from roman import toRoman

def make_dlist_items_named(app, document):
    labels = app.env.domaindata['std']['labels']
    anonlabels = app.env.domaindata['std']['anonlabels']
    for node in document.traverse(nodes.definition_list_item):
        if node == node.parent.children[0]:
            node_labels = node.parent['ids']
        else:
            node_labels = node['ids']
        if node_labels:
            docname = app.env.docname
            term = clean_astext(node.children[0])

            for label in node_labels:
                labels[label] = (docname, label, term)

def make_enumlist_items_named(app, document):

    def list_item_enumerator(node):
        start = node.parent.get('start', 1)
        index = node.parent.children.index(node) + start

        enumtype = node.parent['enumtype']

        if enumtype == 'arabic':
            suffix = str(index)
        elif enumtype == 'loweralpha':
            suffix = chr(ord('a') + index - 1)
        elif enumtype == 'upperalpha':
            suffix = chr(ord('A') + index - 1)
        elif enumtype == 'lowerroman':
            suffix = toRoman(index).lower()
        elif enumtype == 'upperroman':
            suffix = toRoman(index).upper()
        else:
            raise Exception("unknown enumeration list type")

        if (isinstance(node.parent.parent, nodes.list_item)
                and isinstance(node.parent.parent.parent, nodes.enumerated_list)):
            prefix = list_item_enumerator(node.parent.parent) + '.'
        else:
            prefix = ''

        return prefix + suffix

    labels = app.env.domaindata['std']['labels']
    anonlabels = app.env.domaindata['std']['anonlabels']
    docname = app.env.docname

    for node in document.traverse(nodes.enumerated_list):
        child_labels = [node['ids']] + [c['ids'] for c in node.children[1:]]
        for (i, child) in enumerate(node.children):
            for label in child_labels[i]:
                labels[label] = (docname, label, list_item_enumerator(child))

def setup(app):
    app.connect('doctree-read', make_dlist_items_named)
    app.connect('doctree-read', make_enumlist_items_named)

    return {
        'version' : '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }

