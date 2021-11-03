# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from docutils import nodes
from docutils.parsers.rst import Directive
from sphinx.writers.html import HTMLTranslator
from typing import Dict, Any

import json

error_codes_data = {}

CONFIG_OPT = 'error_codes_json_export'

def load_data(app, config):
    global error_codes_data

    if CONFIG_OPT in config:
        file_name = config[CONFIG_OPT]
        try:
            with open(file_name) as f:
                tmp = json.load(f)
                error_codes_data = {error["code"]: error for error in tmp["errorCodes"]}
        except EnvironmentError:
            print(f"Failed to open file: '{file_name}'")
            raise


def setup(app):
    app.add_config_value(CONFIG_OPT, '', 'env')
    app.connect('config-inited', load_data)
    app.add_node(error_code_node)
    app.add_directive('list-all-error-codes', ListAllErrorCodesDirective)
    # Callback functions for populating error code section after the doctree is resolved
    app.connect('doctree-resolved', process_error_code_nodes)
    # Overwriting standard Sphinx translator to allow linking of return types
    app.set_translator('html', PatchedHTMLTranslator)


class error_code_node(nodes.General, nodes.Element):
    def __init__(self, codes, expl="", res=""):
        nodes.Element.__init__(self)
        print("found error node codes: %s" % codes)
        self.codes = codes
        self.expl = expl
        self.res = res


class ListAllErrorCodesDirective(Directive):
    has_contents = True
    required_arguments = 0
    final_argument_whitespace = True
    def run(self):
        return [error_code_node([], None, None)]


def text_node(n, txt):
    # Doubling the parameter, as TextElements want the raw text and the text
    return n(txt, txt)


def process_error_code_nodes(app, doctree, fromDocName):
    def build_indented_bold_and_non_bold_node(bold_text: str, non_bold_text: str):
        bold = text_node(n=nodes.strong, txt=bold_text)
        non_bold = text_node(n=nodes.inline, txt=non_bold_text)
        both = nodes.definition('', bold)
        both += non_bold
        return both

    def item_to_node(item: Dict[str, Any]) -> nodes.definition_list_item:
        node = nodes.definition_list_item()
        term_node = text_node(nodes.term, "%s" % (item["code"]))
        definition_node = nodes.definition('', text_node(nodes.paragraph, ''))
        if item["deprecation"]:
            definition_node += build_indented_bold_and_non_bold_node(
                bold_text="Deprecated: ",
                non_bold_text=item['deprecation'])
        if item["explanation"]:
            definition_node += build_indented_bold_and_non_bold_node(
                bold_text="Explanation: ",
                non_bold_text=item['explanation'])
        definition_node += build_indented_bold_and_non_bold_node(
            bold_text="Category: ",
            non_bold_text=item['category'])
        if item["conveyance"]:
            definition_node += build_indented_bold_and_non_bold_node(
                bold_text="Conveyance: ",
                non_bold_text=item['conveyance'])
        if item["resolution"]:
            definition_node += build_indented_bold_and_non_bold_node(
                bold_text="Resolution: ",
                non_bold_text=item['resolution'])
        permalink_node = build_permalink(
            app=app,
            fromDocName=fromDocName,
            term=item["code"],
            # NOTE: This is the path to the docs file in Sphinx's source dir
            docname='error-codes/self-service/index',
            node_to_permalink_to=term_node)
        node += [permalink_node, definition_node]

        return node

    # A node of this tree is a dict that can contain
    #   1. further nodes and/or
    #   2. 'leaves' in the form of a list of error (code) data
    # Thus, the resulting tree is very similar to a trie
    def build_hierarchical_tree_of_error_data(data) -> defaultdict:
        create_node = lambda: defaultdict(create_node)
        root = defaultdict(create_node)
        for error_data in data:
            current = root
            for group in error_data["hierarchicalGrouping"]:
                current = current[group]
            if 'error-codes' in current:
                current['error-codes'].append(error_data)
            else:
                current['error-codes'] = [error_data]
        return root

    # DFS to traverse the error code data tree from `build_hierarchical_tree_of_error_data`
    # While traversing the tree, the presentation of the error codes on the documentation is built
    def dfs(tree, node, prefix: str) -> None:
        if 'error-codes' in tree:
            dlist = nodes.definition_list()
            for code in tree['error-codes']:
                dlist += item_to_node(item=code)
            node += dlist
        i = 1
        for subtopic, subtree in tree.items():
            if subtopic == 'error-codes':
                continue
            subprefix = f"{prefix}{i}."
            i += 1
            subtree_node = text_node(n=nodes.rubric, txt = subprefix + " " + subtopic)
            dfs(tree=subtree, node=subtree_node, prefix=subprefix)
            node += subtree_node

    for node in doctree.traverse(error_code_node):
        # Valid error codes given to the .. error-codes:: directive as argument
        # given_error_codes = [error_codes_data[code] for code in node.codes if code in error_codes_data]
        # Code for manually overwriting the explanation or resolution of an error code

        section = nodes.section()
        root = nodes.rubric(rawsource = "", text = "")
        section += root
        tree = build_hierarchical_tree_of_error_data(data=error_codes_data.values())
        dfs(tree=tree, node=root, prefix="")
        node.replace_self(new=[section])


# Build a permalink/anchor to a specific command/metric
def build_permalink(app, fromDocName, term, docname, node_to_permalink_to):
    reference_node = nodes.reference('', '')
    reference_node['refuri'] = app.builder.get_relative_uri(fromDocName, docname) + '#' + term

    reference_node += node_to_permalink_to

    target_node = nodes.target('', '', ids=[term])
    node_to_permalink_to += target_node
    return reference_node


class PatchedHTMLTranslator(HTMLTranslator):
    # We overwrite this method as otherwise an assertion fails whenever we create a reference whose parent is
    # not a TextElement. Concretely, this enables using method `build_return_type_node` for creating links from
    # return types to the appropriate scaladocs
    # Similar to solution from https://stackoverflow.com/a/61669375

    def visit_reference(self, node):
        # type: (nodes.Node) -> None
        atts = {'class': 'reference'}
        if node.get('internal') or 'refuri' not in node:
            atts['class'] += ' internal'
        else:
            atts['class'] += ' external'
        if 'refuri' in node:
            atts['href'] = node['refuri'] or '#'
            if self.settings.cloak_email_addresses and \
               atts['href'].startswith('mailto:'):
                atts['href'] = self.cloak_mailto(atts['href'])
                self.in_mailto = 1
        else:
            assert 'refid' in node, \
                   'References must have "refuri" or "refid" attribute.'
            atts['href'] = '#' + node['refid']
        if not isinstance(node.parent, nodes.TextElement):
            # ---------------------
            # Commenting out this assertion is the only change compared to Sphinx version 3.4.3
            # assert len(node) == 1 and isinstance(node[0], nodes.image)
            # ---------------------
            atts['class'] += ' image-reference'
        if 'reftitle' in node:
            atts['title'] = node['reftitle']
        if 'target' in node:
            atts['target'] = node['target']
        self.body.append(self.starttag(node, 'a', '', **atts))

        if node.get('secnumber'):
            self.body.append(('%s' + self.secnumber_suffix) %
                             '.'.join(map(str, node['secnumber'])))

