# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from pygments.lexers.haskell import HaskellLexer
from pygments.lexer import inherit
from pygments.token import *

class DamlLexer(HaskellLexer):

    name = 'Daml'
    aliases = ['daml']
    filenames = ['*.daml']

    daml_reserved = ('template', 'with', 'controller', 'can', 'ensure', 'daml', 'observer', 'signatory', 'agreement', 'controller', 'nonconsuming', 'return', 'this')

    tokens = {
        'root': [
            (r'\b(%s)(?!\')\b' % '|'.join(daml_reserved), Keyword.Reserved),
            (r'\b(True|False)\b', Keyword.Constant),
            (r'≡', Operator),
            inherit
        ]
    }



