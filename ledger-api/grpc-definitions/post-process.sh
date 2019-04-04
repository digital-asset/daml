#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Relegate any un-labelled headers to L4
perl -i -pe 's/^#+ ([^\]\[]*)$/#### \1/g' -i docs_rst_mmd.md
# Change links to :ref: notation
perl -i -pe 's/\[([^\[\]\n]*)\]\[([^\[\]\n]*)\]/:ref:`\1 <\2>`/g'  docs_rst_mmd.md
# Fix links to external google documentation
re='s,:ref:`(\.?google\.protobuf\.[^<>`\n]*) <(.*)>`, [\1](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#\2),g'
sed -r "$re" -i docs_rst_mmd.md
# Fix paragraph breaks within tables (perl because of multi-line feature)
perl -0777 -i -pe 's{(\| [^|]*(?:\n[^|]*)+\n[^|]*\|)}{($r=$1)=~s/\n/\\\n/g;$r}ge' docs_rst_mmd.md

# Convert any mardown to rst via pandoc
pandoc --columns=1000 -f markdown_mmd -t rst docs_rst_mmd.md -o docs_rst_mmd.rst
rm docs_rst_mmd.md
# Unescape underscores in labels
sed -r 's,\.\. \\\_(.*):,\.\. \_\1:,g' -i docs_rst_mmd.rst
# Remove duplicate backticks in ref links
sed -r 's,:ref:``([^`]*)``,:ref:`\1`  ,g' -i docs_rst_mmd.rst
sed -r 's,(\.\. \_[^:]*: ),\1 ,g' -i docs_rst_mmd.rst
# Add :local: to contents
sed -r 's,(\.\. contents::.*),\1\n  :local:,g' -i docs_rst_mmd.rst