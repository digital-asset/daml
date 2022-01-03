#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Fix links to external google protobuf documentation
re='s,:ref:`(\.?google\.protobuf\.[^<>`\n]*) <(.*)>`, `\1 <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#\2>`__,g'
sed -r "$re" -i docs.rst
# Fix links to external google rpc documentation
re='s,:ref:`(\.?google\.rpc\.[^<>`\n]*) <(.*)>`, `\1 <https://cloud.google.com/tasks/docs/reference/rpc/google.rpc#\2>`__,g'
sed -r "$re" -i docs.rst
