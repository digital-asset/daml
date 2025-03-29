#!/bin/bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Compile the attached model that contains Iou contracts
daml build --project-root model

# Start canton console with json enabled and upload daml model
../../bin/canton -c json.conf --bootstrap json.canton
