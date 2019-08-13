#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

stack setup --resolver=lts-12.10
stack runhaskell CI.hs --package=extra
