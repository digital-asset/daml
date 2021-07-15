// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

package object logging {
  type LoggingContextOf[+P] = LoggingContextOf.Module.Instance.T[P]
}
