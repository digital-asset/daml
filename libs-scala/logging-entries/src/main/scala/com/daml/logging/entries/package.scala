// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

package object entries {
  type LoggingKey = String
  type LoggingEntry = (LoggingKey, LoggingValue)
}
