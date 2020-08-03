// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.Bytes

/**
  * A raw key with a flag indicating whether it is a log entry key or not.
  */
case class AnnotatedRawKey(key: Bytes, isLogEntry: Boolean)
