// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool

import com.daml.ledger.api.rewrite.testtool.infrastructure.{LedgerSession, LedgerTestSuite}

package object tests {

  val all: Map[String, LedgerSession => LedgerTestSuite] = Map(
    "DivulgenceIT" -> (new Divulgence(_)),
    "Identity" -> (new Identity(_))
  )

}
