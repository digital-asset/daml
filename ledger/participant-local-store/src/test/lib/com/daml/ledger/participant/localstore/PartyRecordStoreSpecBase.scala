// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.localstore

import com.daml.ledger.participant.localstore.api.PartyRecordStore
import com.daml.ledger.resources.TestResourceContext
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, AsyncTestSuite, EitherValues, OptionValues}

import scala.concurrent.Future

trait PartyRecordStoreSpecBase
    extends TestResourceContext
    with Matchers
    with OptionValues
    with EitherValues { self: AsyncTestSuite =>

  def newStore(): PartyRecordStore

  final protected def testIt(
      f: PartyRecordStore => Future[Assertion]
  ): Future[Assertion] = f(
    newStore()
  )

}
