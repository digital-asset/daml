// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.common.domain.ServiceAgreementId
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.service.ServiceAgreementAcceptance
import com.digitalasset.canton.topology.DefaultTestIdentities
import org.scalatest.wordspec.AsyncWordSpec

trait ServiceAgreementAcceptanceStoreTest extends BaseTest { this: AsyncWordSpec =>

  def serviceAgreementAcceptanceStore(newStore: => ServiceAgreementAcceptanceStore): Unit = {

    val acceptance = ServiceAgreementAcceptance(
      ServiceAgreementId.tryCreate("foobar"),
      DefaultTestIdentities.participant1,
      SymbolicCrypto.emptySignature,
      CantonTimestamp.Epoch,
    )

    "insert and list a new acceptance" in {
      val store = newStore
      for {
        _ <- store.insertAcceptance(acceptance).valueOrFail("insert first acceptance")
        acceptance2 = acceptance.copy(participantId = DefaultTestIdentities.participant2)
        _ <- store.insertAcceptance(acceptance2).valueOrFail("insert second acceptance")
        acceptances <- store.listAcceptances()
      } yield {
        acceptances should have size 2
      }
    }

    "ignore insert of a second acceptance" in {
      val store = newStore
      for {
        _ <- store.insertAcceptance(acceptance).valueOrFail("insert first acceptance")
        acceptance2 = acceptance.copy(timestamp = CantonTimestamp.Epoch.addMicros(100))
        _ <- store.insertAcceptance(acceptance2).valueOrFail("insert second acceptance")
        acceptances <- store.listAcceptances()
      } yield {
        acceptances should have size 1
      }

    }

  }

}
