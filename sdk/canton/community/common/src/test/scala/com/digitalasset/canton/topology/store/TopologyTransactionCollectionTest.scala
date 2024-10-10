// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Remove, Replace}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class TopologyTransactionCollectionTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private lazy val uid1 = UniqueIdentifier.tryFromProtoPrimitive("da::tluafed")
  private lazy val uid2 = UniqueIdentifier.tryFromProtoPrimitive("da::chop")
  private lazy val uid3 = UniqueIdentifier.tryFromProtoPrimitive("da::otherNamespace")

  private lazy val factory: TestingOwnerWithKeys =
    new TestingOwnerWithKeys(
      DefaultTestIdentities.daSequencerId,
      loggerFactory,
      parallelExecutionContext,
    )

  private def mkStoredTransaction(
      mapping: TopologyMapping,
      changeOp: TopologyChangeOp = Replace,
      serial: PositiveInt = PositiveInt.one,
  ): StoredTopologyTransaction[TopologyChangeOp, TopologyMapping] = {
    val mkTx =
      if (changeOp == Replace) factory.mkAddMultiKey[TopologyMapping] _
      else factory.mkRemove[TopologyMapping] _
    val tm = CantonTimestamp.now()
    StoredTopologyTransaction(
      SequencedTime(tm),
      EffectiveTime(tm),
      None,
      mkTx(
        mapping,
        NonEmpty(Set, factory.SigningKeys.key1),
        serial,
        false,
      ),
    )
  }
  private def mkDomainParametersChange(
      domainId: DomainId,
      changeOp: TopologyChangeOp = Replace,
      serial: PositiveInt = PositiveInt.one,
  ) =
    mkStoredTransaction(
      DomainParametersState(domainId, TestDomainParameters.defaultDynamic),
      changeOp,
      serial,
    )

  private lazy val replaceDOP1 = mkDomainParametersChange(DomainId(uid1))
  private lazy val removeDOP1 =
    mkDomainParametersChange(DomainId(uid1), Remove, serial = PositiveInt.two)
  private lazy val replaceDOP2 = mkDomainParametersChange(DomainId(uid2))
  private lazy val removeDOP3 =
    mkDomainParametersChange(DomainId(uid3), Remove, serial = PositiveInt.three)
  private lazy val replaceIDD1 = mkStoredTransaction(
    IdentifierDelegation(uid1, factory.SigningKeys.key1)
  )

  "StoredTopologyTransactions" should {
    lazy val simpleTransactionCollection = StoredTopologyTransactions(
      Seq(replaceDOP1, removeDOP1, replaceDOP2, removeDOP3, replaceIDD1)
    )

    "collect for simple collection" in {
      simpleTransactionCollection
        .collectOfType[Replace]
        .result should contain theSameElementsAs
        Seq(replaceDOP1, replaceDOP2, replaceIDD1)

      simpleTransactionCollection
        .collectOfType[Remove]
        .result should contain theSameElementsAs
        Seq(removeDOP1, removeDOP3)

      simpleTransactionCollection
        .collectOfMapping[IdentifierDelegation] shouldBe StoredTopologyTransactions(
        Seq(replaceIDD1)
      )

      simpleTransactionCollection
        .collectOfMapping[DomainParametersState]
        .result should contain theSameElementsAs
        Seq(replaceDOP1, removeDOP1, replaceDOP2, removeDOP3)

      simpleTransactionCollection.collectLatestByUniqueKey.result should contain theSameElementsAs
        Seq(removeDOP1, replaceDOP2, removeDOP3, replaceIDD1)

      TopologyTransactions.collectLatestByUniqueKey(
        simpleTransactionCollection.result
      ) should contain theSameElementsAs
        Seq(removeDOP1, replaceDOP2, removeDOP3, replaceIDD1)

      // remove duplicates
      TopologyTransactions.collectLatestByUniqueKey(
        simpleTransactionCollection.result ++ simpleTransactionCollection.result
      ) should contain theSameElementsAs Seq(removeDOP1, replaceDOP2, removeDOP3, replaceIDD1)
    }
  }
}
