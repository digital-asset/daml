// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.digitalasset.canton.damltests.upgrade.v2.java.upgrade.{FetchQuote, Quote}
import com.digitalasset.canton.damltests.upgrade.v2.java.upgradeif.IQuote
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import scala.jdk.CollectionConverters.*

class InterfaceFetchIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "a view only consists of a fetch interface" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.dars.upload(UpgradingBaseTest.UpgradeV1)
    participant1.dars.upload(UpgradingBaseTest.UpgradeV2)

    participant2.synchronizers.connect_local(sequencer1, alias = daName)
    participant2.dars.upload(UpgradingBaseTest.UpgradeV1)
    participant2.dars.upload(UpgradingBaseTest.UpgradeV2)

    val bank = participant1.parties.enable(
      "bank",
      synchronizeParticipants = Seq(participant2),
    )
    val alice = participant2.parties.enable(
      "alice",
      synchronizeParticipants = Seq(participant1),
    )

    val quoteCid: Quote.ContractId =
      JavaDecodeUtil
        .decodeAllCreated(Quote.COMPANION)(
          participant1.ledger_api.javaapi.commands.submit(
            Seq(bank),
            new Quote(
              List(bank.toProtoPrimitive).asJava,
              List(alice.toProtoPrimitive).asJava,
              "DA",
              100,
            ).create.commands.asScala.toSeq,
          )
        )
        .loneElement
        .id

    val fetchQuoteCid: FetchQuote.ContractId =
      JavaDecodeUtil
        .decodeAllCreated(FetchQuote.COMPANION)(
          participant2.ledger_api.javaapi.commands.submit(
            Seq(alice),
            new FetchQuote(
              alice.toProtoPrimitive,
              alice.toProtoPrimitive,
              alice.toProtoPrimitive,
            ).create.commands.asScala.toSeq,
          )
        )
        .loneElement
        .id

    // TemplateId based fetch
    participant2.ledger_api.javaapi.commands.submit(
      Seq(alice),
      fetchQuoteCid.exerciseFQ_ExFetch(quoteCid).commands().asScala.toSeq,
    )

    // Interface based fetch
    participant2.ledger_api.javaapi.commands.submit(
      Seq(alice),
      fetchQuoteCid
        .exerciseFQ_IFetch(quoteCid.toInterface(IQuote.INTERFACE))
        .commands()
        .asScala
        .toSeq,
    )

  }
}
