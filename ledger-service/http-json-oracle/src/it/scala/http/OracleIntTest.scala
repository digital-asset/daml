// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import HttpServiceOracleInt.defaultJdbcConfig
import dbbackend.JdbcConfig
import com.daml.fetchcontracts.util.IdentifierConverters.apiIdentifier
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.value.test.ValueGenerators.{coidGen, idGen, party}
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.nonempty.NonEmpty
import com.daml.platform.participant.util.LfEngineToApi.lfValueToApiValue
import com.daml.testing.oracle.OracleAroundAll

import akka.stream.scaladsl.Source
import org.scalacheck.Gen
import org.scalactic.source
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Future

class OracleIntTest
    extends AbstractDatabaseIntegrationTest
    with OracleAroundAll
    with Matchers
    with Inside {
  override protected def jdbcConfig: JdbcConfig =
    defaultJdbcConfig(oracleJdbcUrlWithoutCredentials, oracleUserName, oracleUserPwd)

  "fetchAndPersist" - {
    import dao.{logHandler, jdbcDriver}, jdbcDriver.q.queries

    "avoids class 61 errors under concurrent update" in {
      import com.daml.ledger.api.{v1 => lav1}
      import com.daml.lf.value.Value
      import lav1.event.CreatedEvent
      import lav1.active_contracts_service.{GetActiveContractsResponse => GACR}

      val offsetBetweenSetupAndRuns = "B"
      val laterEndOffset = "D"
      val fakeJwt = com.daml.jwt.domain.Jwt("shouldn't matter")
      val fakeLedgerId = LedgerApiDomain.LedgerId("nonsense-ledger-id")
      val onlyContractId = assertGen(coidGen.map(_.coid))
      val onlyTemplateId = assertGen(idGen)
      val onlyDomainTemplateId = domain.TemplateId(
        onlyTemplateId.packageId,
        onlyTemplateId.qualifiedName.module.dottedName,
        onlyTemplateId.qualifiedName.name.dottedName,
      )
      val onlyStakeholder = assertGen(party)
      val initialFetch = new ContractsFetch(
        { (_, _, _, verbose) => _ =>
          val onlyPayload = inside(
            lfValueToApiValue(
              verbose,
              Value.ValueRecord(
                Some(onlyTemplateId),
                ImmArray(
                  (Some(Ref.Name assertFromString "owner"), Value.ValueParty(onlyStakeholder))
                ),
              ),
            )
          ) { case Right(lav1.value.Value(lav1.value.Value.Sum.Record(rec))) => rec }
          val onlyContract =
            CreatedEvent(
              "",
              onlyContractId,
              Some(apiIdentifier(onlyTemplateId)),
              None,
              Some(onlyPayload),
            )
          Source.fromIterator { () =>
            Seq(
              GACR("", "", Seq(onlyContract)),
              GACR(offsetBetweenSetupAndRuns, "", Seq.empty),
            ).iterator
          }
        },
        (_, _, _, _, _) => _ => Source.empty,
        { (_, _) => _ =>
          Future successful Some(
            LedgerClientJwt.Terminates fromDomain domain.Offset(offsetBetweenSetupAndRuns)
          )
        },
      )

      initialFetch.fetchAndPersist(
        fakeJwt,
        fakeLedgerId,
        NonEmpty(Set, domain.Party(onlyStakeholder: String)),
        List(onlyDomainTemplateId),
      )
    }
  }

  private[this] def assertGen[A](ga: Gen[A])(implicit loc: source.Position): A =
    ga.sample getOrElse fail("can't generate random value")
}
