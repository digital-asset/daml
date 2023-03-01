// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import HttpServiceOracleInt.defaultJdbcConfig
import dbbackend.JdbcConfig
import util.Logging.instanceUUIDLogCtx
import com.daml.fetchcontracts.util.AbsoluteBookmark
import com.daml.fetchcontracts.util.IdentifierConverters.apiIdentifier
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.value.test.ValueGenerators.{coidGen, idGen, party}
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.nonempty.NonEmpty
import com.daml.platform.participant.util.LfEngineToApi.lfValueToApiValue
import com.daml.testing.oracle.OracleAroundAll

import akka.stream.scaladsl.Source
import doobie.free.{connection => fconn}
import org.scalacheck.Gen
import org.scalactic.source
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class OracleIntTest
    extends AbstractDatabaseIntegrationTest
    with OracleAroundAll
    with AkkaBeforeAndAfterAll
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

      val numContracts = 1000
      val padOffset = (s: String) => s.reverse.padTo(10, '0').reverse

      val offsetBetweenSetupAndRuns = padOffset(numContracts.toString)
      val laterEndOffset = padOffset(numContracts.toString + numContracts.toString)

      val fakeJwt = com.daml.jwt.domain.Jwt("shouldn't matter")
      val fakeLedgerId = LedgerApiDomain.LedgerId("nonsense-ledger-id")
      val contractIds =
        Iterator.continually(assertGen(coidGen.map(_.coid))).take(numContracts).toList
      val onlyTemplateId = assertGen(idGen)
      val onlyDomainTemplateId = domain.TemplateId(
        onlyTemplateId.packageId,
        onlyTemplateId.qualifiedName.module.dottedName,
        onlyTemplateId.qualifiedName.name.dottedName,
      )
      val onlyStakeholder = assertGen(party)

      val permanentAcs: LedgerClientJwt.GetActiveContracts = { (_, _, _, verbose) => _ =>
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
        val contracts = contractIds.map(cid =>
          CreatedEvent(
            "",
            cid,
            Some(apiIdentifier(onlyTemplateId)),
            None,
            Some(onlyPayload),
          )
        )

        Source.fromIterator { () =>
          Seq(
            GACR("", "", contracts.toSeq),
            GACR(offsetBetweenSetupAndRuns, "", Seq.empty),
          ).iterator
        }
      }

      def terminates(off: String) = LedgerClientJwt.Terminates fromDomain domain.Offset(off)

      def fixedEndOffset(off: String): LedgerClientJwt.GetTermination = { (_, _) => _ =>
        Future successful Some(terminates(off))
      }

      val initialFetch = new ContractsFetch(
        permanentAcs,
        (_, _, _, _, _) => _ => Source.empty,
        fixedEndOffset(offsetBetweenSetupAndRuns),
      )
      val fetchAfterwards = new ContractsFetch(
        permanentAcs,
        { (_, _, _, startOff, endOff) => _ =>
          val deliverEverything =
            inside(startOff.value) {
              case lav1.ledger_offset.LedgerOffset.Value.Absolute(startAbs) =>
                inside(startAbs) {
                  case `offsetBetweenSetupAndRuns` => true
                  case `laterEndOffset` => false
                }
            }
          endOff should ===(terminates(laterEndOffset))
          if (deliverEverything) Source.fromIterator { () =>
            import lav1.event.{ArchivedEvent, Event}
            import lav1.transaction.{Transaction => Tx}
            Iterator
              .range(0, numContracts)
              .map(i =>
                Tx(
                  events = Seq(
                    Event(
                      Event.Event.Archived(
                        ArchivedEvent(
                          "",
                          contractIds(i),
                          Some(apiIdentifier(onlyTemplateId)),
                          Seq(onlyStakeholder),
                        )
                      )
                    )
                  ),
                  offset = padOffset(i.toString + numContracts.toString),
                )
              )
          }
          else Source.empty
        },
        fixedEndOffset(laterEndOffset),
      )

      // get into the start state where fetch will use tx streams
      instanceUUIDLogCtx { implicit lcx =>
        def runAFetch(fetch: ContractsFetch, expectedOffset: String) =
          dao
            .transact(
              fetch
                .fetchAndPersist(
                  fakeJwt,
                  fakeLedgerId,
                  NonEmpty(Set, domain.Party(onlyStakeholder: String)),
                  List(onlyDomainTemplateId),
                )
                .map(_ should ===(AbsoluteBookmark(terminates(expectedOffset))))
            )
            .unsafeToFuture()

        for {
          // prep the db
          _ <- dao
            .transact(for {
              _ <- queries.dropAllTablesIfExist
              _ <- queries.initDatabase
              _ <- fconn.commit
            } yield ())
            .unsafeToFuture()
          // prep the ACS
          _ <- runAFetch(initialFetch, offsetBetweenSetupAndRuns)
          // then use the transaction stream in parallel a bunch
          _ <- Future.traverse(1 to 16) { _ =>
            runAFetch(fetchAfterwards, laterEndOffset)
          }
        } yield fail()
      }
    }
  }

  private[this] def assertGen[A](ga: Gen[A])(implicit loc: source.Position): A =
    ga.sample getOrElse fail("can't generate random value")
}
