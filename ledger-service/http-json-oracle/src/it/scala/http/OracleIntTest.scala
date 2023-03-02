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
import scala.util.Random.shuffle

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class OracleIntTest
    extends AbstractDatabaseIntegrationTest
    with OracleAroundAll
    with AkkaBeforeAndAfterAll
    with Matchers
    with Inside {
  import OracleIntTest._

  override protected def jdbcConfig: JdbcConfig =
    defaultJdbcConfig(oracleJdbcUrlWithoutCredentials, oracleUserName, oracleUserPwd)

  "fetchAndPersist" - {

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
        assertGen(
          Gen
            .containerOfN[Set, String](numContracts, coidGen.map(_.coid))
            .map(ids => shuffle(ids.toSeq))
        )
      contractIds should have size numContracts.toLong
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
            GACR("", "", contracts),
            GACR(offsetBetweenSetupAndRuns, "", Seq.empty),
          ).iterator
        }
      }

      def terminates(off: String) = LedgerClientJwt.Terminates fromDomain domain.Offset(off)

      def fixedEndOffset(off: String): LedgerClientJwt.GetTermination = { (_, _) => _ =>
        Future successful Some(terminates(off))
      }

      val (collectedIds, collectorHandler) = contractIdCollector
      val loggedDao = dbbackend.ContractDao(
        jdbcConfig.copy(baseConfig = jdbcConfig.baseConfig.copy(tablePrefix = "sixtyone_")),
        setupLogHandler = composite(_, collectorHandler),
      )
      import loggedDao.{logHandler, jdbcDriver, transact}, jdbcDriver.q.queries

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
            contractIds.view.zipWithIndex.map { case (cid, i) =>
              Tx(
                events = Seq(
                  Event(
                    Event.Event.Archived(
                      ArchivedEvent(
                        "",
                        cid,
                        Some(apiIdentifier(onlyTemplateId)),
                        Seq(onlyStakeholder),
                      )
                    )
                  )
                ),
                offset = padOffset(i.toString + numContracts.toString),
              )
            }.iterator
          }
          else Source.empty
        },
        fixedEndOffset(laterEndOffset),
      )

      val runningTest = instanceUUIDLogCtx { implicit lcx =>
        def runAFetch(fetch: ContractsFetch, expectedOffset: String) =
          transact(
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
          _ <- transact(for {
            _ <- queries.dropAllTablesIfExist
            _ <- queries.initDatabase
            _ <- fconn.commit
          } yield ())
            .unsafeToFuture()
          // prep the ACS, get into the start state where fetch will use tx streams
          _ <- runAFetch(initialFetch, offsetBetweenSetupAndRuns)
          // then use the transaction stream in parallel a bunch
          _ <- Future.traverse(1 to 16) { _ =>
            runAFetch(fetchAfterwards, laterEndOffset)
          }
        } yield {
          collectedIds.result().view.flatten.toSet should contain allElementsOf contractIds
          fail("got to end successfully")
        }
      }
      complete(runningTest) lastly {
        loggedDao.close()
      }
    }
  }

  private[this] def assertGen[A](ga: Gen[A])(implicit loc: source.Position): A =
    ga.sample getOrElse fail("can't generate random value")
}

object OracleIntTest {
  import doobie.util.{log => dlog}

  private def composite(first: dlog.LogHandler, second: dlog.LogHandler): dlog.LogHandler =
    dlog.LogHandler { ev =>
      first.unsafeRun(ev)
      second.unsafeRun(ev)
    }

  private def collector[A, C](
      filter: dlog.LogEvent PartialFunction A
  )(into: collection.Factory[A, C]): (collection.mutable.Builder[Nothing, C], dlog.LogHandler) = {
    val sink = into.newBuilder
    val fun = filter.lift
    (sink, dlog.LogHandler(fun(_).foreach(sink.+=)))
  }

  // just making sure this log analysis tactic works
  private def contractIdCollector = collector(Function unlift possibleContractIds)(Vector)

  private def possibleContractIds(ev: dlog.LogEvent): Option[NonEmpty[List[String]]] = ev match {
    case dlog.Success(sql, args, _, _) if (sql contains "DELETE") && (sql contains "contract_id") =>
      NonEmpty from (args collect { case it: String => it })
    case _ => None
  }
}
