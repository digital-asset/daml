// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.ledger.api.{v1 => lav1}
import lav1.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitRequest,
  SubmitAndWaitForTransactionTreeResponse,
}
import lav1.transaction.{Transaction, TransactionTree}
import com.daml.http.util.{Logging => HLogging}
import com.daml.logging.LoggingContextOf
import LoggingContextOf.{label, newLoggingContext}
import HttpServiceTestFixture.jwtForParties

import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.{\/-, NonEmptyList}
import scalaz.syntax.foldable._
import scalaz.syntax.tag._

import java.util.concurrent.CopyOnWriteArrayList
import scala.{collection => sc}
import scala.concurrent.{Future, ExecutionContext => EC}
import scala.jdk.CollectionConverters._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class CommandServiceTest extends AsyncWordSpec with Matchers with Inside {
  import CommandServiceTest._

  "create" should {
    // exercise and createAndExercise use the exact same party-handling code
    "let CommandMeta parties override JWT" in {
      val (cs, txns, trees) = simpleCommand()
      val specialActAs = NonEmptyList("bar")
      val specialReadAs = List("quux")
      def create(meta: Option[domain.CommandMeta]) =
        domain.CreateCommand(tplId, lav1.value.Record(), meta)
      for {
        normal <- cs.create(multiPartyJwt, multiPartyJwp, create(None))
        overridden <- cs.create(
          multiPartyJwt,
          multiPartyJwp,
          create(
            Some(
              domain.CommandMeta(
                None,
                actAs = Some(domain.Party subst specialActAs),
                readAs = Some(domain.Party subst specialReadAs),
                submissionId = None,
                deduplicationPeriod = None,
              )
            )
          ),
        )
      } yield {
        normal shouldBe a[\/-[_]]
        overridden shouldBe a[\/-[_]]
        inside(txns) {
          case sc.Seq(
                SubmitAndWaitRequest(Some(normalC)),
                SubmitAndWaitRequest(Some(overriddenC)),
              ) =>
            normalC.actAs should ===(multiPartyJwp.actAs)
            normalC.readAs should ===(multiPartyJwp.readAs)
            overriddenC.actAs should ===(specialActAs.toList)
            overriddenC.readAs should ===(specialReadAs)
        }
        trees shouldBe empty
      }
    }
  }
}

object CommandServiceTest {
  private val multiPartyJwp = domain.JwtWritePayload(
    domain.LedgerId("what"),
    domain.ApplicationId("myapp"),
    submitter = domain.Party subst NonEmptyList("foo", "bar"),
    readAs = domain.Party subst List("baz", "quux"),
  )
  private lazy val multiPartyJwt = jwtForParties(
    actAs = domain.Party unsubst multiPartyJwp.submitter.toList,
    readAs = domain.Party unsubst multiPartyJwp.readAs,
    ledgerId = Some(multiPartyJwp.ledgerId.unwrap),
  )
  private val tplId = domain.ContractTypeId.Template("Foo", "Bar", "Baz")

  implicit private val ignoredLoggingContext
      : LoggingContextOf[HLogging.InstanceUUID with HLogging.RequestID] =
    newLoggingContext(label[HLogging.InstanceUUID with HLogging.RequestID])(identity)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private def simpleCommand()(implicit
      ec: EC
  ): (CommandService, sc.Seq[SubmitAndWaitRequest], sc.Seq[SubmitAndWaitRequest]) = {
    val txns = new CopyOnWriteArrayList[SubmitAndWaitRequest]()
    val trees = new CopyOnWriteArrayList[SubmitAndWaitRequest]()
    (
      new CommandService(
        submitAndWaitForTransaction = (_, req) =>
          _ =>
            Future {
              txns.add(req)
              import lav1.event.{CreatedEvent, Event}, Event.Event.Created
              import com.daml.fetchcontracts.util.IdentifierConverters.apiIdentifier
              val creation = Event(
                Created(
                  CreatedEvent(
                    templateId = Some(apiIdentifier(tplId)),
                    createArguments = Some(lav1.value.Record()),
                  )
                )
              )
              \/-(SubmitAndWaitForTransactionResponse(Some(Transaction(events = Seq(creation)))))
            },
        submitAndWaitForTransactionTree = (_, req) =>
          _ =>
            Future {
              trees.add(req)
              \/-(SubmitAndWaitForTransactionTreeResponse(Some(TransactionTree())))
            },
      ),
      txns.asScala,
      trees.asScala,
    )
  }
}
