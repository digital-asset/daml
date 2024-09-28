// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.ledger.api.v2 as lav2
import lav2.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import lav2.transaction.{Transaction, TransactionTree}
import com.digitalasset.canton.http.util.Logging as HLogging
import com.daml.logging.LoggingContextOf
import LoggingContextOf.{label, newLoggingContext}
import com.daml.jwt.{AuthServiceJWTCodec, AuthServiceJWTPayload, JwtSigner, StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.jwt.{DecodedJwt, Jwt}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.tracing.NoTracing
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.{NonEmptyList, \/-}
import scalaz.syntax.foldable.*
import scalaz.syntax.tag.*
import spray.json.*
import scalaz.syntax.show.*

import java.util.concurrent.CopyOnWriteArrayList
import scala.collection as sc
import scala.concurrent.{Future, ExecutionContext as EC}
import scala.jdk.CollectionConverters.*

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class CommandServiceTest extends AsyncWordSpec with Matchers with Inside with NoTracing {
  import CommandServiceTest.*

  "create" should {
    // exercise and createAndExercise use the exact same party-handling code
    "let CommandMeta parties override JWT" in {
      val (cs, txns, trees) = simpleCommand()
      val specialActAs = NonEmptyList("bar")
      val specialReadAs = List("quux")
      def create(meta: Option[domain.CommandMeta.NoDisclosed]) =
        domain.CreateCommand(tplId, lav2.value.Record(), meta)
      for {
        normal <- cs.create(jwtForParties, multiPartyJwp, create(None))
        overridden <- cs.create(
          jwtForParties,
          multiPartyJwp,
          create(
            Some(
              util.JwtPartiesTest.partiesOnlyMeta(
                actAs = domain.Party subst specialActAs,
                readAs = domain.Party subst specialReadAs,
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

object CommandServiceTest extends BaseTest {
  private val multiPartyJwp = domain.JwtWritePayload(
    domain.ApplicationId("myapp"),
    submitter = domain.Party subst NonEmptyList("foo", "bar"),
    readAs = domain.Party subst List("baz", "quux"),
  )
  private val tplId =
    domain.ContractTypeId.Template(
      com.digitalasset.daml.lf.data.Ref.PackageRef.assertFromString("Foo"),
      "Bar",
      "Baz",
    )

  private[http] val applicationId: domain.ApplicationId = domain.ApplicationId("test")

  implicit private val ignoredLoggingContext
      : LoggingContextOf[HLogging.InstanceUUID with HLogging.RequestID] =
    newLoggingContext(label[HLogging.InstanceUUID with HLogging.RequestID])(identity)

  lazy val jwtForParties: Jwt = {
    import AuthServiceJWTCodec.JsonImplicits.*
    val payload: JsValue = {
      val standardJwtPayload: AuthServiceJWTPayload =
        StandardJWTPayload(
          issuer = None,
          userId = applicationId.unwrap,
          participantId = None,
          exp = None,
          format = StandardJWTTokenFormat.Scope,
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
        )
      standardJwtPayload.toJson
    }
    JwtSigner.HMAC256
      .sign(
        DecodedJwt(
          """{"alg": "HS256", "typ": "JWT"}""",
          payload.prettyPrint,
        ),
        "secret",
      )
      .fold(e => throw new IllegalArgumentException(s"cannot sign a JWT: ${e.shows}"), identity)
  }

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
            _ =>
              Future {
                txns.add(req)
                import lav2.event.{CreatedEvent, Event}, Event.Event.Created
                import com.digitalasset.canton.fetchcontracts.util.IdentifierConverters.apiIdentifier
                val creation = Event(
                  Created(
                    CreatedEvent(
                      templateId = Some(apiIdentifier(tplId)),
                      createArguments = Some(lav2.value.Record()),
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
        loggerFactory = loggerFactory,
      ),
      txns.asScala,
      trees.asScala,
    )
  }
}
