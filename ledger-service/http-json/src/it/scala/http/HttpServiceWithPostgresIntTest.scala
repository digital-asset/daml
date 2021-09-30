// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.daml.http.HttpServiceTestFixture.{getContractId, getResult}
import com.daml.http.domain.ContractId
import com.daml.http.util.ClientUtil.boxedRecord
import com.daml.scalautil.Statement.discard
import com.daml.testing.postgresql.PostgresAroundAll
import com.daml.ledger.api.v1.{value => v}
import com.daml.ledger.service.MetadataReader
import com.daml.lf.data.Ref
import org.scalatest.Assertion
import spray.json.{JsString, JsValue}

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceWithPostgresIntTest
    extends AbstractHttpServiceIntegrationTest
    with PostgresAroundAll
    with HttpServicePostgresInt {

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  "query persists all active contracts" in withHttpService { (uri, encoder, _, _) =>
    val (party, headers) = getUniquePartyAndAuthHeaders("Alice")
    val searchDataSet = genSearchDataSet(party)
    searchExpectOk(
      searchDataSet,
      jsObject("""{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR"}}"""),
      uri,
      encoder,
      headers,
    ).flatMap { searchResult: List[domain.ActiveContract[JsValue]] =>
      discard { searchResult should have size 2 }
      discard { searchResult.map(getField("currency")) shouldBe List.fill(2)(JsString("EUR")) }
      selectAllDbContracts.flatMap { listFromDb =>
        discard { listFromDb should have size searchDataSet.size.toLong }
        val actualCurrencyValues: List[String] = listFromDb
          .flatMap { case (_, _, _, payload, _, _, _) =>
            payload.asJsObject().getFields("currency")
          }
          .collect { case JsString(a) => a }
        val expectedCurrencyValues = List("EUR", "EUR", "GBP", "BTC")
        // the initial create commands submitted asynchronously, we don't know the exact order, that is why sorted
        actualCurrencyValues.sorted shouldBe expectedCurrencyValues.sorted
      }
    }
  }

  "Bug1" in withHttpServiceAndClient { (uri, encoder, _, client, _) =>
    import scalaz.std.vector._
    import scalaz.syntax.tag._
    import scalaz.syntax.traverse._
    import scalaz.std.scalaFuture._
    import scalaz.std.option.some
    import com.daml.ledger.api.refinements.{ApiTypes => lar}
    import shapeless.record.{Record => ShRecord}
    val partyIds = Vector("Alice", "Bob")
    val partyManagement = client.partyManagementClient
    def userCreateCommand(
        username: domain.Party,
        following: Seq[domain.Party] = Seq.empty,
    ): domain.CreateCommand[v.Record, domain.TemplateId.OptionalPkg] = {
      val templateId = domain.TemplateId(None, "User", "User")
      val followingList = following.map(party => v.Value(v.Value.Sum.Party(party.unwrap)))
      val arg = v.Record(
        fields = List(
          v.RecordField("username", Some(v.Value(v.Value.Sum.Party(username.unwrap)))),
          v.RecordField(
            "following",
            some(v.Value(v.Value.Sum.List(v.List.of(followingList)))),
          ),
        )
      )

      domain.CreateCommand(templateId, arg, None)
    }
    def userExerciseFollowCommand(
        contractId: lar.ContractId,
        toFollow: domain.Party,
    ): domain.ExerciseCommand[v.Value, domain.EnrichedContractId] = {
      val templateId = domain.TemplateId(None, "User", "User")
      val reference = domain.EnrichedContractId(Some(templateId), contractId)
      val arg = recordFromFields(ShRecord(userToFollow = v.Value.Sum.Party(toFollow.unwrap)))
      val choice = lar.Choice("Follow")

      domain.ExerciseCommand(reference, choice, boxedRecord(arg), None)
    }

    def followUser(contractId: lar.ContractId, actAs: domain.Party, toFollow: domain.Party) = {
      val exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId] =
        userExerciseFollowCommand(contractId, toFollow)
      val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

      postJsonRequest(
        uri.withPath(Uri.Path("/v1/exercise")),
        exerciseJson,
        headers = headersWithPartyAuth(actAs = List(actAs.unwrap)),
      )
        .map { case (exerciseStatus, exerciseOutput) =>
          exerciseStatus shouldBe StatusCodes.OK
          assertStatus(exerciseOutput, StatusCodes.OK)
          ()
        }

    }
    val packageId: Ref.PackageId = MetadataReader
      .templateByName(metadataUser)(Ref.QualifiedName.assertFromString("User:User"))
      .headOption
      .map(_._1)
      .getOrElse(fail(s"Cannot retrieve packageId"))

    def queryUsers(fromPerspectiveOfParty: domain.Party) = {
      val query = jsObject(s"""{
             "templateIds": ["$packageId:User:User"],
             "query": {}
          }""")

      postJsonRequest(
        uri.withPath(Uri.Path("/v1/query")),
        query,
        headers = headersWithPartyAuth(actAs = List(fromPerspectiveOfParty.unwrap)),
      ).map { case (searchStatus, searchOutput) =>
        searchStatus shouldBe StatusCodes.OK
        assertStatus(searchOutput, StatusCodes.OK)
      }
    }

    partyIds
      .traverse { p =>
        partyManagement.allocateParty(Some(p), Some(s"$p & Co. LLC"))
      }
      .flatMap { allocatedParties =>
        val commands = allocatedParties
          .map(details =>
            (
              details.party.asInstanceOf[String],
              userCreateCommand(domain.Party(details.party.asInstanceOf[String])),
            )
          )
        for {
          users <- commands.traverse { case (party, command) =>
            postCreateCommand(
              command,
              encoder,
              uri,
              headers = headersWithPartyAuth(actAs = List(party)),
            ).map { case (status, output) =>
              status shouldBe StatusCodes.OK
              assertStatus(output, StatusCodes.OK)
              getContractId(getResult(output))
            }: Future[ContractId]
          }
          aliceUserId = users(0)
          bobUserId = users(1)
          _ <- followUser(aliceUserId, domain.Party("Alice"), domain.Party("Bob"))
          _ <- queryUsers(domain.Party("Bob"))
          _ <- followUser(bobUserId, domain.Party("Bob"), domain.Party("Alice"))
          _ <- queryUsers(domain.Party("Alice"))
        } yield succeed
      }: Future[Assertion]
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def selectAllDbContracts
      : Future[List[(String, String, JsValue, JsValue, Vector[String], Vector[String], String)]] = {
    import doobie.implicits._, doobie.postgres.implicits._
    import dao.jdbcDriver.q.queries, queries.Implicits._

    val q =
      sql"""SELECT contract_id, tpid, key, payload, signatories, observers, agreement_text FROM ${queries.contractTableName}"""
        .query[(String, String, JsValue, JsValue, Vector[String], Vector[String], String)]

    dao.transact(q.to[List]).unsafeToFuture()
  }

  private def getField(k: String)(a: domain.ActiveContract[JsValue]): JsValue =
    a.payload.asJsObject().getFields(k) match {
      case Seq(x) => x
      case xs @ _ => fail(s"Expected exactly one value, got: $xs")
    }
}
