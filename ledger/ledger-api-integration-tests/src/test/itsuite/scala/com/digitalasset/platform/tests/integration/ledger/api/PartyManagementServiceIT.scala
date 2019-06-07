// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.util.UUID

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain.PartyDetails
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.ledger.client.services.admin.PartyManagementClient
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import io.grpc.{Status, StatusException, StatusRuntimeException}
import org.scalatest._
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import scalaz.syntax.tag._

import scala.concurrent.Future

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Option2Iterable",
    "org.wartremover.warts.StringPlusAny"
  ))
class PartyManagementServiceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with AsyncTimeLimitedTests
    with TestExecutionSequencerFactory
    with Matchers
    with Inside
    with OptionValues {

  override def timeLimit: Span = 15.seconds

  override protected def config: Config = Config.default

  private def partyManagementClient(stub: PartyManagementService): PartyManagementClient = {
    new PartyManagementClient(stub)
  }

  private type GrpcResult[T] = Either[Status.Code, T]

  /**
    * Takes the future produced by a GRPC call.
    * If the call was successful, returns Right(result).
    * If the call failed, returns Left(statusCode).
    */
  private def withGrpcError[T](future: Future[T]): Future[GrpcResult[T]] = {
    future
      .map(Right(_))
      .recoverWith {
        case s: StatusRuntimeException => Future.successful(Left(s.getStatus.getCode))
        case s: StatusException => Future.successful(Left(s.getStatus.getCode))
        case other => fail(s"$other is not a gRPC Status exception.")
      }
  }

  /**
    * Performs checks on the result of a party allocation call.
    * Since the result of the call depends on the implementation,
    * we need to handle all error cases.
    */
  private def whenSuccessful[T, R](resultE: GrpcResult[T])(check: T => Assertion): Assertion =
    resultE match {
      case Right(result) =>
        // The call succeeded, check the result.
        check(result)
      case Left(Status.Code.UNIMPLEMENTED) =>
        // Ledgers are allowed to not implement this call.
        succeed
      case Left(Status.Code.INVALID_ARGUMENT) =>
        // Ledgers are allowed to reject the request if they don't like the hint and/or display name.
        // However, we try really hard to use globally unique hints
        // that are also valid party names, so this outcome is unexpected.
        fail(s"Party allocation failed with INVALID_ARGUMENT.")
      case other =>
        fail(s"Unexpected GRPC error code $other while allocating a party")
    }

  "Party Management Service" when {

    "returning the participant ID" should {
      "succeed" in allFixtures { c =>
        partyManagementClient(c.partyManagementService)
          .getParticipantId()
          .map(id => id.unwrap.isEmpty shouldBe false)

      }
    }

    // Note: The ledger has some freedom in how to implement the allocateParty() call.
    //
    // The ledger may require that the given hint is a valid party name and may reject
    // the call if such a party already exists. This test therefore generates a unique hints.
    //
    // The ledger is allowed to disregard the hint, we can not make any assertions
    // on the relation between the given hint and the allocated party name.
    "allocating parties" should {

      "work when allocating a party with a hint" in allFixtures { c =>
        val client = partyManagementClient(c.partyManagementService)

        val hint = Ref.Party.assertFromString(s"party-${UUID.randomUUID().toString}")
        val displayName = s"Test party '$hint' for PartyManagementServiceIT"

        for {
          initialParties <- client.listKnownParties()
          resultE <- withGrpcError(client.allocateParty(Some(hint), Some(displayName)))
          finalParties <- client.listKnownParties()
        } yield {
          whenSuccessful(resultE)(result => {
            result.displayName shouldBe Some(displayName)
            initialParties.exists(p => p.party == result.party) shouldBe false
            finalParties.contains(result) shouldBe true
          })
        }
      }

      "work when allocating a party without a hint" in allFixtures { c =>
        val client = partyManagementClient(c.partyManagementService)

        val displayName = s"Test party for PartyManagementServiceIT"

        for {
          initialParties <- client.listKnownParties()
          resultE <- withGrpcError(client.allocateParty(None, Some(displayName)))
          finalParties <- client.listKnownParties()
        } yield {
          whenSuccessful(resultE)(result => {
            result.displayName shouldBe Some(displayName)
            initialParties.exists(p => p.party == result.party) shouldBe false
            finalParties.contains(result) shouldBe true
          })
        }
      }

      "work when allocating a party without a hint or display name" in allFixtures { c =>
        val client = partyManagementClient(c.partyManagementService)

        for {
          initialParties <- client.listKnownParties()
          resultE <- withGrpcError(client.allocateParty(None, None))
          finalParties <- client.listKnownParties()
        } yield {
          whenSuccessful(resultE)(result => {
            // Note: the ledger may or may not assign a display name
            initialParties.exists(p => p.party == result.party) shouldBe false
            finalParties.contains(result) shouldBe true
          })
        }
      }

      "create unique party names when allocating many parties" in allFixtures { c =>
        val client = partyManagementClient(c.partyManagementService)
        val N = 100

        for {
          initialParties <- client.listKnownParties()
          // Note: The following call concurrently creates N parties
          resultEs <- Future.traverse(1 to N)(i =>
            withGrpcError(client.allocateParty(None, Some(s"Test party $i"))))
          finalParties <- client.listKnownParties()
        } yield {

          // Collect outcomes
          val results = resultEs.foldRight((0, 0, List.empty[PartyDetails])) { (elem, acc) =>
            elem match {
              case Right(result) =>
                acc.copy(_3 = result :: acc._3)
              case Left(Status.Code.UNIMPLEMENTED) =>
                acc.copy(_1 = acc._1 + 1)
              case Left(Status.Code.INVALID_ARGUMENT) =>
                acc.copy(_2 = acc._2 + 1)
              case other =>
                fail(s"Unexpected GRPC error code $other while allocating a party")
            }
          }

          results match {
            case (0, 0, result) =>
              // All calls succeeded. None of the new parties must be present initially,
              // all of them must be present at the end, and all new party names must be unique.
              result.foreach(r => initialParties.exists(p => p.party == r.party) shouldBe false)
              result.foreach(r => finalParties.contains(r) shouldBe true)
              result.map(_.party).toSet.size shouldBe N
            case (_, 0, Nil) =>
              // All calls returned UNIMPLEMENTED
              succeed
            case (0, _, Nil) =>
              // All calls returned INVALID_ARGUMENT
              fail(s"Party allocation failed with INVALID_ARGUMENT.")
            case (u, i, result) =>
              // A party allocation call may fail, but they all should fail with the same error
              fail(
                s"Not all party allocation calls had the same outcome. $u UNIMPLEMENTED, $i INVALID_ARGUMENT, ${result.size} succeeded")
          }
        }
      }
    }

  }
}
