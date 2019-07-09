// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.util.UUID

import com.digitalasset.daml.lf.data.Ref
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

  override def timeLimit: Span = scaled(15.seconds)

  override protected def config: Config = Config.default

  private def partyManagementClient(stub: PartyManagementService): PartyManagementClient = {
    new PartyManagementClient(stub)
  }

  /**
    * Takes the future produced by a GRPC call.
    * If the call was successful, returns Right(result).
    * If the call failed, returns Left(statusCode).
    */
  private def withGrpcError[T](future: Future[T]): Future[Either[Status.Code, T]] =
    future
      .map(Right(_))
      .recover {
        case s: StatusRuntimeException => Left(s.getStatus.getCode)
        case s: StatusException => Left(s.getStatus.getCode)
      }

  /**
    * Performs checks on the result of a party allocation call.
    * Since the result of the call depends on the implementation,
    * we need to handle all error cases.
    */
  private def whenSuccessful[T, R](resultE: Either[Status.Code, T])(
      check: T => Assertion): Assertion =
    resultE match {
      case Right(result) =>
        // The call succeeded, check the result.
        check(result)
      case Left(Status.Code.UNIMPLEMENTED) =>
        // Ledgers are allowed to not implement this call.
        cancel("The tested ledger does not implement this endpoint")
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
          .map(_.unwrap should not be empty)

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
            result.displayName.value shouldBe displayName
            initialParties shouldNot contain(result)
            finalParties should contain(result)
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
            result.displayName.value shouldBe displayName
            initialParties shouldNot contain(result)
            finalParties should contain(result)
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
            initialParties shouldNot contain(result)
            finalParties should contain(result)
          })
        }
      }

      "create unique party names when allocating many parties" in allFixtures { c =>
        val client = partyManagementClient(c.partyManagementService)
        def allocateParty(i: Int) =
          client.allocateParty(None, Some(s"Test party $i")).map(Right(_)).recover {
            case s: StatusRuntimeException if s.getStatus.getCode == Status.Code.UNIMPLEMENTED =>
              Left(s.getStatus.getCode)
            case s: StatusException if s.getStatus.getCode == Status.Code.UNIMPLEMENTED =>
              Left(s.getStatus.getCode)
          }
        val N = 100
        for {
          initialParties <- client.listKnownParties()
          results <- Future.traverse(1 to N)(allocateParty)
          finalParties <- client.listKnownParties()
        } yield {
          results should have size N.toLong
          if (results.head.isLeft) {
            all(results) shouldBe a[Left[_, _]]
            cancel("The tested ledger does not implement this endpoint")
          } else {
            all(results) shouldBe a[Right[_, _]]
            val rights = results.map(_.right.get)
            initialParties should contain noElementsOf rights
            finalParties should contain allElementsOf rights
            results.map(_.right.get.party).toSet.size shouldBe N
          }
        }
      }
    }

  }
}
