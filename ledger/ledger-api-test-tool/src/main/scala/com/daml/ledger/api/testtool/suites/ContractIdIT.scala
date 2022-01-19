// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.error.definitions.LedgerApiErrors
import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions.{
  assertGrpcError,
  assertSelfServiceErrorCode,
  fail,
}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.ledger.api.testtool.suites.ContractIdIT._
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.binding.Primitive.ContractId
import com.daml.ledger.test.semantic.ContractIdTests._
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// Check the Ledger API accepts or rejects non-suffixed contract ID.
// - Central committer ledger implementations (sandboxes, KV...) may accept non-suffixed CID
// - Distributed ledger implementations (e.g. Canton) must reject non-suffixed CID
final class ContractIdIT extends LedgerTestSuite {

  List(
    TestConfiguration(
      description = "v0",
      example = v0Cid,
      accepted = true,
      isSupported = features => features.contractIds.v0.isSupported,
      disabledReason = "V0 contract IDs are not supported",
    ),
    TestConfiguration(
      description = "v0",
      example = v0Cid,
      accepted = false,
      isSupported = features => features.contractIds.v0.isNotSupported,
      disabledReason = "V0 contract IDs are supported",
    ),
    TestConfiguration(
      description = "non-suffixed v1",
      example = nonSuffixedV1Cid,
      accepted = true,
      isSupported =
        features => features.contractIds.v1.isNonSuffixed || features.contractIds.v1.isBoth,
      disabledReason = "non-suffixed V1 contract IDs are not supported",
    ),
    TestConfiguration(
      description = "non-suffixed v1",
      example = nonSuffixedV1Cid,
      accepted = false,
      isSupported =
        features => !(features.contractIds.v1.isNonSuffixed || features.contractIds.v1.isBoth),
      disabledReason = "non-suffixed V1 contract IDs are supported",
    ),
    TestConfiguration(
      description = "suffixed v1",
      example = suffixedV1Cid,
      accepted = true,
      isSupported =
        features => features.contractIds.v1.isSuffixed || features.contractIds.v1.isBoth,
      disabledReason = "suffixed V1 contract IDs are not supported",
    ),
    TestConfiguration(
      description = "suffixed v1",
      example = suffixedV1Cid,
      accepted = false,
      isSupported =
        features => !(features.contractIds.v1.isSuffixed || features.contractIds.v1.isBoth),
      disabledReason = "suffixed V1 contract IDs are supported",
    ),
  ).foreach {
    case TestConfiguration(cidDescription, example, accepted, isSupported, disabledReason) =>
      val result = if (accepted) "Accept" else "Reject"

      def test(description: String)(
          update: ExecutionContext => (
              ParticipantTestContext,
              Party,
          ) => Future[Try[_]]
      ): Unit = {
        super.test(
          shortIdentifier = result + camelCase(cidDescription) + "Cid" + camelCase(description),
          description = result + "s " + cidDescription + " Contract Id in " + description,
          participants = allocate(SingleParty),
          enabled = isSupported,
          disabledReason = disabledReason,
        )(implicit ec => { case Participants(Participant(alpha, party)) =>
          update(ec)(alpha, party).map {
            case Success(_) if accepted => ()
            case Failure(err: Throwable) if !accepted =>
              assertGrpcError(
                alpha,
                err,
                Status.Code.INVALID_ARGUMENT,
                LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
                Some(s"""Illegal Contract ID "$example""""),
                checkDefiniteAnswerMetadata = true,
              )
              ()
            case otherwise =>
              fail("Unexpected " + otherwise.fold(err => s"failure: $err", _ => "success"))
          }
        })
      }

      test("create payload") { implicit ec => (alpha, party) =>
        alpha
          .create(party, ContractRef(party, ContractId(example)))
          .transformWith(Future.successful)
      }

      test("exercise target") { implicit ec => (alpha, party) =>
        for {
          contractCid <- alpha.create(party, Contract(party))
          result <-
            alpha
              .exercise(
                party,
                ContractId[ContractRef](example).exerciseChange(_, contractCid),
              )
              .transformWith(Future.successful)
        } yield result match {
          // Assert V1 error code
          case Failure(GrpcException(GrpcStatus(Status.Code.ABORTED, Some(msg)), _))
              if !alpha.features.selfServiceErrorCodes && msg.contains(
                s"Contract could not be found with id $example"
              ) =>
            Success(())

          // Assert self-service error code
          case Failure(exception: StatusRuntimeException)
              if alpha.features.selfServiceErrorCodes &&
                Try(
                  assertSelfServiceErrorCode(
                    statusRuntimeException = exception,
                    expectedErrorCode = LedgerApiErrors.ConsistencyErrors.ContractNotFound,
                  )
                ).isSuccess =>
            Success(())

          case Success(_) => Failure(new UnknownError("Unexpected Success"))
          case otherwise => otherwise.map(_ => ())
        }
      }

      test("choice argument") { implicit ec => (alpha, party) =>
        for {
          contractCid <- alpha.create(party, Contract(party))
          contractRefCid <- alpha.create(party, ContractRef(party = party, ref = contractCid))
          result <- alpha
            .exercise(party, contractRefCid.exerciseChange(_, ContractId(example)))
            .transformWith(Future.successful)
        } yield result
      }

      test("create-and-exercise payload") { implicit ec => (alpha, party) =>
        for {
          contractCid <- alpha.create(party, Contract(party))
          result <- alpha
            .exercise(
              party,
              p =>
                ContractRef(party = p, ref = ContractId(example)).createAnd
                  .exerciseChange(p, contractCid),
            )
            .transformWith(Future.successful)
        } yield result
      }

      test("create-and-exercise choice argument") { implicit ec => (alpha, party) =>
        for {
          contractCid <- alpha.create(party, Contract(party))
          result <- alpha
            .exercise(
              party,
              p =>
                ContractRef(party = p, ref = contractCid).createAnd
                  .exerciseChange(p, ContractId(example)),
            )
            .transformWith(Future.successful)
        } yield result
      }

      test("exercise by key") { implicit ec => (alpha, party) =>
        for {
          contractCid <- alpha.create(party, Contract(party))
          _ <- alpha.create(party, ContractRef(party = party, ref = contractCid))
          result <- alpha
            .exerciseByKey(
              party,
              ContractRef.id,
              Value(Value.Sum.Party(Party.unwrap(party))),
              "Change",
              Value(
                Value.Sum.Record(
                  Record(None, List(RecordField("", Some(Value(Value.Sum.ContractId(example))))))
                )
              ),
            )
            .transformWith(Future.successful)
        } yield result
      }
  }
}

object ContractIdIT {
  private val v0Cid = "#V0 Contract ID"
  private val nonSuffixedV1Cid = (0 to 32).map("%02x".format(_)).mkString
  private val suffixedV1Cid = (0 to 48).map("%02x".format(_)).mkString

  private def camelCase(s: String): String =
    s.split("[ -]").iterator.map(_.capitalize).mkString("")

  final private case class TestConfiguration(
      description: String,
      example: String,
      accepted: Boolean,
      isSupported: Features => Boolean,
      disabledReason: String,
  )
}
