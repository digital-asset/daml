// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.ErrorCode
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions.{
  assertGrpcError,
  assertErrorCode,
  fail,
}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.ledger.api.testtool.suites.v1_8.ContractIdIT._
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.binding.Primitive.ContractId
import com.daml.ledger.test.semantic.ContractIdTests._
import io.grpc.StatusRuntimeException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// See `daml-lf/spec/contract-id.rst` for more information on contract ID formats.
// Check the Ledger API accepts or rejects non-suffixed contract ID.
// - Central committer ledger implementations (sandboxes, KV...) may accept non-suffixed CID
// - Distributed ledger implementations (e.g. Canton) must reject non-suffixed CID
final class ContractIdIT extends LedgerTestSuite {
  List(
    // Support for v0 contract ids existed only in sandbox-classic in
    // SDK 1.18 and older and has been dropped completely.
    TestConfiguration(
      description = "v0",
      example = v0Cid,
      accepted = false,
    ),
    TestConfiguration(
      description = "non-suffixed v1",
      example = nonSuffixedV1Cid,
      accepted = true,
      isSupported = features => features.contractIds.v1.isNonSuffixed,
      disabledReason = "non-suffixed V1 contract IDs are not supported",
      failsInPreprocessing = true,
    ),
    TestConfiguration(
      description = "non-suffixed v1",
      example = nonSuffixedV1Cid,
      accepted = false,
      isSupported = features => !features.contractIds.v1.isNonSuffixed,
      disabledReason = "non-suffixed V1 contract IDs are supported",
      failsInPreprocessing = true,
    ),
    TestConfiguration(
      description = "suffixed v1",
      example = suffixedV1Cid,
      accepted = true,
    ),
  ).foreach {
    case TestConfiguration(
          cidDescription,
          example,
          accepted,
          isSupported,
          disabledReason,
          failsInPreprocessing,
        ) =>
      val result = if (accepted) "Accept" else "Reject"

      def test(
          description: String,
          parseErrorCode: ErrorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
      )(
          update: ExecutionContext => (
              ParticipantTestContext,
              Party,
          ) => Future[Try[_]]
      ): Unit = {
        super.test(
          shortIdentifier = result + camelCase(cidDescription) + "Cid" + camelCase(description),
          description = result + "s " + cidDescription + " Contract Id in " + description,
          partyAllocation = allocate(SingleParty),
          enabled = isSupported,
          disabledReason = disabledReason,
        )(implicit ec => { case Participants(Participant(alpha, party)) =>
          update(ec)(alpha, party).map {
            case Success(_) if accepted => ()
            case Failure(err: Throwable) if !accepted =>
              val (prefix, errorCode) =
                if (failsInPreprocessing)
                  (
                    "Illegal Contract ID",
                    LedgerApiErrors.CommandExecution.Preprocessing.PreprocessingFailed,
                  )
                else
                  ("cannot parse ContractId", parseErrorCode)
              assertGrpcError(
                err,
                errorCode,
                Some(s"""$prefix "$example""""),
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

      test("exercise target", parseErrorCode = LedgerApiErrors.RequestValidation.InvalidField) {
        implicit ec => (alpha, party) =>
          for {
            contractCid <- alpha.create(party, Contract(party))
            result <-
              alpha
                .exercise(
                  party,
                  ContractId[ContractRef](example).exerciseChange(contractCid),
                )
                .transformWith(Future.successful)
          } yield result match {
            case Failure(exception: StatusRuntimeException)
                if Try(
                  assertErrorCode(
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
            .exercise(party, contractRefCid.exerciseChange(ContractId(example)))
            .transformWith(Future.successful)
        } yield result
      }

      test("create-and-exercise payload") { implicit ec => (alpha, party) =>
        for {
          contractCid <- alpha.create(party, Contract(party))
          result <- alpha
            .exercise(
              party,
              ContractRef(party = party, ref = ContractId(example)).createAnd
                .exerciseChange(contractCid),
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
              ContractRef(party = party, ref = contractCid).createAnd
                .exerciseChange(ContractId(example)),
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
      isSupported: Features => Boolean = _ => true,
      disabledReason: String = "",
      // Invalid v1 cids (e.g. no suffix when one is required) fail during command preprocessing
      // while invalid v0 cids fail earlier.
      failsInPreprocessing: Boolean = false,
  )
}
