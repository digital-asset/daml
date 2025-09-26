// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.{
  assertErrorCode,
  assertGrpcError,
  fail,
}
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.api.testtool.suites.v2_dev.ContractKeysContractIdIT.*
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.javaapi.data.{ContractId, DamlRecord}
import com.daml.ledger.test.java.experimental.contractidtests.{Contract, ContractRef}
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.ledger.error.groups.{
  CommandExecutionErrors,
  ConsistencyErrors,
  RequestValidationErrors,
}
import com.digitalasset.canton.util.TryUtil
import io.grpc.StatusRuntimeException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// See `daml-lf/spec/contract-id.rst` for more information on contract ID formats.
// Check the Ledger API accepts or rejects non-suffixed contract ID.
// - Central committer ledger implementations (sandboxes, KV...) may accept non-suffixed CID
// - Distributed ledger implementations (e.g. Canton) must reject non-suffixed CID
final class ContractKeysContractIdIT extends LedgerTestSuite {
  implicit val contractCompanion
      : ContractCompanion.WithoutKey[Contract.Contract$, Contract.ContractId, Contract] =
    Contract.COMPANION
  implicit val contractRefCompanion: ContractCompanion.WithKey[
    ContractRef.Contract,
    ContractRef.ContractId,
    ContractRef,
    String,
  ] = ContractRef.COMPANION

  List(
    TestConfiguration(
      description = "non-suffixed v1",
      example = nonSuffixedV1Cid,
      accepted = false,
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
          parseErrorCode: ErrorCode = RequestValidationErrors.InvalidArgument,
      )(
          update: ExecutionContext => (
              ParticipantTestContext,
              Party,
          ) => Future[Try[_]]
      ): Unit =
        super.test(
          shortIdentifier = result + camelCase(cidDescription) + "CKCid" + camelCase(description),
          description = result + "s " + cidDescription + " Contract Id in " + description,
          partyAllocation = allocate(SingleParty),
          enabled = isSupported,
          disabledReason = disabledReason,
        )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
          update(ec)(alpha, party).map {
            case Success(_) if accepted => ()
            case Failure(err: Throwable) if !accepted =>
              val (prefix, errorCode) =
                if (failsInPreprocessing)
                  (
                    "Illegal Contract ID",
                    CommandExecutionErrors.Preprocessing.PreprocessingFailed,
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

      test("create payload") { implicit ec => (alpha, party) =>
        alpha
          .create(party, new ContractRef(party, new Contract.ContractId(example)))
          .transformWith(Future.successful)
      }

      test("exercise target", parseErrorCode = RequestValidationErrors.InvalidField) {
        implicit ec => (alpha, party) =>
          for {
            contractCid <- alpha.create(party, new Contract(party))
            result <-
              alpha
                .exercise(
                  party,
                  new ContractRef.ContractId(example).exerciseChange(contractCid),
                )
                .transformWith(Future.successful)
          } yield result match {
            case Failure(exception: StatusRuntimeException)
                if Try(
                  assertErrorCode(
                    statusRuntimeException = exception,
                    expectedErrorCode = ConsistencyErrors.ContractNotFound,
                  )
                ).isSuccess =>
              TryUtil.unit

            case Success(_) => Failure(new UnknownError("Unexpected Success"))
            case otherwise => otherwise.map(_ => ())
          }
      }

      test("choice argument") { implicit ec => (alpha, party) =>
        for {
          contractCid <- alpha.create(party, new Contract(party))
          contractRefCid <- alpha.create(party, new ContractRef(party, contractCid))
          result <- alpha
            .exercise(party, contractRefCid.exerciseChange(new Contract.ContractId(example)))
            .transformWith(Future.successful)
        } yield result
      }

      test("create-and-exercise payload") { implicit ec => (alpha, party) =>
        for {
          contractCid <- alpha.create(party, new Contract(party))
          result <- alpha
            .exercise(
              party,
              new ContractRef(party, new Contract.ContractId(example)).createAnd
                .exerciseChange(contractCid),
            )
            .transformWith(Future.successful)
        } yield result
      }

      test("create-and-exercise choice argument") { implicit ec => (alpha, party) =>
        for {
          contractCid <- alpha.create(party, new Contract(party))
          result <- alpha
            .exercise(
              party,
              new ContractRef(party, contractCid).createAnd
                .exerciseChange(new Contract.ContractId(example)),
            )
            .transformWith(Future.successful)
        } yield result
      }

      test("exercise by key") { implicit ec => (alpha, party) =>
        for {
          contractCid <- alpha.create(party, new Contract(party))
          _ <- alpha.create(party, new ContractRef(party, contractCid))
          result <- alpha
            .exerciseByKey(
              party,
              ContractRef.TEMPLATE_ID_WITH_PACKAGE_ID,
              party,
              "Change",
              new DamlRecord(
                new DamlRecord.Field(new ContractId(example))
              ),
            )
            .transformWith(Future.successful)
        } yield result
      }
  }
}

object ContractKeysContractIdIT {
  private val nonSuffixedV1Cid = (0 to 32).map("%02x".format(_)).mkString
  private val suffixedV1Cid =
    "00bb20b46000000000000000000000000000000000000000000000000000000000ca11122023967e650dce35adf1dd5deb7e7bdf00aa5c677c9091a23520faa49cb2cda76a"

  private def camelCase(s: String): String =
    s.split("[ -]").iterator.map(_.capitalize).mkString("")

  final private case class TestConfiguration(
      description: String,
      example: String,
      accepted: Boolean,
      isSupported: Features => Boolean = _ => true,
      disabledReason: String = "",
      // Invalid v1 cids (e.g. no suffix when one is required) fail during command preprocessing.
      failsInPreprocessing: Boolean = false,
  )
}
