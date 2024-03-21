// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import java.util.UUID

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
}
import com.daml.ledger.api.v1.admin.config_management_service.{
  ConfigManagementServiceGrpc,
  GetTimeModelRequest,
}
import com.daml.ledger.api.v1.admin.package_management_service.{
  ListKnownPackagesRequest,
  PackageManagementServiceGrpc,
}
import com.daml.ledger.api.v1.admin.party_management_service.{
  ListKnownPartiesRequest,
  PartyManagementServiceGrpc,
}
import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionEndRequest,
}
import com.daml.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
}
import com.daml.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  LedgerConfigurationServiceGrpc,
}
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc,
}
import com.daml.ledger.api.v1.package_service.{ListPackagesRequest, PackageServiceGrpc}
import com.daml.ledger.api.v1.testing.time_service.{GetTimeRequest, TimeServiceGrpc}
import com.daml.ledger.api.v1.transaction_service.{GetLedgerEndRequest, TransactionServiceGrpc}
import com.daml.platform.sandbox.fixture.SandboxFixture
import io.grpc
import io.grpc._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Assertion, Inside}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn
import scala.util.{Failure, Success, Try}

class LegacyServiceIT
    extends AsyncWordSpec
    with Matchers
    with Inside
    with SandboxFixture
    with ScalaFutures
    with SuiteResourceManagementAroundAll {

  val legacyCallInterceptor: ClientInterceptor = new ClientInterceptor {
    override def interceptCall[ReqT, RespT](
        method: grpc.MethodDescriptor[ReqT, RespT],
        callOptions: CallOptions,
        next: Channel,
    ): ClientCall[ReqT, RespT] = {
      val legacyMethod = method
        .toBuilder()
        .setFullMethodName(method.getFullMethodName.replace("com.daml", "com.digitalasset"))
        .build()
      next.newCall(legacyMethod, callOptions)
    }
  }

  private val randomLedgerId = UUID.randomUUID().toString

  private def expectNotUnimplemented[A](block: => A): Assertion = {
    inside(Try(block)) {
      case Success(_) => succeed
      case Failure(exc: StatusRuntimeException) =>
        exc.getStatus.getCode should not be Status.Code.UNIMPLEMENTED
      case Failure(otherwise) => fail(otherwise)
    }
  }

  "Ledger API Server" should {
    "offer com.digitalasset.ledger.api.v1.ActiveContractsService" in {
      expectNotUnimplemented {
        val acs =
          ActiveContractsServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        acs.getActiveContracts(GetActiveContractsRequest(randomLedgerId)).toList
      }
    }

    "offer com.digitalasset.ledger.api.v1.CommandCompletionService" in {
      expectNotUnimplemented {
        val completion =
          CommandCompletionServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        completion.completionEnd(CompletionEndRequest(randomLedgerId))
      }
    }

    "offer com.digitalasset.ledger.api.v1.CommandService" in {
      expectNotUnimplemented {
        val command =
          CommandServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        command.submitAndWait(SubmitAndWaitRequest())
      }
    }

    "offer com.digitalasset.ledger.api.v1.CommandSubmissionService" in {
      expectNotUnimplemented {
        val submission =
          CommandSubmissionServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        submission.submit(SubmitRequest())
      }
    }

    "offer com.digitalasset.ledger.api.v1.LedgerConfigurationService" in {
      expectNotUnimplemented {
        val configuration = LedgerConfigurationServiceGrpc
          .blockingStub(channel)
          .withInterceptors(legacyCallInterceptor)
        configuration.getLedgerConfiguration(GetLedgerConfigurationRequest(randomLedgerId)).toList
      }
    }

    "offer com.digitalasset.ledger.api.v1.LedgerIdentityService" in {
      expectNotUnimplemented {
        val identity =
          LedgerIdentityServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        identity.getLedgerIdentity(GetLedgerIdentityRequest())
      }: @nowarn(
        "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
      )
    }

    "offer com.digitalasset.ledger.api.v1.PackageServiceService" in {
      expectNotUnimplemented {
        val pkg = PackageServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        pkg.listPackages(ListPackagesRequest(randomLedgerId))
      }
    }

    "offer com.digitalasset.ledger.api.v1.TransactionService" in {
      expectNotUnimplemented {
        val transaction =
          TransactionServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        transaction.getLedgerEnd(GetLedgerEndRequest(randomLedgerId))
      }
    }

    "offer com.digitalasset.ledger.api.v1.testing.TimeService" in {
      expectNotUnimplemented {
        val testingTime =
          TimeServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        testingTime.getTime(GetTimeRequest(randomLedgerId)).toList
      }
    }

    "offer com.digitalasset.ledger.api.v1.admin.ConfigManagementService" in {
      expectNotUnimplemented {
        val adminConfig =
          ConfigManagementServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        adminConfig.getTimeModel(GetTimeModelRequest())
      }
    }

    "offer com.digitalasset.ledger.api.v1.admin.PackageManagementService" in {
      expectNotUnimplemented {
        val adminPackage =
          PackageManagementServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        adminPackage.listKnownPackages(ListKnownPackagesRequest())
      }
    }

    "offer com.digitalasset.ledger.api.v1.admin.PartyManagementService" in {
      expectNotUnimplemented {
        val adminParty =
          PartyManagementServiceGrpc.blockingStub(channel).withInterceptors(legacyCallInterceptor)
        adminParty.listKnownParties(ListKnownPartiesRequest())
      }
    }

  }

}
