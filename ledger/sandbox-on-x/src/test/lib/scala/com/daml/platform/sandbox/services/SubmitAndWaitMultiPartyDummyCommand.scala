// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import java.util.UUID

import com.daml.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.daml.platform.sandbox.auth.ServiceCallAuthTests
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

trait SubmitAndWaitMultiPartyDummyCommand extends TestCommands { self: ServiceCallAuthTests =>

  protected def dummySubmitAndWaitRequest(
      party: String,
      actAs: Seq[String],
      readAs: Seq[String],
  ): SubmitAndWaitRequest =
    SubmitAndWaitRequest(
      dummyMultiPartyCommands(
        wrappedLedgerId,
        s"$serviceCallName-${UUID.randomUUID}",
        party,
        actAs,
        readAs,
      )
        .update(_.commands.applicationId := serviceCallName)
        .commands
    )

  private def service(token: Option[String]) =
    stub(CommandServiceGrpc.stub(channel), token)

  protected def submitAndWait(
      token: Option[String],
      party: String,
      actAs: Seq[String],
      readAs: Seq[String],
  ): Future[Empty] =
    for {
      _ <- uploadPackageFiles(packageFiles, channel, toHeader(adminTokenStandardJWT))
      _ <- service(token).submitAndWait(dummySubmitAndWaitRequest(party, actAs, readAs))
    } yield Empty()

  protected def submitAndWaitForTransaction(
      token: Option[String],
      party: String,
      actAs: Seq[String],
      readAs: Seq[String],
  ): Future[Empty] =
    for {
      _ <- uploadPackageFiles(packageFiles, channel, toHeader(adminTokenStandardJWT))
      _ <- service(token)
        .submitAndWaitForTransaction(dummySubmitAndWaitRequest(party, actAs, readAs))
    } yield Empty()

  protected def submitAndWaitForTransactionId(
      token: Option[String],
      party: String,
      actAs: Seq[String],
      readAs: Seq[String],
  ): Future[Empty] =
    for {
      _ <- uploadPackageFiles(packageFiles, channel, toHeader(adminTokenStandardJWT))
      _ <- service(token)
        .submitAndWaitForTransactionId(dummySubmitAndWaitRequest(party, actAs, readAs))
    } yield Empty()

  protected def submitAndWaitForTransactionTree(
      token: Option[String],
      party: String,
      actAs: Seq[String],
      readAs: Seq[String],
  ): Future[Empty] =
    for {
      _ <- uploadPackageFiles(packageFiles, channel, toHeader(adminTokenStandardJWT))
      _ <- service(token)
        .submitAndWaitForTransactionTree(dummySubmitAndWaitRequest(party, actAs, readAs))
    } yield Empty()

}
