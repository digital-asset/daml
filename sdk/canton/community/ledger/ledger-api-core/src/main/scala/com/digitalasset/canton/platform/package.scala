// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Type aliases used throughout the package */
package object platform {
  import com.digitalasset.daml.lf.value.Value as lfval
  private[platform] type ContractId = lfval.ContractId
  private[platform] val ContractId = com.digitalasset.daml.lf.value.Value.ContractId
  private[platform] type Value = lfval.VersionedValue
  private[platform] type Contract = lfval.VersionedContractInstance
  private[platform] val Contract = lfval.VersionedContractInstance

  import com.digitalasset.daml.lf.transaction as lftx
  private[platform] type NodeId = lftx.NodeId
  private[platform] type Node = lftx.Node
  private[platform] type Create = lftx.Node.Create
  private[platform] type Exercise = lftx.Node.Exercise
  private[platform] type Key = lftx.GlobalKey
  private[platform] val Key = lftx.GlobalKey

  import com.digitalasset.daml.lf.data as lfdata
  private[platform] type Party = lfdata.Ref.Party
  private[platform] val Party = lfdata.Ref.Party
  private[platform] type Identifier = lfdata.Ref.Identifier
  private[platform] val Identifier = lfdata.Ref.Identifier
  private[platform] type QualifiedName = lfdata.Ref.QualifiedName
  private[platform] val QualifiedName = lfdata.Ref.QualifiedName
  private[platform] type DottedName = lfdata.Ref.DottedName
  private[platform] val DottedName = lfdata.Ref.DottedName
  private[platform] type ModuleName = lfdata.Ref.ModuleName
  private[platform] val ModuleName = lfdata.Ref.ModuleName
  private[platform] type LedgerString = lfdata.Ref.LedgerString
  private[platform] val LedgerString = lfdata.Ref.LedgerString
  private[platform] type UpdateId = lfdata.Ref.LedgerString
  private[platform] val UpdateId = lfdata.Ref.LedgerString
  private[platform] type WorkflowId = lfdata.Ref.LedgerString
  private[platform] val WorkflowId = lfdata.Ref.LedgerString
  private[platform] type SubmissionId = lfdata.Ref.SubmissionId
  private[platform] val SubmissionId = lfdata.Ref.SubmissionId
  private[platform] type ApplicationId = lfdata.Ref.ApplicationId
  private[platform] val ApplicationId = lfdata.Ref.ApplicationId
  private[platform] type CommandId = lfdata.Ref.CommandId
  private[platform] val CommandId = lfdata.Ref.CommandId
  private[platform] type ParticipantId = lfdata.Ref.ParticipantId
  private[platform] val ParticipantId = lfdata.Ref.ParticipantId
  private[platform] type ChoiceName = lfdata.Ref.ChoiceName
  private[platform] val ChoiceName = lfdata.Ref.ChoiceName
  private[platform] type PackageId = lfdata.Ref.PackageId
  private[platform] val PackageId = lfdata.Ref.PackageId
  private[platform] type PackageName = lfdata.Ref.PackageName
  private[platform] val PackageName = lfdata.Ref.PackageName
  private[platform] type Relation[A, B] = lfdata.Relation[A, B]
  private[platform] type UserId = lfdata.Ref.UserId
  private[platform] val UserId = lfdata.Ref.UserId

  import com.digitalasset.daml.lf.crypto
  private[platform] type Hash = crypto.Hash

  private[platform] type PruneBuffers = Offset => Unit

  implicit class ResourceOwnerOps[T](val resourceOwner: ResourceOwner[T]) extends AnyVal {
    def afterReleased(body: => Unit): ResourceOwner[T] =
      afterReleasedF(Future.successful(body))

    def afterReleasedF(bodyF: => Future[Unit]): ResourceOwner[T] =
      for {
        _ <- ResourceOwner.forReleasable(() => ())(_ => bodyF)
        t <- resourceOwner
      } yield t
  }

  implicit class ResourceOwnerFlagCloseableOps[T <: ResourceCloseable](
      val resourceOwner: ResourceOwner[T]
  ) extends AnyVal {
    def acquireFlagCloseable(
        name: String
    )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Future[T] = {
      val resource = resourceOwner.acquire()(ResourceContext(executionContext))
      resource.asFuture.map(
        _.registerResource(resource, name)
      )
    }
  }
}
