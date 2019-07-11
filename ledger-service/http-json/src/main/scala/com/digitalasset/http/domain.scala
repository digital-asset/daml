// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import scalaz.std.tuple._
import scalaz.std.vector._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{Functor, \/}

object domain {
  type Error = String

  case class JwtPayload(ledgerId: lar.LedgerId, applicationId: lar.ApplicationId, party: lar.Party)

  case class TemplateId[+PkgId](packageId: PkgId, moduleName: String, entityName: String)

  case class ActiveContract[+LfV](
      contractId: String,
      templateId: TemplateId.RequiredPkg,
      key: Option[LfV],
      argument: LfV,
      witnessParties: Seq[String],
      agreementText: String)

  case class ContractLookupRequest[+LfV](
      ledgerId: Option[String],
      id: (TemplateId.OptionalPkg, LfV) \/ (Option[TemplateId.OptionalPkg], String))

  case class GetActiveContractsRequest(templateIds: Set[TemplateId.OptionalPkg])

  case class GetActiveContractsResponse[+LfV](
      offset: String,
      workflowId: Option[String],
      activeContracts: Seq[ActiveContract[LfV]])

  object TemplateId {
    type OptionalPkg = TemplateId[Option[String]]
    type RequiredPkg = TemplateId[String]
    type NoPkg = TemplateId[Unit]

    def fromLedgerApi(in: lav1.value.Identifier): TemplateId.RequiredPkg =
      TemplateId(in.packageId, in.moduleName, in.entityName)
  }

  object ActiveContract {
    def fromLedgerApi(in: lav1.event.CreatedEvent): Error \/ ActiveContract[lav1.value.Value] =
      for {
        templateId <- in.templateId required "templateId"
        argument <- in.createArguments required "createArguments"
        boxedArgument = lav1.value.Value(lav1.value.Value.Sum.Record(argument))
      } yield
        ActiveContract(
          contractId = in.contractId,
          templateId = TemplateId fromLedgerApi templateId,
          key = in.contractKey,
          argument = boxedArgument,
          witnessParties = in.witnessParties,
          agreementText = in.agreementText getOrElse ""
        )

    implicit val covariant: Functor[ActiveContract] = new Functor[ActiveContract] {
      override def map[A, B](fa: ActiveContract[A])(f: A => B) =
        fa.copy(key = fa.key map f, argument = f(fa.argument))
    }
  }

  object ContractLookupRequest {
    implicit val covariant: Functor[ContractLookupRequest] = new Functor[ContractLookupRequest] {
      override def map[A, B](fa: ContractLookupRequest[A])(f: A => B) =
        fa.copy(id = fa.id leftMap (_ map f))
    }
  }

  object GetActiveContractsResponse {
    def fromLedgerApi(in: lav1.active_contracts_service.GetActiveContractsResponse)
      : Error \/ GetActiveContractsResponse[lav1.value.Value] =
      for {
        activeContracts <- in.activeContracts.toVector traverseU (ActiveContract.fromLedgerApi(_))
      } yield
        GetActiveContractsResponse(
          offset = in.offset,
          workflowId = Some(in.workflowId) filter (_.nonEmpty),
          activeContracts = activeContracts)

    implicit val covariant: Functor[GetActiveContractsResponse] =
      new Functor[GetActiveContractsResponse] {
        override def map[A, B](fa: GetActiveContractsResponse[A])(f: A => B) =
          fa copy (activeContracts = fa.activeContracts map (_ map f))
      }
  }

  private[this] implicit final class ErrorOps[A](private val o: Option[A]) extends AnyVal {
    def required(label: String): Error \/ A = o toRightDisjunction s"Missing required field $label"
  }

  case class CreateCommand(
      workflowId: Option[lar.WorkflowId],
      commandId: Option[lar.CommandId],
      templateId: TemplateId.OptionalPkg,
      argument: Option[lav1.value.Record])

}
