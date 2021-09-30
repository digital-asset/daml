// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import com.daml.lf
import com.daml.lf.data.Ref
import com.daml.lf.iface
import com.daml.http.util.ClientUtil.boxedRecord
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.{v1 => lav1}
import scalaz.Isomorphism.{<~>, IsoFunctorTemplate}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.vector._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{
  -\/,
  @@,
  Applicative,
  Bitraverse,
  NonEmptyList,
  OneAnd,
  Order,
  Semigroup,
  Show,
  Tag,
  Tags,
  Traverse,
  \/,
  \/-,
}
import spray.json.JsValue

import scala.annotation.tailrec

package object domain {
  type LfValue = lf.value.Value

  type ContractIdTag = lar.ContractIdTag
  type ContractId = lar.ContractId
  val ContractId = lar.ContractId

  type PartyTag = lar.PartyTag
  type Party = lar.Party
  val Party = lar.Party

  type Offset = String @@ OffsetTag
}

package domain {
  sealed trait OffsetTag

  object Offset {
    private[daml] val tag = Tag.of[OffsetTag]

    def apply(s: String): Offset = tag(s)

    def unwrap(x: Offset): String = tag.unwrap(x)

    def fromLedgerApi(
        gacr: lav1.active_contracts_service.GetActiveContractsResponse
    ): Option[Offset] =
      Option(gacr.offset).filter(_.nonEmpty).map(x => Offset(x))

    def fromLedgerApi(
        gler: lav1.transaction_service.GetLedgerEndResponse
    ): Option[Offset] =
      gler.offset.flatMap(_.value.absolute).filter(_.nonEmpty).map(x => Offset(x))

    def fromLedgerApi(tx: lav1.transaction.Transaction): Offset = Offset(tx.offset)

    def toLedgerApi(o: Offset): lav1.ledger_offset.LedgerOffset =
      lav1.ledger_offset.LedgerOffset(lav1.ledger_offset.LedgerOffset.Value.Absolute(unwrap(o)))

    /* TODO SC bring Terminates?
    def toTerminates(o: Offset): LedgerClientJwt.Terminates.AtAbsolute =
      LedgerClientJwt.Terminates.AtAbsolute(
        lav1.ledger_offset.LedgerOffset.Value.Absolute(unwrap(o))
      )
     */

    implicit val semigroup: Semigroup[Offset] = Tag.unsubst(Semigroup[Offset @@ Tags.LastVal])
    implicit val `Offset ordering`: Order[Offset] = Order.orderBy[Offset, String](Offset.unwrap(_))
  }

  final case class TemplateId[+PkgId](packageId: PkgId, moduleName: String, entityName: String)

  object TemplateId {
    type OptionalPkg = TemplateId[Option[String]]
    type RequiredPkg = TemplateId[String]
    type NoPkg = TemplateId[Unit]

    def fromLedgerApi(in: lav1.value.Identifier): TemplateId.RequiredPkg =
      TemplateId(in.packageId, in.moduleName, in.entityName)

    def qualifiedName(a: TemplateId[_]): Ref.QualifiedName =
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName),
      )

    def toLedgerApiValue(a: RequiredPkg): Ref.Identifier = {
      val qfName = qualifiedName(a)
      val packageId = Ref.PackageId.assertFromString(a.packageId)
      Ref.Identifier(packageId, qfName)
    }

  }

  final case class ActiveContract[+LfV](
      contractId: ContractId,
      templateId: TemplateId.RequiredPkg,
      key: Option[LfV],
      payload: LfV,
      signatories: Seq[Party],
      observers: Seq[Party],
      agreementText: String,
  )

  object ActiveContract {

    def matchesKey(k: LfValue)(a: domain.ActiveContract[LfValue]): Boolean =
      a.key.fold(false)(_ == k)

    def fromLedgerApi(
        gacr: lav1.active_contracts_service.GetActiveContractsResponse
    ): Error \/ List[ActiveContract[lav1.value.Value]] = {
      gacr.activeContracts.toList.traverse(fromLedgerApi(_))
    }

    def fromLedgerApi(in: lav1.event.CreatedEvent): Error \/ ActiveContract[lav1.value.Value] =
      for {
        templateId <- in.templateId required "templateId"
        payload <- in.createArguments required "createArguments"
      } yield ActiveContract(
        contractId = ContractId(in.contractId),
        templateId = TemplateId fromLedgerApi templateId,
        key = in.contractKey,
        payload = boxedRecord(payload),
        signatories = Party.subst(in.signatories),
        observers = Party.subst(in.observers),
        agreementText = in.agreementText getOrElse "",
      )

    implicit val covariant: Traverse[ActiveContract] = new Traverse[ActiveContract] {

      override def map[A, B](fa: ActiveContract[A])(f: A => B): ActiveContract[B] =
        fa.copy(key = fa.key map f, payload = f(fa.payload))

      override def traverseImpl[G[_]: Applicative, A, B](
          fa: ActiveContract[A]
      )(f: A => G[B]): G[ActiveContract[B]] = {
        import scalaz.syntax.apply._
        val gk: G[Option[B]] = fa.key traverse f
        val ga: G[B] = f(fa.payload)
        ^(gk, ga)((k, a) => fa.copy(key = k, payload = a))
      }
    }
  }

  private[daml] trait Aliases {
    import com.daml.fetchcontracts.{domain => here}
    type LfValue = here.LfValue
    type TemplateId[+PkgId] = here.TemplateId[PkgId]
    final val TemplateId = here.TemplateId
    type ContractId = here.ContractId
    final val ContractId = here.ContractId
    type Party = here.Party
    final val Party = here.Party
    type Offset = here.Offset
    final val Offset = here.Offset
    type ActiveContract[+LfV] = here.ActiveContract[LfV]
    final val ActiveContract = here.ActiveContract
  }
}
