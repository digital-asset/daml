// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts

import com.daml.lf
import lf.data.Ref
import util.ClientUtil.boxedRecord
import com.daml.ledger.api.{v1 => lav1}
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.nonempty.NonEmpty
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{@@, Applicative, Order, Semigroup, Show, Tag, Tags, Traverse, \/}

package object domain {
  type LfValue = lf.value.Value

  type ContractIdTag = lar.ContractIdTag
  type ContractId = lar.ContractId
  val ContractId = lar.ContractId

  type PartyTag = lar.PartyTag
  type Party = lar.Party
  val Party = lar.Party

  type PartySet = NonEmpty[Set[Party]]

  type Offset = String @@ OffsetTag

  private[daml] implicit final class `fc domain ErrorOps`[A](private val o: Option[A])
      extends AnyVal {
    def required(label: String): Error \/ A =
      o toRightDisjunction Error(Symbol("ErrorOps_required"), s"Missing required field $label")
  }
}

package domain {
  final case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"domain.Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

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

    implicit val semigroup: Semigroup[Offset] = Tag.unsubst(Semigroup[Offset @@ Tags.LastVal])
    implicit val `Offset ordering`: Order[Offset] = Order.orderBy[Offset, String](Offset.unwrap(_))
  }

  // TODO #14067 make these separate types
  object ContractTypeId {
    // TODO #14067 Resolved makes the semantics a little tricky; we would like
    // to prove that Template and Interface completely partition the
    // Resolved[Unknown[_]] type, which is not true of the unadorned unresolved
    // contract type IDs
    type Unknown[+PkgId] = TemplateId[PkgId]

    /** A contract type ID known to be a template, not an interface. */
    type Template[+PkgId] = TemplateId[PkgId] // <: Unknown
    /** A contract type ID known to be an interface, not a template. */
    type Interface[+PkgId] = TemplateId[PkgId] // <: Unknown
    // TODO #14067 write these like this instead:
    // object Unknown extends Like[Unknown]
    val Unknown = TemplateId
    val Template = TemplateId
    val Interface = TemplateId

    // TODO #14067 make an opaque subtype, produced by PackageService on
    // confirmed-present IDs only
    /** A resolved [[ContractTypeId]], typed `CtTyId`. */
    type Resolved[+CtTyId] = CtTyId

    /** A contract type ID companion. */
    sealed abstract class Like[CtId[T] <: Unknown[T]] {
      type OptionalPkg = CtId[Option[String]]
      type RequiredPkg = CtId[String]
      type NoPkg = CtId[Unit]
      type Resolved = ContractTypeId.Resolved[RequiredPkg]

      // treat the companion like a typeclass instance
      implicit def `ContractTypeId.Like companion`: this.type = this

      def apply[PkgId](packageId: PkgId, moduleName: String, entityName: String): CtId[PkgId]

      private[this] def qualifiedName(a: CtId[_]): Ref.QualifiedName =
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
  }

  final case class TemplateId[+PkgId](packageId: PkgId, moduleName: String, entityName: String)

  object TemplateId extends ContractTypeId.Like[TemplateId] {
    def fromLedgerApi(in: lav1.value.Identifier): TemplateId.RequiredPkg =
      TemplateId(in.packageId, in.moduleName, in.entityName)
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

  // This allows us to avoid rewriting all the imports and references
  // to http.domain.  We can snap the indirections and remove these
  // as convenient
  private[daml] trait Aliases {
    import com.daml.fetchcontracts.{domain => here}
    type Error = here.Error
    final val Error = here.Error
    type LfValue = here.LfValue
    final val ContractTypeId = here.ContractTypeId
    type TemplateId[+PkgId] = here.TemplateId[PkgId]
    final val TemplateId = here.TemplateId
    type ContractId = here.ContractId
    final val ContractId = here.ContractId
    type Party = here.Party
    final val Party = here.Party
    type PartySet = here.PartySet
    type Offset = here.Offset
    final val Offset = here.Offset
    type ActiveContract[+LfV] = here.ActiveContract[LfV]
    final val ActiveContract = here.ActiveContract
  }
}
