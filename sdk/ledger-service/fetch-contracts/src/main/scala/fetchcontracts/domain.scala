// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts

import com.daml.lf
import com.google.rpc.Code
import com.google.rpc.status.Status
import util.ClientUtil.boxedRecord
import com.daml.ledger.api.{v1 => lav1}
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.nonempty.NonEmpty
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, @@, Applicative, Order, Semigroup, Show, Tag, Tags, Traverse, \/}

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

  final case class ActiveContract[+CtTyId, +LfV](
      contractId: ContractId,
      templateId: CtTyId,
      key: Option[LfV],
      payload: LfV,
      signatories: Seq[Party],
      observers: Seq[Party],
      agreementText: String,
  )

  object ActiveContract {
    type ResolvedCtTyId[+LfV] = ActiveContract[ContractTypeId.Resolved, LfV]

    def matchesKey(k: LfValue)(a: ResolvedCtTyId[LfValue]): Boolean =
      a.key.fold(false)(_ == k)

    case object IgnoreInterface

    def fromLedgerApi[RQ, CtTyId](
        resolvedQuery: RQ,
        gacr: lav1.active_contracts_service.GetActiveContractsResponse,
    )(implicit
        RQ: ForQuery[RQ, CtTyId]
    ): Error \/ List[ActiveContract[CtTyId, lav1.value.Value]] = {
      gacr.activeContracts.toList.traverse(fromLedgerApi(resolvedQuery, _))
    }

    sealed trait ExtractFrom[+CtTyId] {
      type IdKeyPayload[+T] = Error \/ (T, Option[lav1.value.Value], lav1.value.Record)
      def getIdKeyPayload(in: lav1.event.CreatedEvent): IdKeyPayload[CtTyId]
    }
    object ExtractFrom {
      def apply(id: ContractTypeId.Resolved): ExtractFrom[ContractTypeId.Resolved] = id match {
        case ContractTypeId.Interface(_, mod, entity) => ExtractFrom.InterfaceView(mod, entity)
        case ContractTypeId.Template(_, _, _) => ExtractFrom.Template
      }

      final case class InterfaceView(module: String, entity: String)
          extends ExtractFrom[ContractTypeId.Interface.Resolved] {
        def getIdKeyPayload(
            in: lav1.event.CreatedEvent
        ): IdKeyPayload[ContractTypeId.Interface.Resolved] = {
          val view = in.interfaceViews.find(
            _.interfaceId.exists(id => id.moduleName == module && id.entityName == entity)
          )
          view match {
            case None =>
              val msg = s"Missing view with id matching '$module:$entity' in $in"
              -\/(Error(Symbol("ErrorOps_view_missing"), msg))
            case Some(v) =>
              viewError(v) match {
                case Some(s) => -\/(Error(Symbol("ErrorOps_view_eval"), s.toString))
                case None =>
                  for {
                    id <- v.interfaceId.required("interfaceId")
                    payload <- v.viewValue required "interviewView"
                  } yield (ContractTypeId.Interface.fromLedgerApi(id), None, payload)
              }
          }
        }

        private def viewError(view: lav1.event.InterfaceView): Option[Status] = {
          view.viewStatus.filter(_.code != Code.OK_VALUE)
        }
      }

      final case object Template extends ExtractFrom[ContractTypeId.Template.Resolved] {
        def getIdKeyPayload(
            in: lav1.event.CreatedEvent
        ): IdKeyPayload[ContractTypeId.Template.Resolved] = for {
          id <- in.templateId.required("templateId").map(ContractTypeId.Template.fromLedgerApi)
          payload <- in.createArguments required "createArguments"
        } yield (id, in.contractKey, payload)
      }
    }

    def fromLedgerApi[RQ, CtTyId](
        resolvedQuery: RQ,
        in: lav1.event.CreatedEvent,
    )(implicit RQ: ForQuery[RQ, CtTyId]): Error \/ ActiveContract[CtTyId, lav1.value.Value] = {
      val extractor: ExtractFrom[CtTyId] = RQ.extractor(resolvedQuery)
      extractor.getIdKeyPayload(in).map { case (id, key, payload) =>
        ActiveContract(
          contractId = ContractId(in.contractId),
          templateId = id,
          key = key,
          payload = boxedRecord(payload),
          signatories = Party.subst(in.signatories),
          observers = Party.subst(in.observers),
          agreementText = in.agreementText getOrElse "",
        )
      }
    }

    /** Either a [[ResolvedQuery]] or [[IgnoreInterface]].  Enables well-founded
      * overloading of `fromLedgerApi` on these contexts.
      */
    sealed abstract class ForQuery[-RQ, CtTyId] extends Product with Serializable {
      def extractor(from: RQ): ExtractFrom[CtTyId]
    }
    object ForQuery {
      implicit case object Resolved
          extends ForQuery[ContractTypeId.Resolved, ContractTypeId.Resolved] {
        def extractor(id: ContractTypeId.Resolved) = ExtractFrom(id)
      }
      implicit case object Tpl
          extends ForQuery[IgnoreInterface.type, ContractTypeId.Template.Resolved] {
        def extractor(_i: IgnoreInterface.type) = ExtractFrom.Template
      }
    }

    implicit def covariant[C]: Traverse[ActiveContract[C, *]] = new Traverse[ActiveContract[C, *]] {
      override def map[A, B](fa: ActiveContract[C, A])(f: A => B): ActiveContract[C, B] =
        fa.copy(key = fa.key map f, payload = f(fa.payload))

      override def traverseImpl[G[_]: Applicative, A, B](
          fa: ActiveContract[C, A]
      )(f: A => G[B]): G[ActiveContract[C, B]] = {
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
    type ContractTypeId[+PkgId] = here.ContractTypeId[PkgId]
    final val ContractTypeId = here.ContractTypeId
    type ContractId = here.ContractId
    final val ContractId = here.ContractId
    type Party = here.Party
    final val Party = here.Party
    type PartySet = here.PartySet
    type Offset = here.Offset
    final val Offset = here.Offset
    type ActiveContract[+CtTyId, +LfV] = here.ActiveContract[CtTyId, LfV]
    final val ActiveContract = here.ActiveContract
    final val ResolvedQuery = here.ResolvedQuery
    type ResolvedQuery = here.ResolvedQuery
    final val PackageResolvedContractTypeId = here.PackageResolvedContractTypeId
    type PackageResolvedContractTypeId[+CtTyId] = here.PackageResolvedContractTypeId[CtTyId]
  }
}
