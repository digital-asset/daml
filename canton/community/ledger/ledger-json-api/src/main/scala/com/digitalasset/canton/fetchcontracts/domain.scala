// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.fetchcontracts

import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.daml.ledger.api.v1 as lav1
import com.daml.ledger.api.v2 as lav2
import com.daml.lf
import util.ClientUtil.boxedRecord
import com.daml.nonempty.NonEmpty
import scalaz.std.option.*
import scalaz.std.string.*
import scalaz.syntax.std.option.*
import scalaz.syntax.traverse.*
import scalaz.{@@, Applicative, Order, Semigroup, Show, Tag, Tags, Traverse, \/}

package object domain {
  type LfValue = lf.value.Value

  type ContractId = lar.ContractId
  val ContractId = lar.ContractId

  type Party = lar.Party
  val Party = lar.Party

  type PartySet = NonEmpty[Set[Party]]

  type Offset = String @@ OffsetTag

  implicit final class `fc domain ErrorOps`[A](private val o: Option[A]) extends AnyVal {
    def required(label: String): Error \/ A =
      o toRightDisjunction Error(Symbol("ErrorOps_required"), s"Missing required field $label")
  }
}

package domain {

  import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
  import com.digitalasset.canton.http.domain.{ContractTypeId, ResolvedQuery}
  import scalaz.\/-

  final case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"domain.Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  sealed trait OffsetTag

  object Offset {
    val tag = Tag.of[OffsetTag]

    def apply(s: String): Offset = tag(s)

    def unwrap(x: Offset): String = tag.unwrap(x)

    def fromLedgerApi(
        offset: ParticipantOffset
    ): Option[Offset] =
      offset.value.absolute.filter(_.nonEmpty).map(x => Offset(x))

    def fromLedgerApi(tx: lav2.transaction.Transaction): Offset = Offset(tx.offset)

    def toLedgerApi(o: Offset): lav2.participant_offset.ParticipantOffset =
      lav2.participant_offset.ParticipantOffset(
        lav2.participant_offset.ParticipantOffset.Value.Absolute(unwrap(o))
      )

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

    case object IgnoreInterface

    def fromLedgerApi[RQ, CtTyId](
        resolvedQuery: RQ,
        in: lav1.event.CreatedEvent,
    )(implicit RQ: ForQuery[RQ, CtTyId]): Error \/ ActiveContract[CtTyId, lav1.value.Value] = {
      type IdKeyPayload =
        (Error \/ CtTyId, Option[lav1.value.Value], Error \/ lav1.value.Record)

      def templateFallback = {
        val id = in.templateId.required("templateId").map(ContractTypeId.Template.fromLedgerApi)
        (id, in.contractKey, in.createArguments required "createArguments")
      }

      val (getId, key, getPayload): IdKeyPayload = RQ match {
        case ForQuery.Resolved =>
          resolvedQuery match {
            case ResolvedQuery.ByInterfaceId(interfaceId) =>
              import util.IdentifierConverters.apiIdentifier
              val id = apiIdentifier(interfaceId)
              val payload = in.interfaceViews
                .find(_.interfaceId.exists(_ == id))
                .flatMap(_.viewValue) required "interfaceView"
              (\/-(interfaceId: CtTyId), None, payload)
            case ResolvedQuery.ByTemplateId(_) | ResolvedQuery.ByTemplateIds(_) => templateFallback
          }
        case ForQuery.Tpl => templateFallback
      }

      for {
        id <- getId
        payload <- getPayload
      } yield ActiveContract(
        contractId = ContractId(in.contractId),
        templateId = id,
        key = key,
        payload = boxedRecord(payload),
        signatories = Party.subst(in.signatories),
        observers = Party.subst(in.observers),
        agreementText = in.agreementText getOrElse "",
      )
    }

    /** Either a [[com.digitalasset.canton.http.domain.ResolvedQuery]] or [[IgnoreInterface]]. Enables well-founded
      * overloading of `fromLedgerApi` on these contexts.
      */
    sealed abstract class ForQuery[-RQ, CtTyId] extends Product with Serializable
    object ForQuery {
      implicit case object Resolved extends ForQuery[ResolvedQuery, ContractTypeId.Resolved]
      implicit case object Tpl
          extends ForQuery[IgnoreInterface.type, ContractTypeId.Template.Resolved]
    }

    implicit def covariant[C]: Traverse[ActiveContract[C, *]] = new Traverse[ActiveContract[C, *]] {
      override def map[A, B](fa: ActiveContract[C, A])(f: A => B): ActiveContract[C, B] =
        fa.copy(key = fa.key map f, payload = f(fa.payload))

      override def traverseImpl[G[_]: Applicative, A, B](
          fa: ActiveContract[C, A]
      )(f: A => G[B]): G[ActiveContract[C, B]] = {
        import scalaz.syntax.apply.*
        val gk: G[Option[B]] = fa.key traverse f
        val ga: G[B] = f(fa.payload)
        ^(gk, ga)((k, a) => fa.copy(key = k, payload = a))
      }
    }
  }
}
