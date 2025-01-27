// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.ledger.api.v2 as lav2
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.daml.lf
import scalaz.std.option.*
import scalaz.std.string.*
import scalaz.syntax.std.option.*
import scalaz.syntax.traverse.*
import scalaz.{@@, Applicative, Order, Semigroup, Show, Tag, Tags, Traverse, \/}

import fetchcontracts.util.ClientUtil.boxedRecord

package object fetchcontracts {
  type LfValue = lf.value.Value

  type ContractId = lar.ContractId
  val ContractId = lar.ContractId

  type Party = lar.Party
  val Party = lar.Party

  type PartySet = NonEmpty[Set[Party]]

  type Offset = String @@ OffsetTag

  implicit final class `fc ErrorOps`[A](private val o: Option[A]) extends AnyVal {
    def required(label: String): Error \/ A =
      o toRightDisjunction Error(Symbol("ErrorOps_required"), s"Missing required field $label")
  }
}

package fetchcontracts {

  import com.digitalasset.canton.data.Offset as CoreOffset
  import com.digitalasset.canton.http.{ContractTypeId, ResolvedQuery}
  import com.digitalasset.daml.lf.data.{Bytes, Ref}
  import com.google.protobuf.ByteString
  import scalaz.-\/

  import java.nio.{ByteBuffer, ByteOrder}

  final case class Error(id: Symbol, message: String)

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"fetchcontracts.Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  sealed trait OffsetTag

  object Offset {
    val tag = Tag.of[OffsetTag]

    // TODO(#22819) remove as json v1 is removed and offsets are no longer HexStrings
    def apply(s: String): Offset = tag(s)
    def apply(l: Long): Offset = tag(fromLong(l))

    def unwrap(x: Offset): String = tag.unwrap(x)

    def tryToLong(x: Offset): Long = java.lang.Long.parseUnsignedLong(unwrap(x), 16)

    def fromLedgerApi(tx: lav2.transaction.Transaction): Offset = Offset(tx.offset)

    implicit val semigroup: Semigroup[Offset] = Tag.unsubst(Semigroup[Offset @@ Tags.LastVal])
    implicit val `Offset ordering`: Order[Offset] = Order.orderBy[Offset, String](Offset.unwrap)

    // TODO(#22819) remove as json v1 is removed and offsets are no longer HexStrings
    private def fromOldHexString(s: Ref.HexString): Long = {
      val bytes = Bytes.fromHexString(s)
      // first byte was the version byte, so we must remove it
      ByteBuffer.wrap(bytes.toByteArray).getLong(1)
    }

    // TODO(#22819) remove as json v1 is removed and offsets are no longer HexStrings
    def assertFromStringToLong(s: String): Long =
      if (s.isEmpty) 0L
      else {
        val hex = Ref.HexString.assertFromString(s)
        fromOldHexString(hex)
      }

    // TODO(#22819) remove as json v1 is removed and offsets are no longer HexStrings
    private def toOldHexString(offset: Long): Ref.HexString = {
      val longBasedByteLength: Int = 9 // One byte for the version plus 8 bytes for Long
      val versionUpstreamOffsetsAsLong: Byte = 0

      com.digitalasset.daml.lf.data.Bytes
        .fromByteString(
          ByteString.copyFrom(
            ByteBuffer
              .allocate(longBasedByteLength)
              .order(ByteOrder.BIG_ENDIAN)
              .put(0, versionUpstreamOffsetsAsLong)
              .putLong(1, offset)
          )
        )
        .toHexString
    }

    // TODO(#22819) remove as json v1 is removed and offsets are no longer HexStrings
    def fromLong(l: Long): String =
      toOldHexString(CoreOffset.tryFromLong(l).unwrap)
  }

  final case class ActiveContract[+CtTyId, +LfV](
      contractId: ContractId,
      templateId: CtTyId,
      key: Option[LfV],
      payload: LfV,
      signatories: Seq[Party],
      observers: Seq[Party],
  )

  object ActiveContract {
    type ResolvedCtTyId[+LfV] = ActiveContract[ContractTypeId.ResolvedPkgId, LfV]

    def fromLedgerApi[CtTyId[T] <: ContractTypeId.Definite[T]](
        extractor: ExtractAs[CtTyId],
        in: lav2.event.CreatedEvent,
    ): Error \/ ActiveContract[ContractTypeId.ResolvedPkgIdOf[CtTyId], lav2.value.Value] =
      extractor.getIdKeyPayload(in).map { case (id, key, payload) =>
        ActiveContract(
          contractId = ContractId(in.contractId),
          templateId = id,
          key = key,
          payload = boxedRecord(payload),
          signatories = Party.subst(in.signatories),
          observers = Party.subst(in.observers),
        )
      }

    // Strategy for extracting data from the created event,
    // depending on the kind of thing we were expecting, i.e. template or interface view.
    sealed trait ExtractAs[+CtTyId[T] <: ContractTypeId.Definite[T]] {
      def getIdKeyPayload(in: lav2.event.CreatedEvent): ExtractAs.IdKeyPayload[CtTyId]
    }

    object ExtractAs {
      type IdKeyPayload[+CtId[_]] =
        Error \/ (ContractTypeId.ResolvedPkgIdOf[CtId], Option[lav2.value.Value], lav2.value.Record)

      def apply(id: ContractTypeId.Definite[_]): ExtractAs[ContractTypeId.Definite] = id match {
        case ContractTypeId.Interface(_, mod, entity) => ExtractAs.InterfaceView(mod, entity)
        case ContractTypeId.Template(_, _, _) => ExtractAs.Template
      }

      def apply(resolvedQuery: ResolvedQuery): ExtractAs[ContractTypeId.Definite] =
        resolvedQuery match {
          case ResolvedQuery.ByInterfaceId(intfId) =>
            ExtractAs.InterfaceView(intfId.original.moduleName, intfId.original.entityName)
          case ResolvedQuery.ByTemplateId(_) => ExtractAs.Template
          case ResolvedQuery.ByTemplateIds(_) => ExtractAs.Template
        }

      // For interfaces we need to find the correct view and extract the payload and id from that.
      final case class InterfaceView(module: String, entity: String)
          extends ExtractAs[ContractTypeId.Interface] {
        import com.google.rpc.Code
        import com.google.rpc.status.Status

        def getIdKeyPayload(in: lav2.event.CreatedEvent): IdKeyPayload[ContractTypeId.Interface] = {
          val view = in.interfaceViews.find(
            // We ignore the package id when matching views.
            // The search result should have already selected the correct packages,
            // and if the query used a package name, multiple different corresponding
            // package ids may be returned.
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
                    payload <- v.viewValue.required("interviewView")
                  } yield (ContractTypeId.Interface.fromLedgerApi(id), None, payload)
              }
          }
        }

        private def viewError(view: lav2.event.InterfaceView): Option[Status] =
          view.viewStatus.filter(_.code != Code.OK_VALUE)
      }

      // For templates we can get the data more directly.
      final case object Template extends ExtractAs[ContractTypeId.Template] {
        def getIdKeyPayload(
            in: lav2.event.CreatedEvent
        ): IdKeyPayload[ContractTypeId.Template] = for {
          id <- in.templateId.required("templateId")
          payload <- in.createArguments.required("createArguments")
        } yield (ContractTypeId.Template.fromLedgerApi(id), in.contractKey, payload)
      }
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
