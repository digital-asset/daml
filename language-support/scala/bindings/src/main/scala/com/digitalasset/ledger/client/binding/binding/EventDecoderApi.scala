// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import com.digitalasset.ledger.api.refinements.ApiTypes

import scala.collection.immutable.{Map, Seq}
import scalaz.Id.Id
import com.digitalasset.ledger.api.v1.{event => rpcevent, value => rpcvalue}

abstract class EventDecoderApi(
    rawUnsupportedTemplates: Map[String, (String, String)],
    val templateTypes: Seq[TemplateCompanion[_]]) {
  val unsupportedTemplates: Map[String, Primitive.TemplateId[_]] = {
    type K[A] = Map[String, A]
    Primitive.TemplateId.substEx[K](rawUnsupportedTemplates transform {
      case (_, (packageId, s)) =>
        rpcvalue.Identifier(packageId, s)
    })
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  val decoderTable: Map[ApiTypes.TemplateId, rpcevent.CreatedEvent => Option[Template[_]]] =
    templateTypes.map(_.decoderEntry).toMap

  private[this] val dtl = {
    type F[A] = A => Option[rpcevent.CreatedEvent => Option[Template[_]]]
    ApiTypes.TemplateId.unsubst[F, rpcvalue.Identifier](decoderTable.lift)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  final def createdEventToContractRef(
      createdEvent: rpcevent.CreatedEvent): Either[EventDecoderError, Contract.OfAny] = {
    for {
      templateToContract <- createdEvent.templateId flatMap dtl toRight DecoderTableLookupFailure
      tadt <- templateToContract(createdEvent).toRight(
        CreateEventToContractMappingError: EventDecoderError)
    } yield
      Contract(
        Primitive.substContractId[Id, Nothing](ApiTypes.ContractId(createdEvent.contractId)),
        tadt,
        createdEvent.agreementText,
        createdEvent.signatories,
        createdEvent.observers
      )
  }
}
