// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import cats.implicits.catsSyntaxEitherId
import com.digitalasset.canton.*
import com.digitalasset.canton.admin.health.v30 as proto
import com.digitalasset.canton.health.ComponentHealthState.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

/** Simple representation of the health state of a component, easily (de)serializable (from)to protobuf or JSON
  */
final case class ComponentStatus(name: String, state: ComponentHealthState) extends PrettyPrinting {
  def toProtoV30: proto.ComponentStatus =
    proto.ComponentStatus(
      name = name,
      status = state.toComponentStatusV0,
    )

  override val pretty: Pretty[ComponentStatus] = ComponentStatus.componentStatusPretty
}

object ComponentStatus {
  def fromProtoV30(
      dependency: proto.ComponentStatus
  ): ParsingResult[ComponentStatus] =
    dependency.status match {
      case proto.ComponentStatus.Status.Ok(value) =>
        ComponentStatus(
          dependency.name,
          ComponentHealthState.Ok(value.description),
        ).asRight
      case proto.ComponentStatus.Status
            .Degraded(value: proto.ComponentStatus.StatusData) =>
        ComponentStatus(
          dependency.name,
          Degraded(UnhealthyState(value.description)),
        ).asRight
      case proto.ComponentStatus.Status.Failed(value) =>
        ComponentStatus(
          dependency.name,
          Failed(UnhealthyState(value.description)),
        ).asRight
      case _ =>
        ProtoDeserializationError.UnrecognizedField("Unknown state").asLeft
    }

  implicit val componentStatusEncoder: Encoder[ComponentStatus] = deriveEncoder[ComponentStatus]

  implicit val componentStatusPretty: Pretty[ComponentStatus] = {
    import Pretty.*
    prettyInfix[ComponentStatus](_.name.unquoted, ":", _.state)
  }
}
