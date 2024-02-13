// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import cats.implicits.catsSyntaxEitherId
import com.digitalasset.canton.*
import com.digitalasset.canton.health.ComponentHealthState.*
import com.digitalasset.canton.health.admin.v30 as proto
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

import scala.annotation.nowarn

/** Simple representation of the health state of a component, easily (de)serializable (from)to protobuf or JSON
  */
final case class ComponentStatus(name: String, state: ComponentHealthState) extends PrettyPrinting {
  def toProtoV30: proto.StatusResponse.ComponentStatus =
    proto.StatusResponse.ComponentStatus(
      name = name,
      status = state.toComponentStatusV0,
    )

  override val pretty: Pretty[ComponentStatus] = ComponentStatus.componentStatusPretty
}

object ComponentStatus {
  def fromProtoV30(
      dependency: proto.StatusResponse.ComponentStatus
  ): ParsingResult[ComponentStatus] =
    dependency.status match {
      case proto.StatusResponse.ComponentStatus.Status.Ok(value) =>
        ComponentStatus(
          dependency.name,
          ComponentHealthState.Ok(value.description),
        ).asRight
      case proto.StatusResponse.ComponentStatus.Status
            .Degraded(value: proto.StatusResponse.ComponentStatus.StatusData) =>
        ComponentStatus(
          dependency.name,
          Degraded(UnhealthyState(value.description)),
        ).asRight
      case proto.StatusResponse.ComponentStatus.Status.Failed(value) =>
        ComponentStatus(
          dependency.name,
          Failed(UnhealthyState(value.description)),
        ).asRight
      case _ =>
        ProtoDeserializationError.UnrecognizedField("Unknown state").asLeft
    }

  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  implicit val componentStatusEncoder: Encoder[ComponentStatus] = deriveEncoder[ComponentStatus]

  implicit val componentStatusPretty: Pretty[ComponentStatus] = {
    import Pretty.*
    prettyInfix[ComponentStatus](_.name.unquoted, ":", _.state)
  }
}
