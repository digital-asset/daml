// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import com.digitalasset.base.error.BaseError
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.api.client.data.ComponentHealthState.{
  Degraded,
  Failed,
  Fatal,
  UnhealthyState,
}
import com.digitalasset.canton.admin.health.v30 as protoV30
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ShowUtil
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

final case class ComponentStatus(name: String, state: ComponentHealthState) extends PrettyPrinting {
  override val pretty: Pretty[ComponentStatus] = ComponentStatus.componentStatusPretty
}

object ComponentStatus {
  def fromProtoV30(
      dependency: protoV30.ComponentStatus
  ): ParsingResult[ComponentStatus] =
    dependency.status match {
      case protoV30.ComponentStatus.Status.Empty =>
        ProtoDeserializationError.FieldNotSet("ComponentStatus.staus").asLeft
      case protoV30.ComponentStatus.Status.Ok(value) =>
        ComponentStatus(
          dependency.name,
          ComponentHealthState.Ok(value.description),
        ).asRight
      case protoV30.ComponentStatus.Status.Degraded(value) =>
        ComponentStatus(
          dependency.name,
          Degraded(UnhealthyState(value.description)()),
        ).asRight
      case protoV30.ComponentStatus.Status.Failed(value) =>
        ComponentStatus(
          dependency.name,
          Failed(UnhealthyState(value.description)()),
        ).asRight
      case protoV30.ComponentStatus.Status.Fatal(value) =>
        ComponentStatus(
          dependency.name,
          Fatal(UnhealthyState(value.description)()),
        ).asRight
    }

  implicit val componentStatusEncoder: Encoder[ComponentStatus] = deriveEncoder[ComponentStatus]

  implicit val componentStatusPretty: Pretty[ComponentStatus] = {
    import Pretty.*
    prettyInfix[ComponentStatus](_.name.unquoted, ":", _.state)
  }
}

/** Generic State implementation of a component This can be used as a base health state for most
  * component. However ComponentHealth (below) does not enforce the use of this class and a custom
  * State class can be used instead
  */
sealed trait ComponentHealthState extends PrettyPrinting {
  override protected def pretty: Pretty[ComponentHealthState] =
    ComponentHealthState.prettyComponentHealthState
}

object ComponentHealthState extends ShowUtil {
  import PrettyUtil.*

  private val NotInitializedState: ComponentHealthState =
    ComponentHealthState.failed("Not Initialized")

  implicit val componentHealthStateEncoder: Encoder[ComponentHealthState] =
    deriveEncoder[ComponentHealthState]

  implicit val prettyComponentHealthState: Pretty[ComponentHealthState] = {
    case ok: Ok =>
      prettyOfClass[Ok](unnamedParamIfDefined(_.description.map(_.unquoted))).treeOf(ok)
    case notInitialized if notInitialized == NotInitializedState =>
      prettyOfString[ComponentHealthState](_ => "Not Initialized").treeOf(notInitialized)
    case unhealthy: HasUnhealthyState => HasUnhealthyState.prettyHasUnhealthyState.treeOf(unhealthy)
  }

  final case class Ok(description: Option[String] = None) extends ComponentHealthState {}

  object Ok {
    implicit val okEncoder: Encoder[Ok.type] = Encoder.encodeString.contramap(_ => "ok")
  }

  def failed(description: String): Failed = Failed(UnhealthyState(Some(description))())

  /** Degraded state, as in not fully but still functional. A degraded component will NOT cause a
    * service to report NOT_SERVING
    *
    * @param state
    *   data
    */
  final case class Degraded(state: UnhealthyState = UnhealthyState()())
      extends ComponentHealthState
      with HasUnhealthyState {}

  object Degraded {
    implicit val degradedEncoder: Encoder[Degraded] = deriveEncoder[Degraded]
  }

  /** The component has failed, any service that depends on it will report NOT_SERVING
    *
    * @param state
    *   data
    */
  final case class Failed(state: UnhealthyState = UnhealthyState()())
      extends ComponentHealthState
      with HasUnhealthyState {}

  object Failed {
    implicit val failedEncoder: Encoder[Failed] = deriveEncoder[Failed]
  }

  /** Used to indicate liveness problem, when the node should be restarted externally
    * @param state
    *   data
    */
  final case class Fatal(state: UnhealthyState = UnhealthyState()())
      extends ComponentHealthState
      with HasUnhealthyState {}

  /** Unhealthy state data
    *
    * @param description
    *   description of the state
    * @param error
    *   associated canton error
    */
  final case class UnhealthyState(
      description: Option[String] = None,
      error: Option[BaseError] = None,
  )(elc: Option[ErrorLoggingContext] = None) {
    val errorAsStringOpt: Option[String] = error.map { error =>
      s"${error.code.codeStr(elc.flatMap(_.traceContext.traceId))}: ${error.cause}"
    }
  }

  object UnhealthyState {
    implicit val unhealthyStateEncoder: Encoder[UnhealthyState] =
      Encoder.encodeString.contramap(_.show)
    implicit val prettyUnhealthyState: Pretty[UnhealthyState] = prettyOfString[UnhealthyState] {
      state =>
        s"${state.description.getOrElse("")}${state.errorAsStringOpt.map(e => s", error: $e").getOrElse("")}"
    }
  }

  object HasUnhealthyState {
    // Use a separate pretty instance for HasUnhealthyState objects to slim down the Tree structure and avoid
    // too many layers of wrapping
    implicit val prettyHasUnhealthyState: Pretty[HasUnhealthyState] =
      prettyOfClass[HasUnhealthyState](
        unnamedParamIfDefined(_.state.description.map(_.unquoted)),
        paramIfDefined("error", _.state.errorAsStringOpt.map(_.unquoted)),
      )
  }
  trait HasUnhealthyState { def state: UnhealthyState }
}
