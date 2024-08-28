// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import com.daml.error.BaseError
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.admin.api.client.data.ComponentHealthState.{
  Degraded,
  Failed,
  Fatal,
  UnhealthyState,
}
import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.health.admin.v0 as protoV0
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ShowUtil
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

import scala.annotation.nowarn

final case class ComponentStatus(name: String, state: ComponentHealthState) extends PrettyPrinting {
  def toProtoV0: protoV0.NodeStatus.ComponentStatus =
    protoV0.NodeStatus.ComponentStatus(
      name = name,
      status = state.toComponentStatusV0,
    )

  override val pretty: Pretty[ComponentStatus] = ComponentStatus.componentStatusPretty
}

object ComponentStatus {
  def fromProtoV0(dependency: protoV0.NodeStatus.ComponentStatus): ParsingResult[ComponentStatus] =
    dependency.status match {
      case protoV0.NodeStatus.ComponentStatus.Status.Empty =>
        ProtoDeserializationError.FieldNotSet("ComponentStatus.staus").asLeft
      case protoV0.NodeStatus.ComponentStatus.Status.Ok(value) =>
        ComponentStatus(
          dependency.name,
          ComponentHealthState.Ok(value.description),
        ).asRight
      case protoV0.NodeStatus.ComponentStatus.Status.Degraded(value) =>
        ComponentStatus(
          dependency.name,
          Degraded(UnhealthyState(value.description)),
        ).asRight
      case protoV0.NodeStatus.ComponentStatus.Status.Failed(value) =>
        ComponentStatus(
          dependency.name,
          Failed(UnhealthyState(value.description)),
        ).asRight
      case protoV0.NodeStatus.ComponentStatus.Status.Fatal(value) =>
        ComponentStatus(
          dependency.name,
          Fatal(UnhealthyState(value.description)),
        ).asRight
    }

  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  implicit val componentStatusEncoder: Encoder[ComponentStatus] = deriveEncoder[ComponentStatus]

  implicit val componentStatusPretty: Pretty[ComponentStatus] = {
    import Pretty.*
    prettyInfix[ComponentStatus](_.name.unquoted, ":", _.state)
  }
}

/** Generic State implementation of a component
  * This can be used as a base health state for most component.
  * However ComponentHealth (below) does not enforce the use of this class and a custom State class can be used instead
  */
sealed trait ComponentHealthState extends PrettyPrinting {
  def isOk: Boolean = this match {
    case ComponentHealthState.Ok(_) => true
    case _ => false
  }
  def isFailed: Boolean = this match {
    case ComponentHealthState.Failed(_) => true
    case ComponentHealthState.Fatal(_) => true
    case _ => false
  }
  def isFatal: Boolean = this match {
    case ComponentHealthState.Fatal(_) => true
    case _ => false
  }
  def isDegrading: Boolean = this match {
    case ComponentHealthState.Degraded(_) => true
    case _ => false
  }
  override def pretty: Pretty[ComponentHealthState] =
    ComponentHealthState.prettyComponentHealthState

  def toComponentStatusV0: protoV0.NodeStatus.ComponentStatus.Status

}

object ComponentHealthState extends ShowUtil {
  import PrettyUtil.*

  val ShutdownState: ComponentHealthState =
    ComponentHealthState.failed("Component is closed")
  val NotInitializedState: ComponentHealthState =
    ComponentHealthState.failed("Not Initialized")

  // Json encoder implicits

  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  implicit val componentHealthStateEncoder: Encoder[ComponentHealthState] =
    deriveEncoder[ComponentHealthState]

  implicit val prettyComponentHealthState: Pretty[ComponentHealthState] = {
    case ok: Ok =>
      prettyOfClass[Ok](unnamedParamIfDefined(_.description.map(_.unquoted))).treeOf(ok)
    case notInitialized if notInitialized == NotInitializedState =>
      prettyOfString[ComponentHealthState](_ => "Not Initialized").treeOf(notInitialized)
    case unhealthy: HasUnhealthyState => HasUnhealthyState.prettyHasUnhealthyState.treeOf(unhealthy)
  }

  /** Ok state
    */
  final case class Ok(description: Option[String] = None) extends ComponentHealthState {
    override def toComponentStatusV0: v0.NodeStatus.ComponentStatus.Status =
      protoV0.NodeStatus.ComponentStatus.Status
        .Ok(protoV0.NodeStatus.ComponentStatus.StatusData(description))
  }

  object Ok {
    implicit val okEncoder: Encoder[Ok.type] = Encoder.encodeString.contramap(_ => "ok")
  }

  def failed(description: String): Failed = Failed(UnhealthyState(Some(description)))

  def degraded(description: String): Degraded = Degraded(UnhealthyState(Some(description)))

  def fatal(description: String): Fatal = Fatal(UnhealthyState(Some(description)))

  /** Degraded state, as in not fully but still functional. A degraded component will NOT cause a service
    * to report NOT_SERVING
    *
    * @param state data
    */
  final case class Degraded(state: UnhealthyState = UnhealthyState())
      extends ComponentHealthState
      with HasUnhealthyState {
    override def toComponentStatusV0: v0.NodeStatus.ComponentStatus.Status =
      protoV0.NodeStatus.ComponentStatus.Status.Degraded(state.toComponentStatusDataV0)
  }

  object Degraded {
    @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
    implicit val degradedEncoder: Encoder[Degraded] = deriveEncoder[Degraded]
  }

  /** The component has failed, any service that depends on it will report NOT_SERVING
    *
    * @param state data
    */
  final case class Failed(state: UnhealthyState = UnhealthyState())
      extends ComponentHealthState
      with HasUnhealthyState {
    override def toComponentStatusV0: v0.NodeStatus.ComponentStatus.Status =
      protoV0.NodeStatus.ComponentStatus.Status.Failed(state.toComponentStatusDataV0)
  }

  object Failed {
    @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
    implicit val failedEncoder: Encoder[Failed] = deriveEncoder[Failed]
  }

  /** Used to indicate liveness problem, when the node should be restarted externally
    * @param state data
    */
  final case class Fatal(state: UnhealthyState = UnhealthyState())
      extends ComponentHealthState
      with HasUnhealthyState {
    override def toComponentStatusV0: v0.NodeStatus.ComponentStatus.Status =
      protoV0.NodeStatus.ComponentStatus.Status.Fatal(state.toComponentStatusDataV0)
  }

  /** Unhealthy state data
    *
    * @param description description of the state
    * @param error       associated canton error
    */
  final case class UnhealthyState(
      description: Option[String] = None,
      error: Option[BaseError] = None,
      elc: Option[ErrorLoggingContext] = None,
  ) {
    val errorAsStringOpt: Option[String] = error.map { error =>
      s"${error.code.codeStr(elc.flatMap(_.traceContext.traceId))}: ${error.cause}"
    }

    def toComponentStatusDataV0: protoV0.NodeStatus.ComponentStatus.StatusData =
      protoV0.NodeStatus.ComponentStatus.StatusData(Some(this.show))

  }

  object UnhealthyState {
    implicit val unhealthyStateEncoder: Encoder[UnhealthyState] =
      Encoder.encodeString.contramap(_.show)
    implicit val prettyUnhealthyState: Pretty[UnhealthyState] = prettyOfString[UnhealthyState] {
      state =>
        s"${state.description.getOrElse("")}${state.errorAsStringOpt.map(e => s", error: $e").getOrElse("")}"
    }
  }

  object Unhealthy {
    def unapply(state: ComponentHealthState): Option[UnhealthyState] = state match {
      case _: Ok => None
      case Degraded(degraded) => Some(degraded)
      case Failed(failed) => Some(failed)
      case Fatal(fatal) => Some(fatal)
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