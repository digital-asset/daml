// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.daml.error.BaseError
import com.digitalasset.canton.health.ComponentHealthState.{Degraded, Failed, Fatal, Ok}
import com.digitalasset.canton.health.admin.v30 as proto
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.util.ShowUtil
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

import scala.annotation.nowarn

/** Generic State implementation of a component
  * This can be used as a base health state for most component.
  * However ComponentHealth (below) does not enforce the use of this class and a custom State class can be used instead
  */
sealed trait ComponentHealthState extends ToComponentHealthState with PrettyPrinting {
  def isOk: Boolean = this match {
    case ComponentHealthState.Ok(_) => true
    case _ => false
  }
  def isDegrading: Boolean = this match {
    case ComponentHealthState.Degraded(_) => true
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
  override def toComponentHealthState: ComponentHealthState = this
  override def pretty: Pretty[ComponentHealthState] =
    ComponentHealthState.prettyComponentHealthState

  def toComponentStatusV0: proto.StatusResponse.ComponentStatus.Status = this match {
    case Ok(description) =>
      proto.StatusResponse.ComponentStatus.Status
        .Ok(proto.StatusResponse.ComponentStatus.StatusData(description))
    case Degraded(degraded) =>
      proto.StatusResponse.ComponentStatus.Status.Degraded(degraded.toComponentStatusDataV0)
    case Failed(failed) =>
      proto.StatusResponse.ComponentStatus.Status.Failed(failed.toComponentStatusDataV0)
    case Fatal(fatal) =>
      proto.StatusResponse.ComponentStatus.Status.Fatal(fatal.toComponentStatusDataV0)
  }
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
  final case class Ok(description: Option[String] = None) extends ComponentHealthState

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
      with HasUnhealthyState

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
      with HasUnhealthyState

  object Failed {
    @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
    implicit val failedEncoder: Encoder[Failed] = deriveEncoder[Failed]
  }

  /** Used to indicate liveness problem, when the node should be restarted externally
    * @param state data
    */
  final case class Fatal(state: UnhealthyState = UnhealthyState())
      extends ComponentHealthState
      with HasUnhealthyState

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

    def toComponentStatusDataV0: proto.StatusResponse.ComponentStatus.StatusData =
      proto.StatusResponse.ComponentStatus.StatusData(Some(this.show))
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
