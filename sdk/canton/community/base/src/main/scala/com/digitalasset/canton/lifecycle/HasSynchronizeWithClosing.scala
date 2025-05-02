// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter

import scala.util.Try

trait HasSynchronizeWithClosing extends HasRunOnClosing {

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has finished or the
    * [[LifeCycleManager.synchronizeWithClosingPatience]] has elapsed.
    *
    * @return
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run.
    *
    * @see
    *   HasRunOnClosing.isClosing
    */
  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def synchronizeWithClosing[A](name: String)(f: => A)(implicit
      traceContext: TraceContext
  ): UnlessShutdown[A] =
    synchronizeWithClosingF(name)(Try(f)).map(_.get)

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has completed (as defined by the
    * [[com.digitalasset.canton.util.Thereafter]] instance) or the
    * [[LifeCycleManager.synchronizeWithClosingPatience]] has elapsed.
    *
    * @return
    *   The computation completes with
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run. Otherwise it is the result of running `f`.
    *
    * @see
    *   HasRunOnClosing.isClosing
    */
  def synchronizeWithClosingUSF[F[_], A](name: String)(f: => F[A])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
      A: CanAbortDueToShutdown[F],
  ): F[A] = A.absorbOuter(synchronizeWithClosingF(name)(f))

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has completed (as defined by the
    * [[com.digitalasset.canton.util.Thereafter]] instance) or the
    * [[LifeCycleManager.synchronizeWithClosingPatience]] has elapsed.
    *
    * @return
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run. Otherwise the result of running `f`.
    *
    * @see
    *   HasRunOnClosing.isClosing
    */
  def synchronizeWithClosingF[F[_], A](name: String)(f: => F[A])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
  ): UnlessShutdown[F[A]]
}
