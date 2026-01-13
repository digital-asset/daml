// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter

import scala.util.Try

trait HasSynchronizeWithClosing extends HasRunOnClosing {

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has finished or the `synchronizeWithClosingPatience`
    * has elapsed.
    *
    * DO NOT CALL `this.close` as part of `f`, because it will result in a deadlock. DO NOT PUT
    * retries, especially indefinite ones, inside `f`.
    *
    * @return
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run.
    * @see
    *   HasRunOnClosing.isClosing
    */
  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def synchronizeWithClosingSync[A](name: String)(f: => A)(implicit
      traceContext: TraceContext
  ): UnlessShutdown[A] =
    synchronizeWithClosingUS(name)(Try(f)).map(_.get)

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has completed (as defined by the
    * [[com.digitalasset.canton.util.Thereafter]] instance) or the `synchronizeWithClosingPatience`
    * has elapsed.
    *
    * DO NOT CALL `this.close` as part of `f`, because it will result in a deadlock. DO NOT PUT
    * retries, especially indefinite ones, inside `f`.
    *
    * @return
    *   The computation completes with
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run. Otherwise it is the result of running `f`.
    *
    * @see
    *   HasRunOnClosing.isClosing
    */
  def synchronizeWithClosing[F[_], A](name: String)(f: => F[A])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
      A: CanAbortDueToShutdown[F],
  ): F[A] = A.absorbOuter(synchronizeWithClosingUS(name)(f))

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has completed (as defined by the
    * [[com.digitalasset.canton.util.Thereafter]] instance) or the `synchronizeWithClosingPatience`
    * has elapsed.
    *
    * DO NOT CALL `this.close` as part of `f`, because it will result in a deadlock. DO NOT PUT
    * retries, especially indefinite ones, inside `f`.
    *
    * @return
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run. Otherwise the result of running `f`.
    * @see
    *   HasRunOnClosing.isClosing
    */
  def synchronizeWithClosingUS[F[_], A](name: String)(f: => F[A])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
  ): UnlessShutdown[F[A]]
}
