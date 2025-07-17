// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.participant.admin.party.PartyReplicationTestInterceptor.*
import com.digitalasset.canton.participant.protocol.party.{
  SourceParticipantStore,
  TargetParticipantStore,
}
import com.digitalasset.canton.tracing.TraceContext

/** OnPR test interceptor interface
  *
  * Only for use in integration tests. In fact the test interceptor is only configurable via
  * integration tests and disabled via pureconfig.
  */
trait PartyReplicationTestInterceptor {

  /** Specifies whether the SP proceeds or waits depending on the contents of the store.
    * @return
    *   Proceed to execute as normal or Wait to do nothing.
    */
  def onSourceParticipantProgress(store: SourceParticipantStore)(implicit
      traceContext: TraceContext
  ): ProceedOrWait

  /** Specifies whether the TP proceeds or waits depending on the contents of the store.
    * @return
    *   Proceed to execute as normal or Wait to do nothing.
    */
  def onTargetParticipantProgress(store: TargetParticipantStore)(implicit
      traceContext: TraceContext
  ): ProceedOrWait
}

object PartyReplicationTestInterceptor {
  sealed trait ProceedOrWait
  object Proceed extends ProceedOrWait
  object Wait extends ProceedOrWait

  /** In production, always proceed as any type of disruptive or stalling behavior is to be used in
    * integration tests only.
    */
  object AlwaysProceed extends PartyReplicationTestInterceptor {
    override def onSourceParticipantProgress(store: SourceParticipantStore)(implicit
        traceContext: TraceContext
    ): ProceedOrWait = Proceed
    override def onTargetParticipantProgress(store: TargetParticipantStore)(implicit
        traceContext: TraceContext
    ): ProceedOrWait = Proceed
  }
}
