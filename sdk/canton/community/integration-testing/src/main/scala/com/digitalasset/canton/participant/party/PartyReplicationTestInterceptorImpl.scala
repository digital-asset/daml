// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.party

import com.digitalasset.canton.participant.admin.party.PartyReplicationTestInterceptor
import com.digitalasset.canton.participant.protocol.party.{
  SourceParticipantStore,
  TargetParticipantStore,
}
import com.digitalasset.canton.tracing.TraceContext

class PartyReplicationTestInterceptorImpl extends PartyReplicationTestInterceptor {

  override def onSourceParticipantProgress(store: SourceParticipantStore)(implicit
      traceContext: TraceContext
  ): PartyReplicationTestInterceptor.ProceedOrWait = PartyReplicationTestInterceptor.Proceed

  override def onTargetParticipantProgress(store: TargetParticipantStore)(implicit
      traceContext: TraceContext
  ): PartyReplicationTestInterceptor.ProceedOrWait = PartyReplicationTestInterceptor.Proceed
}

object PartyReplicationTestInterceptorImpl {

  /** Create a test interceptor that proceeds iff the given condition is true (based on the contents
    * of the source participant store).
    */
  def sourceParticipantProceedsIf(
      canProceed: SourceParticipantStore => Boolean
  ): PartyReplicationTestInterceptorImpl =
    new PartyReplicationTestInterceptorImpl {
      override def onSourceParticipantProgress(store: SourceParticipantStore)(implicit
          traceContext: TraceContext
      ): PartyReplicationTestInterceptor.ProceedOrWait = proceedIf(canProceed(store))
    }

  /** Create a test interceptor that proceeds iff the given condition is true (based on the contents
    * of the target participant store).
    */
  def targetParticipantProceedsIf(
      canProceed: TargetParticipantStore => Boolean
  ): PartyReplicationTestInterceptorImpl =
    new PartyReplicationTestInterceptorImpl {
      override def onTargetParticipantProgress(store: TargetParticipantStore)(implicit
          traceContext: TraceContext
      ): PartyReplicationTestInterceptor.ProceedOrWait = proceedIf(canProceed(store))
    }

  private def proceedIf(canProceed: Boolean): PartyReplicationTestInterceptor.ProceedOrWait =
    if (canProceed) PartyReplicationTestInterceptor.Proceed
    else PartyReplicationTestInterceptor.Wait
}
