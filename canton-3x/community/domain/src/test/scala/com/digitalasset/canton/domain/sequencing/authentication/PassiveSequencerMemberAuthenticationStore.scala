// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication

import com.digitalasset.canton.crypto.Nonce
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

class PassiveSequencerMemberAuthenticationStore extends MemberAuthenticationStore {
  override def saveNonce(storedNonce: StoredNonce)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.failed(new PassiveInstanceException("passive sequencer"))

  override def fetchAndRemoveNonce(member: Member, nonce: Nonce)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredNonce]] =
    Future.failed(new PassiveInstanceException("passive sequencer"))

  override def saveToken(token: StoredAuthenticationToken)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.failed(new PassiveInstanceException("passive sequencer"))

  override def fetchTokens(
      member: Member
  )(implicit traceContext: TraceContext): Future[Seq[StoredAuthenticationToken]] =
    Future.failed(new PassiveInstanceException("passive sequencer"))

  override def expireNoncesAndTokens(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.failed(new PassiveInstanceException("passive sequencer"))

  override def invalidateMember(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.failed(new PassiveInstanceException("passive sequencer"))

  override def close(): Unit = ()
}
