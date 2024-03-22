// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Nonce
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.IdempotentInsert.insertIgnoringConflicts
import com.digitalasset.canton.resource.{DbStorage, DbStore, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import java.time.Duration
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

trait HasExpiry {
  val expireAt: CantonTimestamp
}

final case class StoredNonce(
    member: Member,
    nonce: Nonce,
    generatedAt: CantonTimestamp,
    expireAt: CantonTimestamp,
) extends HasExpiry

object StoredNonce {
  def apply(
      member: Member,
      nonce: Nonce,
      generatedAt: CantonTimestamp,
      expirationDuration: Duration,
  ): StoredNonce =
    StoredNonce(member, nonce, generatedAt, generatedAt.add(expirationDuration))
}
final case class StoredAuthenticationToken(
    member: Member,
    expireAt: CantonTimestamp,
    token: AuthenticationToken,
) extends HasExpiry

trait MemberAuthenticationStore extends AutoCloseable {

  /** Save the provided nonce */
  def saveNonce(storedNonce: StoredNonce)(implicit traceContext: TraceContext): Future[Unit]

  /** Fetch and if found immediately remove the nonce for the member and nonce provided. */
  def fetchAndRemoveNonce(member: Member, nonce: Nonce)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredNonce]]

  /** Save the provided authentication token */
  def saveToken(token: StoredAuthenticationToken)(implicit traceContext: TraceContext): Future[Unit]

  /** Fetch all saved tokens for the provided member */
  def fetchTokens(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Seq[StoredAuthenticationToken]]

  /** Expire all nonces and tokens up to and including the provided timestamp */
  def expireNoncesAndTokens(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Remove any token or nonce for participant. */
  def invalidateMember(member: Member)(implicit traceContext: TraceContext): Future[Unit]
}

object MemberAuthenticationStore {
  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      closeContext: CloseContext,
  )(implicit
      executionContext: ExecutionContext
  ): MemberAuthenticationStore =
    storage match {
      case _: MemoryStorage => new InMemoryMemberAuthenticationStore
      case dbStorage: DbStorage =>
        new DbMemberAuthenticationStore(dbStorage, timeouts, loggerFactory, closeContext)
    }
}

class InMemoryMemberAuthenticationStore extends MemberAuthenticationStore {
  // we use a variety of data access and modification patterns that aren't well suited to lockless datastructures
  // so as the numbers of items is typically always small just use a single coarse lock for all accessing and modifications
  // to nonces and tokens
  private val lock = new Object()
  private val nonces = mutable.Buffer[StoredNonce]()
  private val tokens = mutable.Buffer[StoredAuthenticationToken]()

  override def saveNonce(
      storedNonce: StoredNonce
  )(implicit traceContext: TraceContext): Future[Unit] = blocking {
    lock.synchronized {
      nonces += storedNonce
      Future.unit
    }
  }

  override def fetchAndRemoveNonce(member: Member, nonce: Nonce)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredNonce]] = blocking(lock.synchronized {
    val storedNonce = nonces.find(n => n.member == member && n.nonce == nonce)

    storedNonce.foreach(nonces.-=) // remove the nonce

    Future.successful(storedNonce)
  })

  override def saveToken(
      token: StoredAuthenticationToken
  )(implicit traceContext: TraceContext): Future[Unit] = blocking {
    lock.synchronized {
      tokens += token
      Future.unit
    }
  }

  override def fetchTokens(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Seq[StoredAuthenticationToken]] = blocking(lock.synchronized {
    val memberTokens = tokens.filter(_.member == member)

    Future.successful(memberTokens.toSeq)
  })

  override def expireNoncesAndTokens(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = blocking {
    lock.synchronized {
      nonces --= nonces.filter(_.expireAt <= timestamp)
      tokens --= tokens.filter(_.expireAt <= timestamp)
      Future.unit
    }
  }

  override def invalidateMember(member: Member)(implicit traceContext: TraceContext): Future[Unit] =
    blocking(lock.synchronized {
      nonces --= nonces.filter(_.member == member)
      tokens --= tokens.filter(_.member == member)
      Future.unit
    })

  override def close(): Unit = ()
}

class DbMemberAuthenticationStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    override implicit val closeContext: CloseContext,
)(implicit executionContext: ExecutionContext)
    extends MemberAuthenticationStore
    with DbStore {

  import storage.api.*
  import Member.DbStorageImplicits.*

  override def saveNonce(storedNonce: StoredNonce)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    storage.update_(
      // it is safe to use insertIgnoringConflicts here for saving the nonce as the only conflicts that are likely
      // to occur are DB retries from this call
      insertIgnoringConflicts(
        storage,
        "sequencer_authentication_nonces(nonce)",
        sql"""sequencer_authentication_nonces (nonce, member, generated_at_ts, expire_at_ts)
            values (${storedNonce.nonce}, ${storedNonce.member}, ${storedNonce.generatedAt}, ${storedNonce.expireAt})""",
      ),
      functionFullName,
    )

  override def fetchAndRemoveNonce(member: Member, nonce: Nonce)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredNonce]] =
    for {
      timestampsO <- storage.query(
        sql"""select generated_at_ts, expire_at_ts
             from sequencer_authentication_nonces
             where nonce = $nonce and member = $member
             """.as[(CantonTimestamp, CantonTimestamp)].headOption,
        s"$functionFullName:nonce-lookup",
      )
      storedNonceO = timestampsO.map { case (generatedAt, expireAt) =>
        StoredNonce(member, nonce, generatedAt, expireAt)
      }
      _ <- storedNonceO.fold(Future.unit)(_ =>
        storage.update_(
          sqlu"""delete from sequencer_authentication_nonces where nonce = $nonce and member = $member""",
          s"$functionFullName:nonce-removal",
        )
      )
    } yield storedNonceO

  override def saveToken(token: StoredAuthenticationToken)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    storage.update_(
      // tokens are also securely generated so conflicts are also unlikely and can be safely ignored
      insertIgnoringConflicts(
        storage,
        "sequencer_authentication_tokens(token)",
        sql"""sequencer_authentication_tokens (token, member, expire_at_ts)
            values (${token.token}, ${token.member}, ${token.expireAt})""",
      ),
      functionFullName,
    )

  override def fetchTokens(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Seq[StoredAuthenticationToken]] =
    storage.query(
      sql"""
        select token, expire_at_ts from sequencer_authentication_tokens where member = $member
       """
        .as[(AuthenticationToken, CantonTimestamp)]
        .map(_.map { case (token, expireAt) =>
          StoredAuthenticationToken(member, expireAt, token)
        }),
      functionFullName,
    )
  override def expireNoncesAndTokens(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    storage.update_(
      DBIO.seq(
        sqlu"delete from sequencer_authentication_nonces where expire_at_ts <= $timestamp",
        sqlu"delete from sequencer_authentication_tokens where expire_at_ts <= $timestamp",
      ),
      functionFullName,
    )

  override def invalidateMember(member: Member)(implicit traceContext: TraceContext): Future[Unit] =
    storage.update_(
      DBIO.seq(
        sqlu"delete from sequencer_authentication_nonces where member = $member",
        sqlu"delete from sequencer_authentication_tokens where member = $member",
      ),
      functionFullName,
    )
}
