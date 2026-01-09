// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Nonce
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PriorityBlockingQueueUtil
import com.google.common.annotations.VisibleForTesting

import java.time.Duration
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

sealed trait HasExpiry {
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

class MemberAuthenticationStore(
    maxItemsPerMember: PositiveInt,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private implicit val hasExpiryOrdering: Ordering[HasExpiry] = Ordering.by(_.expireAt)
  private val nonces = new AtomicReference[Map[Member, List[StoredNonce]]](Map.empty)
  // note we only need to remember the tokens by member so we can invalidate them on request
  private val tokens = new TrieMap[Member, List[StoredAuthenticationToken]]()
  private val tokenLookup = new TrieMap[AuthenticationToken, StoredAuthenticationToken]()
  private val expiryQueue = new PriorityBlockingQueue[HasExpiry](
    PriorityBlockingQueueUtil.DefaultInitialCapacity,
    Ordering[HasExpiry],
  )

  def saveNonce(nonce: StoredNonce)(implicit traceContext: TraceContext): Unit = {
    nonces
      .getAndUpdate(_.updatedWith(nonce.member) {
        case Some(nonces) =>
          Some(nonce :: nonces.take(maxItemsPerMember.value - 1))
        case None => Some(List(nonce))
      })
      .get(nonce.member)
      .foreach { previously =>
        if (previously.sizeIs > maxItemsPerMember.value - 1) {
          // leave a hint in case anyone will ever hit this.
          logger.info(
            s"Dropping excess nonce for ${nonce.member} as max per member is ${maxItemsPerMember.value}"
          )
        }
      }
    // fine to add subsequently. we only need this to avoid doing a full-pass
    // over all tokens when we clean them up, so we eventually remove older tokens from memory
    expiryQueue.put(nonce)
  }

  private def noneIfEmpty[T](lst: List[T]): Option[List[T]] =
    Option.when(lst.nonEmpty)(lst)

  def fetchAndRemoveNonce(member: Member, nonce: Nonce): Option[StoredNonce] = {
    val cur = nonces.getAndUpdate(_.updatedWith(member) {
      case Some(nonces) =>
        noneIfEmpty(nonces.filter(_.nonce != nonce))
      case None => None
    })
    cur.get(member).flatMap(_.find(_.nonce == nonce))
  }

  def saveToken(token: StoredAuthenticationToken)(implicit traceContext: TraceContext): Unit = {
    tokens
      .updateWith(token.member) {
        case Some(tokens) =>
          // remove excess tokens
          // this is fine if called multiple times as it will subsequently just be a no-op
          if (tokens.sizeIs > maxItemsPerMember.value - 1)
            tokens.drop(maxItemsPerMember.value - 1).foreach { stored =>
              tokenLookup.remove(stored.token).discard
            }
          val limitedTokens = tokens.take(maxItemsPerMember.value - 1)
          if (tokens.sizeIs > maxItemsPerMember.value - 1) {
            logger.info(
              s"Dropping excess auth token for ${token.member} as max per member is ${maxItemsPerMember.value}"
            )
          }
          Some(token :: limitedTokens)
        case None => Some(List(token))
      }
      .discard
    // this is fine racy wise. we only need this entry to invalidate the member
    tokenLookup.put(token.token, token).discard
    // same as above, we only need to remember the member for which we need to schedule a cleanup
    // by adding the token to the queue after it was added to the data structures, we'll make sure
    // that they get cleaned up when the cleanup process picks up
    expiryQueue.put(token)
  }

  @VisibleForTesting
  def fetchTokens(member: Member): Seq[StoredAuthenticationToken] =
    tokens.getOrElse(member, List.empty)

  def tokenForMemberAt(
      member: Member,
      token: AuthenticationToken,
      timestamp: CantonTimestamp,
  ): Option[StoredAuthenticationToken] =
    tokenLookup.get(token).filter(stored => stored.member == member && stored.expireAt > timestamp)

  def fetchMemberOfTokenForInvalidation(token: AuthenticationToken): Option[Member] =
    tokenLookup.get(token).map(_.member)

  def expireNoncesAndTokens(timestamp: CantonTimestamp): Unit = {
    // figure out which members need clean up
    val members = mutable.HashSet[Member]()
    @tailrec
    def go(): Unit = if (!expiryQueue.isEmpty && expiryQueue.peek().expireAt <= timestamp) {
      expiryQueue.poll() match {
        case StoredNonce(member, _, _, _) => members.add(member).discard
        case StoredAuthenticationToken(member, _, _) => members.add(member).discard
      }
      go()
    }
    go()
    members.foreach { member =>
      nonces
        .updateAndGet(_.updatedWith(member) {
          // keep the nonces that expire in the future
          case Some(nonces) =>
            noneIfEmpty(nonces.filter(_.expireAt > timestamp))
          case None => None
        })
        .discard
      // iterate over the tokens which will expire and remove them from the lookup
      tokens.getOrElse(member, List.empty).filterNot(_.expireAt > timestamp).foreach { toRemove =>
        tokenLookup.remove(toRemove.token).discard
      }
      tokens
        .updateWith(member) {
          // keep tokens that expire in the future
          case Some(tokens) => noneIfEmpty(tokens.filter(_.expireAt > timestamp))
          case None => None
        }
        .discard
    }
  }

  def invalidateMember(member: Member): Unit = {
    nonces.getAndUpdate(_.removed(member)).discard
    // this is fine racy wise as the auth token itself is unique
    // while at the same time, the tokenLookup use always makes the self-consistency check
    tokens.remove(member).foreach(_.foreach(stored => tokenLookup.remove(stored.token).discard))
  }
}
