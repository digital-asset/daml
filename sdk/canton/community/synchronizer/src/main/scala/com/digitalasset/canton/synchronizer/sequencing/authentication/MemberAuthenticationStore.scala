// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication

import com.digitalasset.canton.crypto.Nonce
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.topology.Member

import java.time.Duration
import scala.collection.mutable
import scala.concurrent.blocking

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
)

class MemberAuthenticationStore {
  // we use a variety of data access and modification patterns that aren't well suited to lockless datastructures
  // so as the numbers of items is typically always small just use a single coarse lock for all accessing and modifications
  // to nonces and tokens
  private val lock = new Object()
  private val nonces = mutable.Buffer[StoredNonce]()
  private val tokens = mutable.Buffer[StoredAuthenticationToken]()

  def saveNonce(storedNonce: StoredNonce): Unit =
    blocking(lock.synchronized {
      nonces += storedNonce
      ()
    })

  def fetchAndRemoveNonce(member: Member, nonce: Nonce): Option[StoredNonce] =
    blocking(lock.synchronized {
      val storedNonce = nonces.find(n => n.member == member && n.nonce == nonce)
      storedNonce.foreach(nonces.-=) // remove the nonce
      storedNonce
    })

  def saveToken(token: StoredAuthenticationToken): Unit =
    blocking(lock.synchronized {
      tokens += token
      ()
    })

  def fetchTokens(member: Member): Seq[StoredAuthenticationToken] =
    blocking(lock.synchronized {
      tokens.filter(_.member == member).toSeq
    })

  def fetchToken(token: AuthenticationToken): Option[StoredAuthenticationToken] =
    blocking {
      lock.synchronized {
        tokens.find(_.token == token)
      }
    }

  def expireNoncesAndTokens(timestamp: CantonTimestamp): Unit =
    blocking(lock.synchronized {
      nonces --= nonces.filter(_.expireAt <= timestamp)
      tokens --= tokens.filter(_.expireAt <= timestamp)
      ()
    })

  def invalidateMember(member: Member): Unit =
    blocking(lock.synchronized {
      nonces --= nonces.filter(_.member == member)
      tokens --= tokens.filter(_.member == member)
      ()
    })
}
