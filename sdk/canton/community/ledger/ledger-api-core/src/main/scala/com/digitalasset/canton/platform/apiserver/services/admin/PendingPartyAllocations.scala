// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.daml.lf.data.Ref

import scala.collection.concurrent.{Map, TrieMap}
import scala.concurrent.{ExecutionContext, Future}

class PendingPartyAllocations {

  private val pendingAllocations: Map[Ref.UserId, Int] = TrieMap.empty

  private def increment(user: Ref.UserId): Int =
    pendingAllocations
      .updateWith(user) {
        case None => Some(1)
        case Some(n) => Some(n + 1)
      }
      .fold(0)(identity)

  private def decrement(user: Ref.UserId): Int =
    pendingAllocations
      .updateWith(user) {
        case Some(n) if n > 1 => Some(n - 1)
        case _ => None
      }
      .fold(0)(identity)

  def withUser[T](
      user: Option[Ref.UserId]
  )(f: Int => Future[T])(implicit executor: ExecutionContext): Future[T] = user match {
    case None => f(0)
    case Some(userId) => f(increment(userId)).thereafter(_ => decrement(userId).discard)
  }
}
