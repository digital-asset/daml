// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.Monoid
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.{
  AuthorizedDecentralizedNamespaceDefinition,
  AuthorizedIdentifierDelegation,
  AuthorizedNamespaceDelegation,
}

import scala.collection.mutable

/** authorization data
  *
  * this type is returned by the authorization validator. it contains the series of transactions
  * that authorize a certain topology transaction.
  *
  * note that the order of the namespace delegation is in "authorization order".
  */
final case class AuthorizationChain(
    identifierDelegation: Seq[AuthorizedIdentifierDelegation],
    namespaceDelegations: Seq[AuthorizedNamespaceDelegation],
    decentralizedNamespaceDefinitions: Seq[AuthorizedDecentralizedNamespaceDefinition],
) {

  def addIdentifierDelegation(aid: AuthorizedIdentifierDelegation): AuthorizationChain =
    copy(identifierDelegation = identifierDelegation :+ aid)

  def merge(other: AuthorizationChain): AuthorizationChain = {
    AuthorizationChain(
      mergeUnique(this.identifierDelegation, other.identifierDelegation),
      mergeUnique(this.namespaceDelegations, other.namespaceDelegations),
      mergeUnique(this.decentralizedNamespaceDefinitions, other.decentralizedNamespaceDefinitions),
    )
  }

  private def mergeUnique[T](left: Seq[T], right: Seq[T]): Seq[T] = {
    mutable.LinkedHashSet.from(left).addAll(right).toSeq
  }

}

object AuthorizationChain {
  val empty = AuthorizationChain(Seq(), Seq(), Seq())

  implicit val monoid: Monoid[AuthorizationChain] = new Monoid[AuthorizationChain] {
    override def empty: AuthorizationChain = AuthorizationChain.empty

    override def combine(x: AuthorizationChain, y: AuthorizationChain): AuthorizationChain =
      x.merge(y)
  }
}
