// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.ledger.client.binding.{Contract, Primitive as P}

import scala.collection.mutable
import scala.concurrent.Future

/** Simple store for tracking ledger contracts of a given type */
trait SimpleContractTrackerStore[A] {
  def list(): Future[Seq[Contract[A]]]
  def get(id: P.ContractId[A]): Future[Option[Contract[A]]]
  def add(item: Contract[A]): Future[Unit]
  def remove(id: P.ContractId[A]): Future[Unit]
}

class InMemorySimpleTrackerStore[A] extends SimpleContractTrackerStore[A] {
  protected val items: mutable.Map[P.ContractId[A], Contract[A]] =
    mutable.Map[P.ContractId[A], Contract[A]]()

  override def list(): Future[Seq[Contract[A]]] = Future.successful { items.values.toSeq }
  override def get(id: P.ContractId[A]): Future[Option[Contract[A]]] = Future.successful {
    items.get(id)
  }
  override def add(item: Contract[A]): Future[Unit] = Future.successful {
    val _ = items += (item.contractId -> item)
  }
  override def remove(id: P.ContractId[A]): Future[Unit] = Future.successful {
    val _ = items.remove(id)
  }
}
