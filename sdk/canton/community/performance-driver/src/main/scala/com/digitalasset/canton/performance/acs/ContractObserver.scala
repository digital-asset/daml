// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.acs

import com.daml.ledger.api.v2.event.CreatedEvent.toJavaProto
import com.daml.ledger.api.v2.event.{ArchivedEvent, CreatedEvent}
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.{Contract, ContractCompanion, ContractId}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

trait ContractObserver {
  def reset(): Unit
  def processCreate(create: CreatedEvent): Boolean
  def processArchive(archive: ArchivedEvent): Boolean
}

/** Basic observer for a contract of a particular template.
  *
  * A contract observer which will filter out create / archive events of a particular template and
  * present the decoded version of that template to the typed create and archive functions.
  *
  * Is meant to be used for java-codegen generated classes.
  */
abstract class BaseContractObserver[TC <: Contract[TCid, T], TCid <: ContractId[T], T](
    companion: ContractCompanion[TC, TCid, T]
) extends ContractObserver {

  def processCreate(create: CreatedEvent): Boolean = {
    val dc = JavaDecodeUtil
      .decodeCreated[TC](companion)(javaapi.data.CreatedEvent.fromProto(toJavaProto(create)))
    dc.foreach(processCreate_)
    dc.isDefined
  }

  protected def processCreate_(create: TC): Unit

  def processArchive(archive: ArchivedEvent): Boolean = {
    val dc = JavaDecodeUtil
      .decodeArchived[T](companion)(
        javaapi.data.ArchivedEvent.fromProto(ArchivedEvent.toJavaProto(archive))
      )
    dc.foreach(processArchive_)
    dc.isDefined
  }

  protected def processArchive_(archive: ContractId[T]): Unit

}
