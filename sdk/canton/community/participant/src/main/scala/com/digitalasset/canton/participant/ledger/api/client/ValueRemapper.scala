// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.value

/** Utilities for modifying ledger api values, e.g. to make them suitable for importing into canton:
  *
  * Contract ids: Importing a set of contracts often requires awareness of contract dependencies and references to
  *               ensure that when contract ids are modified that references are updates to reflect such modifications.
  *
  * Party ids: Contracts from non-canton daml ledgers are incompatible with canton party ids that contain additional
  *            information (fingerprint suffix). Remapping party ids enables updating embedded party id types to
  *            conform to the canton party id format.
  */
object ValueRemapper {

  /** Helper for CreatedEvents.
    */
  def convertEvent(remapContractId: String => String, mapPartyId: String => String)(
      createdEvent: CreatedEvent
  ): CreatedEvent = {
    createdEvent.copy(
      contractId = remapContractId(createdEvent.contractId),
      signatories = createdEvent.signatories.map(mapPartyId),
      observers = createdEvent.observers.map(mapPartyId),
      witnessParties = createdEvent.witnessParties.map(mapPartyId),
      createArguments =
        createdEvent.createArguments.map(remapRecord(remapContractId, mapPartyId, _)),
      contractKey = createdEvent.contractKey.map(remapValue(remapContractId, mapPartyId)),
    )
  }

  /** Helper specifically useful for CreatedEvents that contain arguments as a record.
    */
  def remapRecord(
      remapContractId: String => String,
      remapParty: String => String,
      record: value.Record,
  ): value.Record = {
    record match {
      case value.Record(id, fields) =>
        val remappedFields = fields.map { case value.RecordField(label, v) =>
          value.RecordField(label, v.map(remapValue(remapContractId, remapParty)))
        }
        value.Record(id, remappedFields)
    }
  }

  /** Helper for arbitrary ledger api values.
    */
  def remapValue(remapContractId: String => String, remapParty: String => String)(
      v: value.Value
  ): value.Value =
    value.Value(v.sum match {
      case value.Value.Sum.ContractId(cid) =>
        value.Value.Sum.ContractId(remapContractId(cid))
      case value.Value.Sum.Party(party) =>
        value.Value.Sum.Party(remapParty(party))
      case value.Value.Sum.Record(record) =>
        value.Value.Sum.Record(remapRecord(remapContractId, remapParty, record))
      case value.Value.Sum.List(value.List(seq)) =>
        value.Value.Sum.List(value.List(seq.map(remapValue(remapContractId, remapParty))))
      case value.Value.Sum.TextMap(value.TextMap(entries)) =>
        value.Value.Sum.TextMap(value.TextMap(entries.map { case value.TextMap.Entry(k, v) =>
          value.TextMap.Entry(k, v.map(remapValue(remapContractId, remapParty)))
        }))
      case value.Value.Sum.GenMap(value.GenMap(entries)) =>
        value.Value.Sum.GenMap(value.GenMap(entries.map { case value.GenMap.Entry(k, v) =>
          value.GenMap.Entry(k, v.map(remapValue(remapContractId, remapParty)))
        }))
      case value.Value.Sum.Variant(value.Variant(id, constructor, v)) =>
        value.Value.Sum.Variant(
          value.Variant(id, constructor, v.map(remapValue(remapContractId, remapParty)))
        )
      case value.Value.Sum.Optional(value.Optional(v)) =>
        value.Value.Sum.Optional(value.Optional(v.map(remapValue(remapContractId, remapParty))))
      case v => v
    })

}
