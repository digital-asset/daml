// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.{blinding => proto}
import com.google.protobuf.ProtocolStringList

import scala.collection.JavaConverters._

object BlindingCoder {

  def decode(
      p: proto.Blindinginfo.BlindingInfo,
      nodeIdReader: String => Either[DecodeError, Transaction.NodeId],
  ): Either[DecodeError, BlindingInfo] = {

    val explicitDisclosure =
      p.getExplicitDisclosureList.asScala.map(n =>
        for {
          ni <- nodeIdReader(n.getNodeId)
          parties <- toPartySet(n.getPartiesList)
        } yield ni -> parties,
      )

    val implicitLocal =
      p.getLocalImplicitDisclosureList.asScala.map(n =>
        for {
          ni <- nodeIdReader(n.getNodeId)
          parties <- toPartySet(n.getPartiesList)
        } yield ni -> parties,
      )

    val globalDisclosure =
      p.getGlobalImplicitDisclosureList.asScala.map(n =>
        for {
          parties <- toPartySet(n.getPartiesList)
          coid <- toContractId(n.getContractId)
        } yield AbsoluteContractId(coid) -> parties,
      )

    for {
      explicit <- sequence(explicitDisclosure)
      local <- sequence(implicitLocal)
      global <- sequence(globalDisclosure)
    } yield BlindingInfo(explicit.toMap, local.toMap, global.toMap)

  }

  def encode(
      blindingInfo: BlindingInfo,
      nodeIdWriter: Transaction.NodeId => String,
  ): proto.Blindinginfo.BlindingInfo = {
    val builder = proto.Blindinginfo.BlindingInfo.newBuilder()

    val localImplicit = blindingInfo.localDivulgence.map(nodeParties => {
      val b1 = proto.Blindinginfo.NodeParties.newBuilder()
      b1.setNodeId(nodeIdWriter(nodeParties._1))
      b1.addAllParties(nodeParties._2.toSet[String].asJava)
      b1.build()
    })

    val explicit = blindingInfo.disclosure.map(nodeParties => {
      val b1 = proto.Blindinginfo.NodeParties.newBuilder()
      b1.setNodeId(nodeIdWriter(nodeParties._1))
      b1.addAllParties(nodeParties._2.toSet[String].asJava)
      b1.build()
    })

    val global = blindingInfo.globalDivulgence.map(contractParties => {
      val b1 = proto.Blindinginfo.ContractParties.newBuilder()
      b1.setContractId(contractParties._1.coid)
      b1.addAllParties(contractParties._2.toSet[String].asJava)
      b1.build()
    })

    builder.addAllExplicitDisclosure(explicit.asJava)
    builder.addAllGlobalImplicitDisclosure(global.asJava)
    builder.addAllLocalImplicitDisclosure(localImplicit.asJava)
    builder.build()
  }

  private def toPartySet(strList: ProtocolStringList): Either[DecodeError, Set[Party]] = {
    val parties = strList
      .asByteStringList()
      .asScala
      .map(bs => Party.fromString(bs.toStringUtf8))

    sequence(parties) match {
      case Left(err) => Left(DecodeError(s"Cannot decode party: $err"))
      case Right(l) => Right(l.toSet)
    }
  }

  private def toContractId(s: String): Either[DecodeError, ContractIdString] =
    ContractIdString.fromString(s).left.map(err => DecodeError(s"Cannot decode contractId: $err"))

}
