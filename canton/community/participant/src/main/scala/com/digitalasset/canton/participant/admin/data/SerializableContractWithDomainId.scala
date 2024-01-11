// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import better.files.File
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.TransferCounterO
import com.digitalasset.canton.protocol.messages.HasDomainId
import com.digitalasset.canton.protocol.{HasSerializableContract, SerializableContract}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.{ByteStringUtil, ResourceUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.io.ByteArrayInputStream
import java.util.Base64
import scala.io.Source

// TODO(i14441): Remove deprecated ACS download / upload functionality
@deprecated("Use ActiveContract", since = "2.8.0")
private[canton] final case class SerializableContractWithDomainId(
    domainId: DomainId,
    contract: SerializableContract,
) extends HasDomainId
    with HasSerializableContract {

  import SerializableContractWithDomainId.{Delimiter, encoder}

  override def transferCounter: TransferCounterO = None

  def encode(protocolVersion: ProtocolVersion): String = {
    val byteStr = contract.toByteString(protocolVersion)
    val encoded = encoder.encodeToString(byteStr.toByteArray)
    val domain = domainId.filterString
    s"$domain$Delimiter$encoded\n"
  }
}

@deprecated("Use ActiveContract", since = "2.8.0")
private[canton] object SerializableContractWithDomainId {
  private val Delimiter = ":::"
  private val decoder = java.util.Base64.getDecoder
  private val encoder: Base64.Encoder = java.util.Base64.getEncoder

  private def decode(
      line: String,
      lineNumber: Int,
  ): Either[String, SerializableContractWithDomainId] =
    line.split(Delimiter).toList match {
      case domainId :: contractByteString :: Nil =>
        for {
          domainId <- DomainId.fromString(domainId)
          contract <- SerializableContract
            .fromByteArray(decoder.decode(contractByteString))
            .leftMap(err => s"Failed parsing disclosed contract: $err")
        } yield SerializableContractWithDomainId(domainId, contract)
      case line => Either.left(s"Failed parsing line $lineNumber: $line ")
    }

  private[admin] def loadFromByteString(
      bytes: ByteString,
      gzip: Boolean,
  ): Either[String, LazyList[SerializableContractWithDomainId]] = {
    for {
      decompressedBytes <-
        if (gzip)
          ByteStringUtil
            .decompressGzip(bytes, None)
            .leftMap(err => s"Failed to decompress bytes: $err")
        else Right(bytes)
      contracts <- ResourceUtil.withResource(
        Source.fromInputStream(new ByteArrayInputStream(decompressedBytes.toByteArray))
      ) { inputSource =>
        loadFromSource(inputSource)
      }
    } yield contracts
  }

  private[canton] def loadFromFile(fileInput: File): Iterator[SerializableContractWithDomainId] = {
    val decompressedInput = if (fileInput.toJava.getName.endsWith(".gz")) {
      fileInput.newGzipInputStream(8192)
    } else {
      fileInput.newFileInputStream
    }
    ResourceUtil.withResource(decompressedInput) { fileInput =>
      ResourceUtil.withResource(Source.fromInputStream(fileInput)) { inputSource =>
        loadFromSource(inputSource) match {
          case Left(error) => throw new Exception(error)
          case Right(value) => value.iterator
        }
      }
    }
  }

  private def loadFromSource(
      source: Source
  ): Either[String, LazyList[SerializableContractWithDomainId]] = {
    // build source iterator (we can't load everything into memory)
    LazyList
      .from(
        source
          .getLines()
          .zipWithIndex
      )
      .traverse { case (line, lineNumber) =>
        SerializableContractWithDomainId.decode(line, lineNumber)
      }
  }
}
