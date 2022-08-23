// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse => SGetTransactionTreesResponse
}
import com.daml.ledger.api.v1.TransactionServiceOuterClass.{
  GetTransactionTreesResponse => JGetTransactionTreesResponse
}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger
import scala.io.Source
import scala.util.control.NonFatal
import scala.util.matching.Regex

class DecodeJavaScalaSpec extends AnyFlatSpec {
  val regex: Regex = raw".*TX tree: (.*?)\|\{participant.*".r
  "scala decode" should "decode" in {
    val lines = readFile("/Users/tudor/Downloads/downloaded-logs-20220822-225335.csv")
    val counter = new AtomicInteger(0)

    val (totalScala, totalJava) = lines.foldLeft(0L -> 0L) { case ((avgScala, avgJava), line) =>
      try {
        val decoded = Base64.getDecoder.decode(line)
        val (jDuration: Long, sDuration: Long) = benchmarkElement(decoded)
        val _ = counter.incrementAndGet()
        (avgScala + sDuration) -> (avgJava + jDuration)
      } catch {
        case NonFatal(_) => avgScala -> avgJava
      }
    }

    val averageScala = totalScala / counter.get()
    val averageJava = totalJava / counter.get()

    println(averageScala -> averageJava)
  }

  private def readFile(fn: String): Seq[String] = {
    val source = Source.fromFile(fn)
    val r = source
      .getLines()
      .collect {
        case line if line.contains("TX tree:") =>
          val regex(base64) = line
          base64
      }
      .toSeq
    source.close()
    r
  }

  private def benchmarkElement(bytes: Array[Byte]) = {
    val jTxTree = decodeTxTreeJava(bytes)
    val sTxTree = decodeTxTreeScala(bytes)

    val (_, jDuration) = encodeTxTreeJava(jTxTree)
    val (_, sDuration) = encodeTxTreeScala(sTxTree)
    (jDuration, sDuration)
  }

  private def decodeTxTreeScala(bytes: Array[Byte]) = SGetTransactionTreesResponse.parseFrom(bytes)

  private def decodeTxTreeJava(bytes: Array[Byte]) = JGetTransactionTreesResponse.parseFrom(bytes)

  private def encodeTxTreeScala(tx: SGetTransactionTreesResponse) = timed {
    tx.toByteArray
  }

  private def encodeTxTreeJava(tx: JGetTransactionTreesResponse) = timed {
    tx.toByteArray
  }

  private def timed[T](f: => T) = {
    val start = System.nanoTime()
    val r = f
    val durationNanos = System.nanoTime() - start
    r -> durationNanos / 1000L
  }
}
