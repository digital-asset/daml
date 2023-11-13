// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.ByteBuffer
import javax.net.ssl.{SSLEngine, SSLEngineResult, SSLSession}

import com.daml.logging.{ContextualizedLogger, LoggingContext}

class LoggingSSLEngine(delegate: SSLEngine) extends SSLEngine {

  private val logger = ContextualizedLogger.get(getClass)
  private implicit val emptyContext = LoggingContext.empty

  override def wrap(
      byteBuffers: Array[ByteBuffer],
      i: Int,
      i1: Int,
      byteBuffer: ByteBuffer,
  ): SSLEngineResult = {
    logger.info("wrap start")
    val res = delegate.wrap(byteBuffers, i, i1, byteBuffer)
    logger.info(s"wrap end $res")
    res
  }

  override def unwrap(
      byteBuffer: ByteBuffer,
      byteBuffers: Array[ByteBuffer],
      i: Int,
      i1: Int,
  ): SSLEngineResult = {
    logger.info("unwrap start")
    val res = delegate.unwrap(byteBuffer, byteBuffers, i, i1)
    logger.info(s"unwrap end $res")
    res
  }

  override def getDelegatedTask: Runnable = {
    delegate.getDelegatedTask
  }

  override def closeInbound(): Unit = {
    logger.info("closeInbound start")
    delegate.closeInbound()
    logger.info("closeInbound end")
  }

  override def isInboundDone: Boolean = {
    delegate.isInboundDone
  }

  override def closeOutbound(): Unit = {
    logger.info("closeOutbound start")
    delegate.closeOutbound()
    logger.info("closeOutbound end")
  }

  override def isOutboundDone: Boolean = {
    delegate.isOutboundDone
  }

  override def getSupportedCipherSuites: Array[String] = {
    delegate.getSupportedCipherSuites
  }

  override def getEnabledCipherSuites: Array[String] = {
    delegate.getEnabledCipherSuites
  }

  override def setEnabledCipherSuites(strings: Array[String]): Unit = {
    delegate.setEnabledCipherSuites(strings)
  }

  override def getSupportedProtocols: Array[String] = {
    delegate.getSupportedProtocols
  }

  override def getEnabledProtocols: Array[String] = {
    delegate.getEnabledProtocols
  }

  override def setEnabledProtocols(strings: Array[String]): Unit = {
    delegate.setEnabledProtocols(strings)
  }

  override def getSession: SSLSession = {
    val res = delegate.getSession
    logger.info("getSession end")
    res
  }

  override def beginHandshake(): Unit = {
    logger.info("beginHandshake start")
    delegate.beginHandshake()
    logger.info("beginHandshake end")
  }

  override def getHandshakeStatus: SSLEngineResult.HandshakeStatus = {
    delegate.getHandshakeStatus
  }

  override def setUseClientMode(b: Boolean): Unit = {
    delegate.setUseClientMode(b)
  }

  override def getUseClientMode: Boolean = {
    delegate.getUseClientMode
  }

  override def setNeedClientAuth(b: Boolean): Unit = {
    delegate.setNeedClientAuth(b)
  }

  override def getNeedClientAuth: Boolean = {
    delegate.getNeedClientAuth
  }

  override def setWantClientAuth(b: Boolean): Unit = {
    delegate.setWantClientAuth(b)
  }

  override def getWantClientAuth: Boolean = {
    delegate.getWantClientAuth
  }

  override def setEnableSessionCreation(b: Boolean): Unit = {
    delegate.setEnableSessionCreation(b)
  }

  override def getEnableSessionCreation: Boolean = {
    delegate.getEnableSessionCreation
  }
}
