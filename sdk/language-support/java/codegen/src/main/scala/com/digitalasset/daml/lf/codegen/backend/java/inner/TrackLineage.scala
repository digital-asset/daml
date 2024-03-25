// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import java.util.{ArrayDeque, Deque}

import org.slf4j.MDC

import scala.jdk.CollectionConverters._

/** This object uses the loan pattern to generate a scope within which
  * the entity lineage can be tracked on a per-thread basis, meaning
  * that this approach works under the assumption that a module is
  * processed by a single thread.
  *
  * Using the [[TrackLineage.of]] method the MDC keys "entityType" and
  * "entityName" are managed, with the former keeping track of the
  * type of the entity currently being processed and the latter used
  * to put together the whole entity name.
  *
  * This ensures that the processed entity can be fully tracked by
  * logging without the need of pushing extra information down the
  * call stack.
  */
private[inner] object TrackLineage {

  private[this] val nameLineage = new ThreadLocal[Deque[String]] {
    override protected def initialValue() = new ArrayDeque[String](16)
  }

  private[this] val typeLineage = new ThreadLocal[Deque[String]] {
    override protected def initialValue() = new ArrayDeque[String](16)
  }

  def of[A](entityType: String, entityName: String)(f: => A): A = {
    enter(entityType, entityName)
    try f
    finally exit()
  }

  private def enter(entityType: String, entityName: String): Unit = synchronized {
    typeLineage.get.addLast(entityType)
    MDC.put("entityType", entityType)
    nameLineage.get.addLast(entityName)
    MDC.put("entityName", nameLineageAsString)
  }

  private def exit(): Unit = synchronized {
    typeLineage.get.removeLast()
    if (typeLineage.get.isEmpty) {
      MDC.remove("entityType")
    } else {
      MDC.put("entityType", typeLineage.get.peekLast)
    }
    nameLineage.get.removeLast()
    if (nameLineage.get.isEmpty) {
      MDC.remove("entityName")
    } else {
      MDC.put("entityName", nameLineageAsString)
    }
  }

  private def nameLineageAsString: String = nameLineage.get.asScala.mkString(".")

}
