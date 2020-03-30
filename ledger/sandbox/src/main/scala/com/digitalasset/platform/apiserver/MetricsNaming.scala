// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import com.codahale.metrics.MetricRegistry

private[apiserver] object MetricsNaming {

  private[this] val capitalization = "[A-Z]+".r
  private[this] val startWordCapitalization = "^[A-Z]+".r
  private[this] val endWordAcronym = "[A-Z]{2,}$".r

  private[this] val snakifyWholeWord = (s: String) => if (s.forall(_.isUpper)) s.toLowerCase else s

  private[this] val snakify = (s: String) =>
    capitalization.findAllMatchIn(s).foldRight(s) { (m, r) =>
      val s = m.toString
      if (s.length == 1) r.patch(m.start, s"_${s.toLowerCase}", 1)
      else r.patch(m.start, s"_${s.init.toLowerCase}_${s.last.toLower}", s.length)
  }

  private[this] val snakifyStart = (s: String) =>
    startWordCapitalization.findFirstIn(s).fold(s) { m =>
      s.patch(
        0,
        if (m.length == 1) m.toLowerCase else m.init.toLowerCase,
        math.max(m.length - 1, 1))
  }

  private[this] val snakifyEnd = (s: String) =>
    endWordAcronym.findFirstIn(s).fold(s) { m =>
      s.patch(s.length - m.length, s"_${m.toLowerCase}", m.length)
  }

  // Turns a camelCased string into a snake_cased one
  val camelCaseToSnakeCase: String => String =
    snakifyWholeWord andThen snakifyStart andThen snakifyEnd andThen snakify

  // assert(nameForService("org.example.SomeService") == "daml.lapi.some_service")
  def nameForService(fullServiceName: String): String = {
    val serviceName = camelCaseToSnakeCase(fullServiceName.split('.').last)
    MetricRegistry.name("daml", "lapi", serviceName)
  }

  // assert(nameFor("org.example.SomeService/someMethod") == "daml.lapi.some_service.some_method")
  def nameFor(fullMethodName: String): String = {
    val serviceAndMethodName = fullMethodName.split('/')
    assert(
      serviceAndMethodName.length == 2,
      s"Expected service and method names separated by '/', got '$fullMethodName'")
    val prefix = nameForService(serviceAndMethodName(0))
    val methodName = camelCaseToSnakeCase(serviceAndMethodName(1))
    MetricRegistry.name(prefix, methodName)
  }

}
