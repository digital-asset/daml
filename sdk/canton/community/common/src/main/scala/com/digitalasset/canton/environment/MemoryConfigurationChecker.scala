// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.config.StartupMemoryCheckConfig
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BytesUnit.Bytes

import java.lang.management.ManagementFactory
import javax.management.ObjectName
import scala.util.{Failure, Success, Try}

object MemoryConfigurationChecker {

  /**  The Canton app requires additional memory for off-heap memory, garbage collection overhead, JVM internals, threads, etc.
    *  It is recommended to allocate at least 2x the -Xmx value to the container's memory limit.
    *  This method checks if the -Xmx value is within half of the container's total memory, and depending on the configuration,
    *  logs a warning, error, or crashes the app.
    */
  def check(config: StartupMemoryCheckConfig, logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): Unit = {
    val checkT = for {
      // Fetch total physical memory, which also works for detecting container limits from cgroup, e.g., Kubernetes container.resources.limits.memory.
      mBeanServer <- Try(ManagementFactory.getPlatformMBeanServer)
      totalMemoryBytes <- Try(
        mBeanServer.getAttribute(
          new ObjectName("java.lang", "type", "OperatingSystem"),
          "TotalPhysicalMemorySize",
        )
      ) match {
        case Success(value: java.lang.Long) => Success(Bytes(value))
        case result =>
          Failure(
            new IllegalStateException(
              s"Unexpected attribute type for TotalPhysicalMemorySize $result"
            )
          )
      }
      // Get JVM max heap size (-Xmx)
      maxHeapSizeBytes = Bytes(Runtime.getRuntime.maxMemory())
    } yield {
      val twiceMaxHeapSizeBytes = maxHeapSizeBytes * NonNegativeLong.tryCreate(2)
      val check = twiceMaxHeapSizeBytes <= totalMemoryBytes
      val warnMessage =
        s"""|-Xmx ($maxHeapSizeBytes) exceeds half of the container's total memory $totalMemoryBytes.
            |We recommend to increase the container's memory limit to at least $twiceMaxHeapSizeBytes.""".stripMargin

      (check, config) match {
        case (true, _) =>
          logger.debug(
            s"Memory configuration is safe. -Xmx ($maxHeapSizeBytes) is within half of the container's total memory ($totalMemoryBytes)."
          )
        case (false, StartupMemoryCheckConfig.Warn) =>
          logger.warn(warnMessage)
        case (false, StartupMemoryCheckConfig.Ignore) =>
          logger.debug(warnMessage)
        case (false, StartupMemoryCheckConfig.Crash) =>
          logger.error(warnMessage)
          throw new IllegalStateException(
            "Memory check failed at startup. Consider increasing memory allocation for the container or changing canton parameter to not crash on memory check."
          )
      }
    }

    checkT.getOrElse(throw new IllegalStateException("Memory check failed at startup."))
  }
}
