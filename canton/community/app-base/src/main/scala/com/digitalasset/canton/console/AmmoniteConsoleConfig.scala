// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import ammonite.Main
import ammonite.main.Defaults
import ammonite.util.Colors
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.TracedLogger
import com.typesafe.scalalogging.LazyLogging

import java.io.{File, IOException}

/** Configure behaviour of ammonite
  *
  * @param cacheDir cache dir, defaults to ~/.ammonite. If none is given, in-memory is used.
  *                 If you specify a cache dir, the Canton repl will startup faster.
  *                 In our tests, we have very rarely observed unexpected compile errors when the cache was enabled;
  *                 if you want to avoid that, set the cache dir to None (i.e. `cache-dir = null` in the config file).
  * @param workingDir working directory. if none is given, we'll use the working directory of the Canton process
  * @param colors if true (default), we'll use color output
  * @param verbose if true (not default), we'll emit verbose ammonite output
  * @param defaultLimit default limit parameter for commands that can potentially return many results
  */
final case class AmmoniteConsoleConfig(
    cacheDir: Option[java.io.File] = AmmoniteConsoleConfig.defaultCacheDir,
    workingDir: Option[java.io.File] = None,
    colors: Boolean = true,
    verbose: Boolean = false,
    defaultLimit: PositiveInt = PositiveInt.tryCreate(1000),
)

object AmmoniteConsoleConfig extends LazyLogging {

  private def defaultCacheDir: Option[java.io.File] = {
    val f = new File(System.getProperty("user.home"))
    if (f.exists() && f.isDirectory)
      Some(Defaults.ammoniteHome.toIO)
    else {
      logger.warn(
        s"""Can not determine user home directory using the java system property `user.home`
        | (is set to ${System.getProperty("user.home")}). Please set it
        |on jvm startup using -Duser.home=...""".stripMargin
      )
      None
    }
  }

  private def ensureTmpFilesCanBeCreated(): Unit = {
    try {
      val f = File.createTempFile("dummy", "test")
      val _ = f.delete()
    } catch {
      case e: IOException =>
        logger.error(
          "Unable to create temporary files (javas `File.createTempFile` throws an exception). Please make sure that the jvm can create files. The process will likely start to fail now.",
          e,
        )
    }
  }

  private[console] def create(
      config: AmmoniteConsoleConfig,
      predefCode: String,
      welcomeBanner: Option[String],
      isRepl: Boolean,
      logger: TracedLogger,
  ): (AmmoniteCacheLock, Main) = {
    val cacheLock: AmmoniteCacheLock = config.cacheDir match {
      case Some(file) => AmmoniteCacheLock.create(logger, os.Path(file), isRepl = isRepl)
      case None => AmmoniteCacheLock.InMemory
    }
    // ensure that we can create tmp files
    ensureTmpFilesCanBeCreated()
    val main = Main(
      predefCode = predefCode,
      storageBackend = cacheLock.storage,
      wd = config.workingDir.fold(os.pwd)(x => os.Path(x.getAbsolutePath)),
      welcomeBanner = welcomeBanner,
      verboseOutput =
        config.verbose, // disable things like "Compiling [x]..." messages from ammonite
      // ammonite when run as a binary will log the number of commands that are executed in a session for the maintainer to see usage
      // I don't think this happens when used in an embedded fashion like we're doing, but let's disable just to be sure ( ˇ෴ˇ )
      remoteLogging = false,
      colors = if (config.colors) Colors.Default else Colors.BlackWhite,
    )
    (cacheLock, main)
  }

}
