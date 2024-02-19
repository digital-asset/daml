// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import ammonite.runtime.Storage
import cats.syntax.either.*
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext

import java.io.{File, RandomAccessFile}
import java.nio.channels.OverlappingFileLockException
import scala.concurrent.blocking
import scala.util.control.NonFatal

trait AmmoniteCacheLock {
  def release(): Unit
  def storage: Storage
  def lockFile: Option[java.io.File]
}

object AmmoniteCacheLock {

  // Don't change this to lazy val, as the underlying InMemory storage is not thread safe.
  def InMemory: AmmoniteCacheLock = new AmmoniteCacheLock {
    override def release(): Unit = ()
    override val storage: Storage = Storage.InMemory()
    override def toString: String = "in-memory cache"
    override def lockFile: Option[File] = None
  }

  def create(logger: TracedLogger, path: os.Path, isRepl: Boolean): AmmoniteCacheLock = {
    import TraceContext.Implicits.Empty.*
    def go(index: Int): Either[String, AmmoniteCacheLock] = {
      val newPath = path / s"$index"
      for {
        _ <- Either.cond(index < 255, (), s"Cache dir attempt reached $index, giving up")
        _ <- createDirsIfNecessary(newPath)
        _ <- ensureDirIsWritable(newPath)
        lockO <- acquireLock(logger, newPath, isRepl).leftMap { err =>
          logger.debug("Observed lock exception", err)
          err.getMessage
        }
        lock <- lockO match {
          case Some(value) => Right(value)
          case None => go(index + 1)
        }
      } yield lock
    }
    try {
      // check that cache directory is writable
      val attempt = for {
        _ <- createDirsIfNecessary(path)
        _ <- ensureDirIsWritable(path)
        lock <- go(0)
      } yield lock
      attempt match {
        case Right(lock) => lock
        case Left(err) =>
          logger.warn(
            s"Failed to acquire ammonite cache directory due to ${err}. Will use in-memory instead."
          )
          InMemory
      }
    } catch {
      case NonFatal(e) =>
        logger.warn("Failed to acquire ammonite cache directory. Will use in-memory instead.", e)
        InMemory
    }
  }

  private def createDirsIfNecessary(path: os.Path): Either[String, Unit] =
    if (path.toIO.exists())
      Either.cond(path.toIO.isDirectory, (), s"File ${path} exists but is not a directory")
    else
      Either.cond(
        // create or test again (mkdirs fails if the directory exists in the meantime, which can happen
        // if several tests try to create the directory at the same time
        path.toIO.mkdirs() || path.toIO.exists(),
        (),
        s"Failed to create ammonite cache directory ${path}. Is the path writable?",
      )

  private def ensureDirIsWritable(path: os.Path): Either[String, Unit] = {
    Either.cond(path.toIO.canWrite, (), s"Directory $path is not writable")
  }

  private def acquireLock(logger: TracedLogger, path: os.Path, isRepl: Boolean)(implicit
      traceContext: TraceContext
  ): Either[Throwable, Option[AmmoniteCacheLock]] = blocking(synchronized {
    try {
      val myLockFile = path / "lock"
      if (myLockFile.toIO.exists()) {
        Right(None)
      } else {
        logger.debug(s"Attempting to obtain lock ${myLockFile}")
        val out = new RandomAccessFile(myLockFile.toIO, "rw")
        Option(out.getChannel.tryLock()) match {
          case None =>
            logger.debug(s"Failed to acquire lock for ${myLockFile}")
            out.close()
            Right(None)
          case Some(lock) =>
            myLockFile.toIO.deleteOnExit()
            Right(Some(new AmmoniteCacheLock {
              override def release(): Unit = {
                try {
                  logger.debug(s"Releasing lock $myLockFile...")
                  lock.release()
                  out.close()
                  if (!myLockFile.toIO.delete()) {
                    logger.warn(s"Failed to delete lock file ${myLockFile}")
                  }
                } catch {
                  case NonFatal(e) =>
                    logger.error(s"Releasing ammonite cache lock $lockFile failed", e)
                }
              }

              override val storage: Storage = new Storage.Folder(path, isRepl = isRepl)

              override def toString: String = s"file cache at $path"

              override def lockFile: Option[File] = Some(myLockFile.toIO)
            }))

        }
      }
    } catch {
      case e: OverlappingFileLockException => Right(None)
      case NonFatal(e) => Left(e)
    }
  })

}
