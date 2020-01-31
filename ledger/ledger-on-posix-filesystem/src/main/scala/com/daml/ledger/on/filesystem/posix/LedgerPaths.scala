// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{FileAlreadyExistsException, Files, Path}

class LedgerPaths(root: Path) {

  /** Used as the ledger lock; only one participant can work with the ledger at a time. */
  val ledgerLock: Path = root.resolve("ledger-lock")

  /** Used as the commit lock; when committing, only one commit owns the lock at a time. */
  val commitLock: Path = root.resolve("commit-lock")

  /** The root of the ledger log. */
  val logDirectory: Path = root.resolve("log")

  /** Directory in which each ledger entry is stored. */
  val logEntriesDirectory: Path = logDirectory.resolve("entries")

  /** A counter which is incremented with each commit;
    * always one more than the latest commit in the index.
    */
  val logHead: Path = logDirectory.resolve("head")

  /** Directory of sequential commits, each pointing to an entry in the "entries" directory. */
  val logIndexDirectory: Path = logDirectory.resolve("index")

  /** Directory containing the key-value store of the current state. */
  val stateDirectory: Path = root.resolve("state")

  def initialize(): Unit = {
    Files.createDirectories(root)
    ensureFileExists(ledgerLock)
    ensureFileExists(commitLock)
    Files.createDirectories(logDirectory)
    Files.createDirectories(logEntriesDirectory)
    Files.createDirectories(logIndexDirectory)
    Files.createDirectories(stateDirectory)
    ()
  }

  private def ensureFileExists(path: Path): Unit = {
    Files.createDirectories(path.getParent)
    try {
      Files.createFile(path)
      ()
    } catch {
      // this is fine.
      case _: FileAlreadyExistsException =>
    }
  }
}
