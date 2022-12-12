-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import qualified BazelCache
import qualified CheckReleases
import qualified Docs

import qualified Control.Monad as Control
import qualified Options.Applicative as Opt
import qualified System.IO.Extra as IO

data CliArgs = Docs
             | Check { bash_lib :: String,
                       max_releases :: Maybe Int }
             | BazelCache BazelCache.Opts

parser :: Opt.ParserInfo CliArgs
parser = info "This program is meant to be run by CI cron. You probably don't have sufficient access rights to run it locally."
              (Opt.hsubparser (Opt.command "docs" docs
                            <> Opt.command "check" check
                            <> Opt.command "bazel-cache" bazelCache))
  where info t p = Opt.info (p Opt.<**> Opt.helper) (Opt.progDesc t)
        docs = info "Build & push latest docs, if needed."
                    (pure Docs)
        check = info "Check existing releases."
                     (Check <$> Opt.strOption (Opt.long "bash-lib"
                                         <> Opt.metavar "PATH"
                                         <> Opt.help "Path to Bash library file.")
                            <*> (Opt.optional $
                                  Opt.option Opt.auto (Opt.long "max-releases"
                                         <> Opt.metavar "INT"
                                         <> Opt.help "Max number of releases to check.")))
        bazelCache =
            info "Bazel cache debugging and fixing." $
            fmap BazelCache $ BazelCache.Opts
              <$> fmap (\m -> fromInteger (m * 60)) (Opt.option Opt.auto
                       (Opt.long "age" <>
                        Opt.help "Maximum age of entries that will be considered in minutes")
                    )
              <*> Opt.optional
                    (Opt.strOption
                      (Opt.long "cache-suffix" <>
                      Opt.help "Cache suffix as set by ci/configure-bazel.sh"))
              <*> Opt.option Opt.auto
                    (Opt.long "queue-size" <>
                     Opt.value 128 <>
                     Opt.help "Size of the queue used to distribute tasks among workers")
              <*> Opt.option Opt.auto
                    (Opt.long "concurrency" <>
                     Opt.value 32 <>
                     Opt.help "Number of concurrent workers that validate AC entries")
              <*> fmap BazelCache.Delete
                    (Opt.switch
                       (Opt.long "delete" <>
                        Opt.help "Whether invalid entries should be deleted or just displayed"))


main :: IO ()
main = do
    Control.forM_ [IO.stdout, IO.stderr] $
        \h -> IO.hSetBuffering h IO.LineBuffering
    opts <- Opt.execParser parser
    case opts of
      Docs -> do
          Docs.docs Docs.sdkDocOpts
          Docs.docs Docs.damlOnSqlDocOpts
      Check { bash_lib, max_releases } ->
          CheckReleases.check_releases bash_lib max_releases
      BazelCache opts -> BazelCache.run opts
