-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import BazelCache qualified

import Control.Monad qualified as Control
import Options.Applicative qualified as Opt
import System.IO.Extra qualified as IO

data CliArgs = BazelCache BazelCache.Opts

parser :: Opt.ParserInfo CliArgs
parser = info "This program is meant to be run by CI cron. You probably don't have sufficient access rights to run it locally."
              (Opt.hsubparser (Opt.command "bazel-cache" bazelCache))
  where info t p = Opt.info (p Opt.<**> Opt.helper) (Opt.progDesc t)
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
      BazelCache opts -> BazelCache.run opts
