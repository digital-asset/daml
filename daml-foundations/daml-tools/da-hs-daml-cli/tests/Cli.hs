-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Cli
        ( main
        )
where

import           Test.Tasty
import           Test.Tasty.HUnit
import           System.Timeout
import           System.Environment
import           System.Exit
import           Control.Exception
import           Data.Maybe
import qualified DA.Cli.Damlc                  as Damlc


main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1"
    defaultMain tests

tests :: TestTree
tests = testGroup
        "Cli arguments"
        [ testCase "No flags in strict mode" $ noExit ["ide"]
        , testCase "Bad flags in strict mode" $ fails ["ide", "--badFlag"]
        , testCase "Good flags in lax mode" $ noExit ["lax", "ide"]
        , testCase "Bad flags in lax mode" $ noExit ["lax", "ide", "--badFlag"]
        ]

-- | when developing new tests change this to 10000 (10ms) to avoid flakiness on CI
shortTime :: Int
-- shortTime = 10000
shortTime = 20000

-- | When damlc is supplied with these arguments does it exit in the first 20ms
runForShortTime
        :: [String] -- ^ Cli arguments
        -> IO (Maybe (Either ExitCode ()))
runForShortTime args = timeout shortTime $ try $ withArgs args Damlc.main

fails :: [String] -> Assertion
fails a = do
        b <- runForShortTime a
        b @?= Just (Left $ ExitFailure 1)

noExit :: [String] -> Assertion
noExit a = do
        b <- runForShortTime a
        assertBool "should not exit immediately" $ isNothing b
