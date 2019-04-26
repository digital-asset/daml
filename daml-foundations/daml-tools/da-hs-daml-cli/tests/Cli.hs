-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Cli
    ( main
    ) where

import Control.Exception
import Options.Applicative
import System.Environment
import System.Exit
import Test.Tasty
import Test.Tasty.HUnit

import DA.Cli.Args

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1"
    defaultMain tests

tests :: TestTree
tests = testGroup
    "Cli arguments"
    [ testCase "No flags in strict mode" $ parseSucceeds ["ide"]
    , testCase "Bad flags in strict mode" $ parseFails ["ide", "--badFlag"]
    , testCase "Good flags in lax mode" $ parseSucceeds ["lax", "ide"]
    , testCase "Bad flags in lax mode" $ parseSucceeds ["lax", "ide", "--badFlag"]
    ]

parse :: [String] -> IO ()
parse args = withArgs args $ execParserLax parserInfo

parseSucceeds :: [String] -> Assertion
parseSucceeds = shouldSucceed . parse

parseFails :: [String] -> Assertion
parseFails = shouldFail . parse

parserInfo :: ParserInfo ()
parserInfo = info (subparser cmdIde) idm
   where cmdIde = command "ide" $ info (pure ()) idm

shouldFail :: IO () -> Assertion
shouldFail a = do
    b <- try a
    b @?= Left (ExitFailure 1)

shouldSucceed :: IO () -> Assertion
shouldSucceed a = do
    b :: Either ExitCode () <- try a
    b @?= Right ()
