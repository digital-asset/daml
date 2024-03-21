-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Cli
    ( main
    ) where

import Options.Applicative
import System.Environment.Blank
import Test.Tasty
import Test.Tasty.HUnit

import qualified DA.Cli.Args as ParseArgs

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    defaultMain tests

tests :: TestTree
tests = testGroup
    "Cli arguments"
    [ testCase "No flags in strict mode" $ parseSucceeds ["ide"]
    , testCase "Bad flags in strict mode" $ parseFails ["ide", "--badFlag"]
    , testCase "Good flags in lax mode" $ parseSucceeds ["lax", "ide"]
    , testCase "Bad flags in lax mode" $ parseSucceeds ["lax", "ide", "--badFlag"]
    ]

parse :: [String] -> Maybe ()
parse args = getParseResult $ snd $ ParseArgs.lax parserInfo args

parseSucceeds :: [String] -> Assertion
parseSucceeds args = parse args @?= Just ()

parseFails :: [String] -> Assertion
parseFails args = parse args @?= Nothing

parserInfo :: ParserInfo ()
parserInfo = info (subparser cmdIde) idm
   where cmdIde = command "ide" $ info (pure ()) idm
