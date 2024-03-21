-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Cli
    ( main
    ) where

import Control.Exception (SomeException, try)
import qualified DA.Cli.Args as ParseArgs
import DA.Cli.Damlc (Command (..), fullParseArgs)
import Data.Either (isRight)
import Data.List (isInfixOf)
import qualified Data.Text as T
import qualified Data.Text.Extended as T
import Options.Applicative
import System.Directory (getCurrentDirectory, withCurrentDirectory)
import System.Environment.Blank
import System.IO.Extra (stderr, stdout, withTempDir)
import System.IO.Silently (hCapture)
import Test.Tasty
import Test.Tasty.HUnit

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
    , testCase "Damlc correctly ignores build flags in build-options when running daml test" $ assertDamlcParser ["test"] ["--output", "./myDar.dar"] Nothing
    , testCase "Damlc correctly errors on build flags in daml test cli flags" $ assertDamlcParser ["test", "--output", "./myDar.dar"] [] (Just "Invalid option `--output'")
    , testCase "Damlc correctly errors on test flags in build-options when running daml build" $ assertDamlcParser ["build"] ["--save-coverage", "./here"] (Just "Invalid option `--save-coverage'")
    , testCase "Damlc correctly uses build-options flags in daml test if it can" $ assertDamlcParserRunIO ["test"] ["--debug"] "DEBUG"
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

withCurrentTempDir :: IO a -> IO a
withCurrentTempDir = withTempDir . flip withCurrentDirectory

-- Runs the damlc parser with a set of command line flags/options, and a set of daml.yaml flags/options
-- Takes a maybe expected error infix
assertDamlcParser :: [String] -> [String] -> Maybe String -> Assertion
assertDamlcParser cliArgs damlYamlArgs mExpectedError = withCurrentTempDir $ do
  (err, res) <- runDamlcParser cliArgs damlYamlArgs
  case (isRight res, mExpectedError) of
    (True, Nothing) -> pure ()
    (False, Just expectedErr) | expectedErr `isInfixOf` err -> pure ()
    (False, Just expectedErr) -> assertFailure $ "Computation failed correctly but was missing the expected error \"" <> expectedErr <> "\". Stderr was:\n" <> err
    (True, Just _) -> assertFailure "Expected failure but got success"
    (False, Nothing) -> assertFailure $ "Expected success but got failure:\n" <> err

withDamlProject :: IO a -> IO a
withDamlProject f = do
  cwd <- getCurrentDirectory
  setEnv "DAML_PROJECT" cwd True
  res <- f
  unsetEnv "DAML_PROJECT"
  pure res

-- Run the damlc parser with a set of command line flags/options, and a set of daml.yaml flags/options
-- Run the resulting computation and assert a given string is part of stdout+stderr
assertDamlcParserRunIO :: [String] -> [String] -> String -> Assertion
assertDamlcParserRunIO cliArgs damlYamlArgs expectedOutput = withCurrentTempDir $ do
  (err, res) <- runDamlcParser cliArgs damlYamlArgs
  case res of
    Right (Command _ _ io) -> do
      -- We don't care if the computation succeeds or fails here (it will fail, because we havent installed the sdk fully.)
      -- We're only testing flag behaviour that is discoverable before compilation happens
      (out, _) <- hCapture [stdout, stderr] $ try @SomeException $ withDamlProject io
      assertBool ("Expected " <> expectedOutput <> " in stdout/stderr, but didn't find") $ expectedOutput `isInfixOf` out
    Left _ -> assertFailure $ "Expected parse to succeed but got " <> err

runDamlcParser :: [String] -> [String] -> IO (String, Either SomeException Command)
runDamlcParser cliArgs damlYamlArgs = do
  T.writeFileUtf8 "./daml.yaml" $ T.unlines $
    [ "sdk-version: 0.0.0" -- Fixed version as the parser doesn't care for its value.
    , "name: test"
    , "source: daml"
    , "version: 0.0.1"
    , "dependencies:"
    , "  - daml-prim"
    , "  - daml-stdlib"
    , "build-options:"
    ] <> fmap (("  - " <>) . T.pack) damlYamlArgs

  hCapture [stderr] $ try @SomeException $ fullParseArgs 1 cliArgs
