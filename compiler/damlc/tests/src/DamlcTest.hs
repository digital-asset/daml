-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DamlcTest
   ( main
   ) where

import Control.Monad
import Data.List.Extra
import qualified Data.Text.Extended as T
import System.Directory
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

import DA.Bazel.Runfiles
import SdkVersion

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain (tests damlc)

tests :: FilePath -> TestTree
tests damlc = testGroup "damlc test" $
    [ testCase "Non-existent file" $ do
          (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", "foobar"] ""
          stdout @?= ""
          assertInfixOf "does not exist" stderr
          exitCode @?= ExitFailure 1
    , testCase "File with compile error" $ do
        withTempFile $ \path -> do
            T.writeFileUtf8 path $ T.unlines
              [ "daml 1.2"
              , "module Foo where"
              , "abc"
              ]
            (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", path] ""
            stdout @?= ""
            assertInfixOf "Parse error" stderr
            exitCode @?= ExitFailure 1
    , testCase "File with failing scenario" $ do
        withTempFile $ \path -> do
            T.writeFileUtf8 path $ T.unlines
              [ "daml 1.2"
              , "module Foo where"
              , "x = scenario $ assert False"
              ]
            (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", path] ""
            stdout @?= ""
            assertInfixOf "Scenario execution failed" stderr
            exitCode @?= ExitFailure 1
    , testCase "damlc test --files outside of project" $ withTempDir $ \projDir -> do
          writeFileUTF8 (projDir </> "Main.daml") $ unlines
            [ "daml 1.2"
            , "module Main where"
            , "test = scenario do"
            , "  assert True"
            ]
          (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", projDir </> "Main.daml"] ""
          exitCode @?= ExitSuccess
          assertBool ("Succeeding scenario in " <> stdout) ("Main.daml:test: ok" `isInfixOf` stdout)
          stderr @?= ""
    ] <>
    [ testCase ("damlc test " <> unwords (args "") <> " in project") $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "a")
          writeFileUTF8 (projDir </> "a" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projDir </> "a" </> "A.daml") $ unlines
            [ "daml 1.2 module A where"
            , "a = 1"
            ]
          callProcessSilent damlc ["build", "--project-root", projDir </> "a"]
          createDirectoryIfMissing True (projDir </> "b")
          writeFileUTF8 (projDir </> "b" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: b"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show (projDir </> "a/.daml/dist/a-0.0.1.dar") <> "]"
            ]
          writeFileUTF8 (projDir </> "b" </> "B.daml") $ unlines
            [ "daml 1.2 module B where"
            , "import A"
            , "b = a"
            , "test = scenario do"
            , "  assert True"
            ]
          (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ("test" : "--project-root" : (projDir </> "b") : args projDir) ""
          stderr @?= ""
          assertBool ("Succeeding scenario in " <> stdout) ("B.daml:test: ok" `isInfixOf` stdout)
          exitCode @?= ExitSuccess
    | args <- [\projDir -> ["--files", projDir </> "b" </> "B.daml"], const []]
    ]

-- | Only displays stdout and stderr on errors
-- TODO Move this in a shared testing-utils library
callProcessSilent :: FilePath -> [String] -> IO ()
callProcessSilent cmd args = do
    (exitCode, out, err) <- readProcessWithExitCode cmd args ""
    unless (exitCode == ExitSuccess) $ do
      hPutStrLn stderr $ "Failure: Command \"" <> cmd <> " " <> unwords args <> "\" exited with " <> show exitCode
      hPutStrLn stderr $ unlines ["stdout:", out]
      hPutStrLn stderr $ unlines ["stderr: ", err]
      exitFailure

assertInfixOf :: String -> String -> Assertion
assertInfixOf needle haystack = assertBool ("Expected " <> show needle <> " in output but but got " <> show haystack) (needle `isInfixOf` haystack)
