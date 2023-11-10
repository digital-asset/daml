-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import System.Directory
import System.FilePath
import Test.Tasty
import Test.Tasty.HUnit
import DA.Bazel.Runfiles

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "Bazel runfiles tests"
  [
    fileExistsTest,
    filePathIsAbsoluteTest,
    dirExistsTest,
    dirPathIsAbsoluteTest,
    dirContainsExpectedFilesTest
  ]

fileExistsTest :: TestTree
fileExistsTest = testCase "File path returned by locateRunfiles should exist" $ do
  file <- locateRunfiles (mainWorkspace </> "libs-haskell" </> "bazel-runfiles" </> "test" </> "resources" </> "file.txt")
  fileExist <- doesFileExist file
  assertBool "Pointed file should exist" fileExist

filePathIsAbsoluteTest :: TestTree
filePathIsAbsoluteTest = testCase "File path returned by locateRunfiles should be absolute" $ do
  file <- locateRunfiles (mainWorkspace </> "libs-haskell" </> "bazel-runfiles" </> "test" </> "resources" </> "file.txt")
  assertBool "Returned file path should be absolute" (isAbsolute file)

dirExistsTest :: TestTree
dirExistsTest = testCase "Directory path returned by locateRunfiles should exist" $ do
  dir <- locateRunfiles (mainWorkspace </> "libs-haskell" </> "bazel-runfiles" </> "test" </> "resources" </> "dir")
  dirExist <- doesDirectoryExist dir
  assertBool "Pointed directory should exist" dirExist

dirPathIsAbsoluteTest :: TestTree
dirPathIsAbsoluteTest = testCase "Directory path returned by locateRunfiles should be absolute" $ do
  dir <- locateRunfiles (mainWorkspace </> "libs-haskell" </> "bazel-runfiles" </> "test" </> "resources" </> "dir")
  assertBool "Returned directory path should be absolute" (isAbsolute dir)

dirContainsExpectedFilesTest :: TestTree
dirContainsExpectedFilesTest = testCase "Directory returned by locateRunfiles should contain expected files" $ do
  dir <- locateRunfiles (mainWorkspace </> "libs-haskell" </> "bazel-runfiles" </> "test" </> "resources" </> "dir")
  files <- listDirectory dir
  assertEqual "Returned directory should contain expected # of files" 2 (length files)
