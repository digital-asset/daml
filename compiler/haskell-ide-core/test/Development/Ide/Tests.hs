-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Development.IDE.Tests (main) where

import DAML.Project.Consts
import Test.Tasty as Tasty
import Test.Tasty.HUnit as Tasty
import System.Directory
import System.FilePath
import Control.Exception
import Test.Main

main :: IO ()
main = withRelPath $ Tasty.defaultMain $ Tasty.testGroup "Development.IDE.Tests" [fileTests]

relative :: FilePath
relative = "relative"

path :: FilePath
path = "path.txt"

relPath :: FilePath
relPath = relative </> path

withRelPath :: IO a -> IO a
withRelPath run = bracket create delete (const run) where

    create = do
        createDirectory relative
        writeFile relPath ""
    delete () = removeDirectoryRecursive relative

withRootAt :: Maybe FilePath -> IO a -> IO a
withRootAt location = withEnv [(projectPathEnvVar, location)]

fileTests :: Tasty.TestTree
fileTests = Tasty.testGroup "File Tests"
    [ Tasty.testCase "Absolute and relative files are the same outside of a project" $
      withRootAt Nothing $ do
            absolutePath <- makeRelativeToRoot =<< makeAbsolute relPath
            relative <- makeRelativeToRoot relPath
            assertEqual "Absolute path" relative absolutePath
            assertBool "Path is relative" $ isRelative relative

    , Tasty.testCase "All equivalent paths are equal if there is a project root" $ do
            cwd <- getCurrentDirectory
            withRootAt (Just cwd) $ do
                absolutePath <- makeRelativeToRoot =<< makeAbsolute relPath
                differentCwd <- withCurrentDirectory relative (makeRelativeToRoot path)
                relative <- makeRelativeToRoot relPath
                assertEqual "Different cwd" relative differentCwd
                assertEqual "Absolute path" relative absolutePath

    , Tasty.testCase "All equivalent paths are equal if there is a project root different to the cwd" $ do
            root <- canonicalizePath relative
            withRootAt (Just root) $ do
                absolutePath <- makeRelativeToRoot =<< makeAbsolute relPath
                relative <- makeRelativeToRoot relPath
                assertEqual "Absolute path" relative absolutePath
    ]
Development/Ide/Tests.hs
