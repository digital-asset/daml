-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Test utils
module DA.Test.Util (
    standardizeQuotes,
    standardizeEoL,
    assertInfixOf,
    withTempFileResource,
    withTempDirResource,
    withEnv,
    nullDevice,
    withDevNull,
    assertFileExists,
    assertFileDoesNotExist,
    limitJvmMemory,
    defaultJvmMemoryLimits,
    JvmMemoryLimits(..),
) where

import Control.Monad
import Control.Exception.Safe
import Data.List.Extra (isInfixOf)
import qualified Data.Text as T
import System.Directory
import System.IO.Extra
import System.Info.Extra
import System.Environment.Blank
import Test.Tasty
import Test.Tasty.HUnit

standardizeQuotes :: T.Text -> T.Text
standardizeQuotes msg = let
        repl '‘' = '\''
        repl '’' = '\''
        repl '`' = '\''
        repl  c   = c
    in  T.map repl msg

standardizeEoL :: T.Text -> T.Text
standardizeEoL = T.replace (T.singleton '\r') T.empty

assertInfixOf :: String -> String -> Assertion
assertInfixOf needle haystack = assertBool ("Expected " <> show needle <> " in output but but got " <> show haystack) (needle `isInfixOf` haystack)

withTempFileResource :: (IO FilePath -> TestTree) -> TestTree
withTempFileResource f = withResource newTempFile snd (f . fmap fst)

withTempDirResource :: (IO FilePath -> TestTree) -> TestTree
withTempDirResource f = withResource newTempDir delete (f . fmap fst)
  -- The delete action provided by `newTempDir` calls `removeDirectoryRecursively`
  -- and silently swallows errors. SDK installations are marked read-only
  -- which means that they don’t end up being removed which is obviously
  -- not what we intend.
  -- As usual Windows is terrible and doesn’t let you remove the SDK
  -- if there is a process running. Simultaneously it is also terrible
  -- at process management so we end up with running processes
  -- since child processes aren’t torn down properly
  -- (Bazel will kill them later when the test finishes). Therefore,
  -- we ignore exceptions and hope for the best. On Windows that
  -- means we still leak directories :(
  where delete (d, _delete) = void $ tryIO $ removePathForcibly d

nullDevice :: FilePath
nullDevice
    -- taken from typed-process
    | isWindows = "\\\\.\\NUL"
    | otherwise =  "/dev/null"

-- | Getting a dev-null handle in a cross-platform way seems to be somewhat tricky so we instead
-- use a temporary file.
withDevNull :: (Handle -> IO a) -> IO a
withDevNull a = withTempFile $ \f -> withFile f WriteMode a

-- | Replace all environment variables for test action, then restore them.
-- Avoids System.Environment.setEnv because it treats empty strings as
-- "delete environment variable", unlike main-tester's withEnv which
-- consequently conflates (Just "") with Nothing.
withEnv :: [(String, Maybe String)] -> IO t -> IO t
withEnv vs m = bracket pushEnv popEnv (const m)
    where
        pushEnv :: IO [(String, Maybe String)]
        pushEnv = replaceEnv vs

        popEnv :: [(String, Maybe String)] -> IO ()
        popEnv vs' = void $ replaceEnv vs'

        replaceEnv :: [(String, Maybe String)] -> IO [(String, Maybe String)]
        replaceEnv vs' = do
            forM vs' $ \(key, newVal) -> do
                oldVal <- getEnv key
                case newVal of
                    Nothing -> unsetEnv key
                    Just val -> setEnv key val True
                pure (key, oldVal)

assertFileExists :: FilePath -> IO ()
assertFileExists file = doesFileExist file >>= assertBool (file ++ " was expected to exist, but does not exist")

assertFileDoesNotExist :: FilePath -> IO ()
assertFileDoesNotExist file = doesFileExist file >>= assertBool (file ++ " was expected to not exist, but does exist") . not

data JvmMemoryLimits = JvmMemoryLimits
  { initialHeapSize :: String
  , maxHeapSize :: String
  }

defaultJvmMemoryLimits :: JvmMemoryLimits
defaultJvmMemoryLimits = JvmMemoryLimits
  { initialHeapSize = "128m"
  , maxHeapSize = "256m"
  }

limitJvmMemory :: JvmMemoryLimits -> IO ()
limitJvmMemory JvmMemoryLimits{..} = do
    setEnv "_JAVA_OPTIONS" limits True
  where
    limits = unwords
      [ "-Xms" <> initialHeapSize
      , "-Xmx" <> maxHeapSize
      ]
