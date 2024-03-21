-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Test utils
module DA.Test.Util (
    redactStablePackageIds,
    standardizeQuotes,
    standardizeEoL,
    assertInfixOf,
    withTempFileResource,
    withTempDirResource,
    withEnv,
    withResourceCps,
    nullDevice,
    withDevNull,
    assertFileExists,
    assertFileDoesNotExist,
    limitJvmMemory,
    defaultJvmMemoryLimits,
    JvmMemoryLimits(..),
) where

import Control.Concurrent (putMVar, newEmptyMVar, takeMVar, forkIO)
import Control.Lens (view, _1)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.IO.Unlift (MonadUnliftIO)
import Data.List.Extra (isInfixOf)
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import System.Directory
import System.IO.Extra
import System.Info.Extra
import System.Environment.Blank
import Test.Tasty
import Test.Tasty.HUnit
import qualified UnliftIO.Exception as Unlift

import DA.Daml.StablePackages (allStablePackages)
import DA.Daml.LF.Ast.Base (unPackageId)

-- | Replaces known stable package IDs by 'XXXXXX' in a string.
redactStablePackageIds :: T.Text -> T.Text
redactStablePackageIds msg = foldr redact msg knownPackageIds
  where
    knownPackageIds = map unPackageId (MS.keys allStablePackages)
    redact pkgId text = T.replace pkgId "XXXXXX" text

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
  where delete (d, _delete) = removePathForcibly d

-- | Like Tasty's @withResource@, but instead of accepting setup and teardown
-- functions, it accepts a continuation-passing style function that encapsulates
-- both. Such functions are pervasive in our codebase (e.g. @withDevNull@).
--
-- >>> :t withResourceCps withDevNull
-- withResourceCps withDevNull :: (IO Handle -> TestTree) -> TestTree
withResourceCps :: ((a -> IO ()) -> IO ()) -> ((IO a -> TestTree) -> TestTree)
withResourceCps withResourceIO f = withResource acquire release action
  where
    acquire = do
        resourceMVar <- newEmptyMVar
        doneRunningActionMVar <- newEmptyMVar
        doneCleaningUpMVar <- newEmptyMVar
        _ <-
            forkIO $ do
                withResourceIO
                    ( \resource -> do
                        putMVar resourceMVar resource
                        takeMVar doneRunningActionMVar
                    )
                putMVar doneCleaningUpMVar ()
        resource <- takeMVar resourceMVar
        return (resource, doneRunningActionMVar, doneCleaningUpMVar)
    release (_, doneMVar, doneCleaningUpMVar) = do
      putMVar doneMVar ()
      takeMVar doneCleaningUpMVar
    action = f . fmap (view _1)

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
withEnv :: MonadUnliftIO m => [(String, Maybe String)] -> m t -> m t
withEnv vs m = Unlift.bracket (liftIO pushEnv) (liftIO . popEnv) (const m)
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
