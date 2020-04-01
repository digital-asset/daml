-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    ShouldSucceed(..),
    callProcessSilent,
    callProcessSilentError,
) where

import Control.Monad
import Control.Exception.Safe
import Data.List.Extra (isInfixOf)
import qualified Data.Text as T
import System.IO.Extra
import System.Info.Extra
import System.Environment.Blank
import System.Exit
import System.Process
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
withTempDirResource f = withResource newTempDir snd (f . fmap fst)

nullDevice :: FilePath
nullDevice
    -- taken from typed-process
    | isWindows = "\\\\.\\NUL"
    | otherwise =  "/dev/null"

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

newtype ShouldSucceed = ShouldSucceed Bool

callProcessSilent, callProcessSilentError :: FilePath -> [String] -> IO ()
callProcessSilent = callProcessSilent' (ShouldSucceed True)
callProcessSilentError = callProcessSilent' (ShouldSucceed False)

callProcessSilent' :: ShouldSucceed -> FilePath -> [String] -> IO ()
callProcessSilent' (ShouldSucceed shouldSucceed) cmd args = do
    (exitCode, out, err) <- readProcessWithExitCode cmd args ""
    unless (shouldSucceed == (exitCode == ExitSuccess)) $ do
      hPutStrLn stderr $ "Failure: Command \"" <> cmd <> " " <> unwords args <> "\" exited with " <> show exitCode
      hPutStrLn stderr $ unlines ["stdout: ", out]
      hPutStrLn stderr $ unlines ["stderr: ", err]
      exitFailure
