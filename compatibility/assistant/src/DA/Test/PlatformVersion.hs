-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.PlatformVersion (main) where

import qualified Bazel.Runfiles
import Control.Concurrent.STM
import Control.Exception.Safe
import Control.Monad
import Data.ByteString.Lazy.UTF8 (ByteString, toString)
import Data.Conduit ((.|), runConduitRes)
import qualified Data.Conduit.Combinators as Conduit
import qualified Data.Conduit.Tar as Tar
import qualified Data.Conduit.Zlib as Zlib
import Data.List
import qualified Data.Text as T
import DA.Test.Tar
import System.Directory
import System.Environment.Blank
import System.FilePath
import System.Info (os)
import System.IO.Extra
import qualified System.Process.Typed as Proc
import Test.Tasty
import Test.Tasty.HUnit
import Data.Maybe
import Sandbox (readPortFile, maxRetries)
import Versions (latestStableVersion)
import System.Process (interruptProcessGroupOf)
import qualified DA.Test.Util as Util

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    javaHome <- fromJust <$> getEnv "JAVA_HOME"
    oldPath <- getSearchPath
    setEnv "PATH" (intercalate [searchPathSeparator] ((javaHome </> "bin") : oldPath)) True
    defaultMain $ withSdkResource $ \_getSdkPath ->
        testGroup "platform-version"
            [ testCase "no project, no DAML_PLATFORM_VERSION" $ do
                  env <- extendEnv [("DAML_SDK_VERSION", "0.0.0")]
                  out <- Proc.readProcessStdout_ (Proc.setEnv env $ Proc.shell "daml sandbox --help")
                  assertInfixOf "Sandbox version 0.0.0" out
                  out <- Proc.readProcessStdout_ (Proc.setEnv env $ Proc.shell "daml sandbox-classic --help")
                  assertInfixOf "Sandbox version 0.0.0" out
            , testCase "no project, DAML_PLATFORM_VERSION" $ do
                  env <- extendEnv [("DAML_SDK_VERSION", "0.0.0"), ("DAML_PLATFORM_VERSION", latestStableVersion)]
                  out <- Proc.readProcessStdout_
                      (Proc.setEnv env $ Proc.shell "daml sandbox --help")
                  assertInfixOf ("Sandbox version " <> latestStableVersion) out
                  out <- Proc.readProcessStdout_
                      (Proc.setEnv env $ Proc.shell "daml sandbox-classic --help")
                  assertInfixOf ("Sandbox version " <> latestStableVersion) out
            , testCase "no project, platform-version" $ withTempDir $ \tempDir -> do
                  writeFileUTF8 (tempDir </> "daml.yaml") $ unlines
                    [ "sdk-version: 0.0.0"
                    , "platform-version: " <> latestStableVersion
                    ]
                  out <- Proc.readProcessStdout_ (Proc.setWorkingDir tempDir (Proc.shell "daml sandbox --help"))
                  assertInfixOf ("Sandbox version " <> latestStableVersion) out
                  -- Env var takes precedence
                  env <- extendEnv [("DAML_PLATFORM_VERSION", "0.0.0")]
                  out <- Proc.readProcessStdout_
                      (Proc.setWorkingDir tempDir $ Proc.setEnv env $ Proc.shell "daml sandbox --help")
                  assertInfixOf "Sandbox version 0.0.0" out
            , testCase "daml start" $ withTempDir $ \tempDir -> do
                  writeFileUTF8 (tempDir </> "daml.yaml") $ unlines
                    [ "sdk-version: 0.0.0"
                    , "platform-version: " <> latestStableVersion
                    , "name: foobar"
                    , "version: 0.0.1"
                    , "dependencies: [daml-prim, daml-stdlib]"
                    , "source: ."
                    , "parties: []"
                    ]
                  let conf =
                          Proc.setCreateGroup True $
                          Proc.setStdin Proc.createPipe $
                          Proc.setStdout Proc.byteStringOutput $
                          Proc.setWorkingDir tempDir $
                          Proc.shell $
                            "daml start --shutdown-stdin-close --open-browser=no --json-api-option --port-file --json-api-option " <> show (tempDir </> "portfile")
                  getOut <- Proc.withProcessWait conf $ \ph -> do
                      -- Wait for the port file as a sign that the JSON API has started.
                      _ <- readPortFile maxRetries (tempDir </> "portfile")
                      hClose (Proc.getStdin ph)
                      -- Process management on Windows is a nightmare so we opt
                      -- for the safest option of creating a group and killing everything
                      -- in that group.
                      interruptProcessGroupOf (Proc.unsafeProcessHandle ph)
                      pure $ Proc.getStdout ph
                  out <- atomically getOut
                  putStrLn "got stdout"
                  assertInfixOf ("sandbox version " <> latestStableVersion) out
                  -- Navigator, sadly not prefixed with Navigator
                  assertInfixOf "Version 0.0.0" out
                  -- JSON API, doesn’t even print a version but let’s at least check
                  -- that something started.
                  assertInfixOf "httpPort=7575" out

            ]

extendEnv :: [(String, String)] -> IO [(String, String)]
extendEnv xs = do
    oldEnv <- getEnvironment
    pure (xs ++ filter (\(k, _) -> k `notElem` map fst xs) oldEnv)

withSdkResource :: (IO FilePath -> TestTree) -> TestTree
withSdkResource f =
    withTempDirResource $ \getDir ->
    withResource (installSdk =<< getDir) (const $ pure ()) (const $ f getDir)
  where installSdk targetDir = do
            runfiles <- Bazel.Runfiles.create
            let headSdk = Bazel.Runfiles.rlocation runfiles "head_sdk/sdk-release-tarball.tar.gz"
            let latestStableSdk = Bazel.Runfiles.rlocation runfiles "daml-sdk-tarball-latest-stable/file/downloaded"
            setEnv "DAML_HOME" targetDir True
            withTempDir $ \extractDir -> do
                runConduitRes
                    $ Conduit.sourceFileBS headSdk
                    .| Zlib.ungzip
                    .| Tar.untar (restoreFile (\a b -> fail (T.unpack $ a <> b)) extractDir)
                Proc.runProcess_ $
                    Proc.proc
                        (extractDir </> "daml" </> exe "daml")
                        ["install", "--install-assistant=yes", "--set-path=no", extractDir, "--quiet"]
                pure ()
            oldPath <- getSearchPath
            setEnv "PATH" (intercalate [searchPathSeparator] ((targetDir </> "bin") : oldPath)) True
            Proc.runProcess_ (Proc.shell $ "daml install --quiet " <> latestStableSdk)
            pure ()

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

exe :: FilePath -> FilePath
exe | os == "mingw32" = (<.> "exe")
    | otherwise       = id

assertInfixOf :: String -> ByteString -> Assertion
assertInfixOf needle haystack = Util.assertInfixOf needle (toString haystack)
