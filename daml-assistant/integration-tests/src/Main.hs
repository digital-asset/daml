-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import qualified Codec.Archive.Tar as Tar
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Monad
import Data.Typeable
import Network.HTTP.Client
import Network.HTTP.Types
import Network.Socket
import System.Directory.Extra
import System.Environment
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Main
import Test.Tasty
import Test.Tasty.HUnit

import DA.Bazel.Runfiles
import DamlHelper

main :: IO ()
main =
    withTempDir $ \tmpDir -> do
    -- We manipulate global state via the working directory and
    -- the environment so running tests in parallel will cause trouble.
    setEnv "TASTY_NUM_THREADS" "1"
    oldPath <- getEnv "PATH"
    javaPath <- locateRunfiles "local_jdk/bin"
    mvnPath <- locateRunfiles "mvn_nix/bin"
    let damlDir = tmpDir </> "daml"
    withEnv
        [ ("DAML_HOME", Just damlDir)
        , ("PATH", Just $ (damlDir </> "bin") <> ":" <> javaPath <> ":" <> mvnPath <> ":" <> oldPath)
        ] $ defaultMain (tests tmpDir)

tests :: FilePath -> TestTree
tests tmpDir = testGroup "Integration tests"
    [ testCase "install" $ do
          releaseTarball <- locateRunfiles (mainWorkspace </> "release" </> "sdk-release-tarball.tar.gz")
          createDirectory tarballDir
          callProcessQuiet "tar" ["xf", releaseTarball, "--strip-components=1", "-C", tarballDir]
          callProcessQuiet (tarballDir </> "install.sh") []
    , testCase "daml version" $ callProcessQuiet "daml" ["version"]
    , testCase "daml --help" $ callProcessQuiet "daml" ["--help"]
    , testCase "daml new --list" $ callProcessQuiet "daml" ["new", "--list"]
    , quickstartTests quickstartDir mvnDir
    ]
    where quickstartDir = tmpDir </> "quickstart"
          mvnDir = tmpDir </> "m2"
          tarballDir = tmpDir </> "tarball"

quickstartTests :: FilePath -> FilePath -> TestTree
quickstartTests quickstartDir mvnDir = testGroup "quickstart"
    [ testCase "daml new" $
          callProcessQuiet "daml" ["new", quickstartDir]
    , testCase "daml init" $ withCurrentDirectory quickstartDir $
          callProcessQuiet "daml" ["init"]
    , testCase "daml package" $ withCurrentDirectory quickstartDir $
          -- This location is assumed by the codegen in the quickstart example.
          callProcessQuiet "daml" ["package", "-o", "target/daml/iou.dar"]
    , testCase "daml build " $ withCurrentDirectory quickstartDir $
          callProcessQuiet "daml" ["build"]
    , testCase "daml test" $ withCurrentDirectory quickstartDir $
          callProcessQuiet "daml" ["test", "daml/Main.daml"]
    , testCase "sandbox startup" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull -> do
          p :: Int <- fromIntegral <$> getFreePort
          withCreateProcess ((proc "daml" ["sandbox", "--port", show p, "dist/quickstart.dar"]) { std_out = UseHandle devNull }) $
              \_ _ _ ph -> race_ (waitForProcess' "sandbox" [] ph) $ do
              waitForConnectionOnPort (threadDelay 100000) p
              addr : _ <- getAddrInfo
                  (Just socketHints)
                  (Just "127.0.0.1")
                  (Just $ show p)
              bracket
                  (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
                  close
                  (\s -> connect s (addrAddress addr))
    , testCase "mvn compile" $
      withCurrentDirectory quickstartDir $ do
          mvnDbTarball <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "integration-tests" </> "integration-tests-mvn.tar")
          Tar.extract (takeDirectory mvnDir) mvnDbTarball
          callProcess "mvn" [mvnRepoFlag, "-q", "compile"]
    , testCase "mvn exec:java@run-quickstart" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull1 ->
      withDevNull $ \devNull2 -> do
          sandboxPort :: Int <- fromIntegral <$> getFreePort
          withCreateProcess ((proc "daml" ["sandbox", "--", "--port", show sandboxPort, "--", "--scenario", "Main:setup", "target/daml/iou.dar"]) { std_out = UseHandle devNull1 }) $
              \_ _ _ ph -> race_ (waitForProcess' "sandbox" [] ph) $ do
              waitForConnectionOnPort (threadDelay 500000) sandboxPort
              restPort :: Int <- fromIntegral <$> getFreePort
              withCreateProcess ((proc "mvn" [mvnRepoFlag, "-Dledgerport=" <> show sandboxPort, "-Drestport=" <> show restPort, "exec:java@run-quickstart"]) { std_out = UseHandle devNull2 }) $
                  \_ _ _ ph -> race_ (waitForProcess' "mvn" [] ph) $ do
                  let url = "http://localhost:" <> show restPort <> "/iou"
                  waitForHttpServer (threadDelay 1000000) url
                  threadDelay 5000000
                  manager <- newManager defaultManagerSettings
                  req <- parseRequest url
                  req <- pure req { requestHeaders = [(hContentType, "application/json")] }
                  resp <- httpLbs req manager
                  responseBody resp @?=
                      "{\"0\":{\"issuer\":\"EUR_Bank\",\"owner\":\"Alice\",\"currency\":\"EUR\",\"amount\":100.0,\"observers\":[]}}"
    ]
    where
        mvnRepoFlag = "-Dmaven.repo.local=" <> mvnDir

-- | Like call process but hides stdout.
callProcessQuiet :: FilePath -> [String] -> IO ()
callProcessQuiet cmd args = do
    (exit, _out, err) <- readProcessWithExitCode cmd args ""
    hPutStr stderr err
    unless (exit == ExitSuccess) $ throwIO $ ProcessExitFailure exit cmd args

data ProcessExitFailure = ProcessExitFailure !ExitCode !FilePath ![String]
    deriving (Show, Typeable)

instance Exception ProcessExitFailure

-- This is slightly hacky: we need to find a free port but pass it to an
-- external process. Technically this port could be reused between us
-- getting it from the kernel and the external process listening
-- on that port but ports are usually not reused aggressively so this should
-- be fine and is certainly better than hardcoding the port.
getFreePort :: IO PortNumber
getFreePort = do
    addr : _ <- getAddrInfo
        (Just socketHints)
        (Just "127.0.0.1")
        (Just "0")
    bracket
        (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
        close
        (\s -> do bind s (addrAddress addr)
                  name <- getSocketName s
                  case name of
                      SockAddrInet p _ -> pure p
                      _ -> fail $ "Expected a SockAddrInet but got " <> show name)

socketHints :: AddrInfo
socketHints = defaultHints { addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream }

-- | Like waitForProcess' but throws ProcessExitFailure if the process fails to start.
waitForProcess' :: String -> [String] -> ProcessHandle -> IO ()
waitForProcess' cmd args ph = do
    e <- waitForProcess ph
    unless (e == ExitSuccess) $ throwIO $ ProcessExitFailure e cmd args

-- | Getting a dev-null handle in a cross-platform way seems to be somewhat tricky so we instead
-- use a temporary file.
withDevNull :: (Handle -> IO a) -> IO a
withDevNull a = withTempFile $ \f -> withFile f WriteMode a
