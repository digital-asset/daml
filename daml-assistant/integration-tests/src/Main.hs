-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import qualified Codec.Archive.Tar as Tar
import qualified Codec.Archive.Zip as Zip
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Monad
import qualified Data.ByteString.Lazy as BSL
import Data.Typeable
import Network.HTTP.Client
import Network.HTTP.Types
import Network.Socket
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Main
import Test.Tasty
import Test.Tasty.HUnit

import DA.Bazel.Runfiles
import DamlHelper
import SdkVersion

main :: IO ()
main =
    withTempDir $ \tmpDir -> do
    -- We manipulate global state via the working directory and
    -- the environment so running tests in parallel will cause trouble.
    setEnv "TASTY_NUM_THREADS" "1" True
    oldPath <- getEnv "PATH"
    javaPath <- locateRunfiles "local_jdk/bin"
    mvnPath <- locateRunfiles "mvn_nix/bin"
    let damlDir = tmpDir </> "daml"
    withEnv
        [ ("DAML_HOME", Just damlDir)
        , ("PATH", Just $ (damlDir </> "bin") <> ":" <> javaPath <> ":" <> mvnPath <> maybe "" (":" <>) oldPath)
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
    , packagingTests tmpDir
    , quickstartTests quickstartDir mvnDir
    ]
    where quickstartDir = tmpDir </> "quickstart"
          mvnDir = tmpDir </> "m2"
          tarballDir = tmpDir </> "tarball"


packagingTests :: FilePath -> TestTree
packagingTests tmpDir = testGroup "packaging"
    [ testCaseSteps "Build package with dependency" $ \step -> do
        let projectA = tmpDir </> "a"
        let projectB = tmpDir </> "b"
        let aDar = projectA </> "dist" </> "a.dar"
        let bDar = projectB </> "dist" </> "b.dar"
        step "Creating project a..."
        createDirectoryIfMissing True (projectA </> "daml")
        writeFileUTF8 (projectA </> "daml" </> "A.daml") $ unlines
            [ "daml 1.2"
            , "module A (a) where"
            , "a : ()"
            , "a = ()"
            ]
        writeFileUTF8 (projectA </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: \"1.0\""
            , "source: daml/A.daml"
            , "exposed-modules: [A]"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            ]
        withCurrentDirectory projectA $ callProcessQuiet "daml" ["build"]
        assertBool "a.dar was not created." =<< doesFileExist aDar
        step "Creating project b..."
        createDirectoryIfMissing True (projectB </> "daml")
        writeFileUTF8 (projectB </> "daml" </> "B.daml") $ unlines
            [ "daml 1.2"
            , "module B where"
            , "import A"
            , "b : ()"
            , "b = a"
            ]
        writeFileUTF8 (projectB </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "version: \"1.0\""
            , "name: b"
            , "source: daml/B.daml"
            , "exposed-modules: [B]"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            , "  - " <> aDar
            ]
        withCurrentDirectory projectB $ callProcessQuiet "daml" ["build"]
        assertBool "b.dar was not created." =<< doesFileExist bDar
    , testCase "Top-level source files" $ do
        -- Test that a source file in the project root will be included in the
        -- DAR file. Regression test for #1048.
        let projDir = tmpDir </> "proj"
        createDirectoryIfMissing True projDir
        writeFileUTF8 (projDir </> "A.daml") $ unlines
          [ "daml 1.2"
          , "module A (a) where"
          , "a : ()"
          , "a = ()"
          ]
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> sdkVersion
          , "name: proj"
          , "version: \"1.0\""
          , "source: A.daml"
          , "exposed-modules: [A]"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          ]
        withCurrentDirectory projDir $ callProcessQuiet "daml" ["build"]
        let dar = projDir </> "dist" </> "proj.dar"
        assertBool "proj.dar was not created." =<< doesFileExist dar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile dar
        assertBool "A.daml is missing" (("proj" </> "A.daml") `elem` darFiles)
    ]

quickstartTests :: FilePath -> FilePath -> TestTree
quickstartTests quickstartDir mvnDir = testGroup "quickstart"
    [ testCase "daml new" $
          callProcessQuiet "daml" ["new", quickstartDir, "quickstart-java"]
    , testCase "daml build " $ withCurrentDirectory quickstartDir $
          callProcessQuiet "daml" ["build", "-o", "target/daml/iou.dar"]
    , testCase "daml damlc test" $ withCurrentDirectory quickstartDir $
          callProcessQuiet "daml" ["damlc", "test", "daml/Main.daml"]
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
