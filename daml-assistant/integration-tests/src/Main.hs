-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import qualified Codec.Archive.Tar as Tar
import qualified Codec.Archive.Zip as Zip
import Conduit hiding (connect)
import qualified Data.Conduit.Zlib as Zlib
import qualified Data.Conduit.Tar.Extra as Tar.Conduit
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Monad
import qualified Data.ByteString.Lazy as BSL
import Data.List.Extra
import qualified Data.Text as T
import Data.Typeable
import Network.HTTP.Client
import Network.HTTP.Types
import Network.Socket
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.Info.Extra
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
    oldPath <- getSearchPath
    javaPath <- locateRunfiles "local_jdk/bin"
    mvnPath <- locateRunfiles "mvn_dev_env/bin"
    sbtPath <- locateRunfiles "sbt_dev_env/bin"
    tarPath <- locateRunfiles "tar_dev_env/bin"
    let damlDir = tmpDir </> "daml"
    withEnv
        [ ("DAML_HOME", Just damlDir)
        , ("PATH", Just $ intercalate [searchPathSeparator] ((damlDir </> "bin") : tarPath : javaPath : mvnPath : sbtPath : oldPath))
        ] $ defaultMain (tests damlDir tmpDir)

tests :: FilePath -> FilePath -> TestTree
tests damlDir tmpDir = testGroup "Integration tests"
    [ testCase "install" $ do
          releaseTarball <- locateRunfiles (mainWorkspace </> "release" </> "sdk-release-tarball.tar.gz")
          createDirectory tarballDir
          runConduitRes
              $ sourceFileBS releaseTarball
              .| Zlib.ungzip
              .| Tar.Conduit.untar (Tar.Conduit.restoreFile throwError tarballDir)
          callProcessQuiet (tarballDir </> "daml" </> damlInstallerName) ["install", "--activate", "--set-path=no", tarballDir]
    , testCase "daml version" $ callProcessQuiet damlName ["version"]
    , testCase "daml --help" $ callProcessQuiet damlName ["--help"]
    , testCase "daml new --list" $ callProcessQuiet damlName ["new", "--list"]
    , noassistantTests damlDir
    , packagingTests tmpDir
    , quickstartJavaTests quickstartJavaDir mvnDir
    , quickstartScalaTests quickstartScalaDir
    , cleanTests cleanDir
    ]
    where quickstartJavaDir = tmpDir </> "quickstart-java"
          quickstartScalaDir = tmpDir </> "quickstart-scala"
          cleanDir = tmpDir </> "clean"
          mvnDir = tmpDir </> "m2"
          tarballDir = tmpDir </> "tarball"
          throwError msg e = fail (T.unpack $ msg <> " " <> e)

-- | These tests check that it is possible to invoke (a subset) of damlc
-- commands outside of the assistant.
noassistantTests :: FilePath -> TestTree
noassistantTests damlDir = testGroup "no assistant"
    [ testCase "damlc build" $ withTempDir $ \projDir -> do
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: a"
              , "version: \"1.0\""
              , "source: Main.daml"
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          writeFileUTF8 (projDir </> "Main.daml") $ unlines
              [ "daml 1.2"
              , "module Main where"
              , "a : ()"
              , "a = ()"
              ]
          let damlcPath = damlDir </> "sdk" </> sdkVersion </> "damlc" </> "da-hs-damlc-app"
          callProcess damlcPath ["build", "--project-root", projDir, "--init-package-db", "no"]
    ]

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
        withCurrentDirectory projectA $ callProcessQuiet damlName ["build"]
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
        withCurrentDirectory projectB $ callProcessQuiet damlName ["build"]
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
        withCurrentDirectory projDir $ callProcessQuiet damlName ["build"]
        let dar = projDir </> "dist" </> "proj.dar"
        assertBool "proj.dar was not created." =<< doesFileExist dar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile dar
        -- Note that we really want a forward slash here instead of </> since filepaths in
        -- zip files use forward slashes.
        assertBool "A.daml is missing" ("proj/A.daml" `elem` darFiles)
    , testCase "Project without exposed modules" $ withTempDir $ \projDir -> do
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
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
        withCurrentDirectory projDir $ callProcessQuiet damlName ["build"]
    ]

quickstartJavaTests :: FilePath -> FilePath -> TestTree
quickstartJavaTests quickstartDir mvnDir = testGroup "quickstart-java" $
    [ testCase "daml new" $
          callProcessQuiet damlName ["new", quickstartDir, "quickstart-java"]
    , testCase "daml build " $ withCurrentDirectory quickstartDir $
          callProcessQuiet damlName ["build", "-o", "target/daml/iou.dar"]
    ] <>
    (if isWindows then [] else -- We seem to have an issue where grpc_init sometimes locks up and the
    -- whole test times out. Tracked in https://github.com/digital-asset/daml/issues/1354
    [ testCase "daml test" $ withCurrentDirectory quickstartDir $
          callProcessQuiet damlName ["test"]
    , testCase "daml damlc test --files" $ withCurrentDirectory quickstartDir $
          callProcessQuiet damlName ["damlc", "test", "--files", "daml/Main.daml"]
    ]) <>
    [ testCase "sandbox startup" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull -> do
          p :: Int <- fromIntegral <$> getFreePort
          withCreateProcess ((proc damlName ["sandbox", "--port", show p, "target/daml/iou.dar"]) { std_out = UseHandle devNull }) $
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
              -- waitForProcess' will block on Windows so we explicitely kill the process.
              terminateProcess ph
    ] <>
    -- The mvn tests seem to fail on Windows for some reason so for now we disable them.
    -- mvn itself does seem to work fine outside of this test so it seems to be some
    -- setup issue.
    -- See https://github.com/digital-asset/daml/issues/1127
    if isWindows then [] else
    [ testCase "mvn compile" $
      withCurrentDirectory quickstartDir $ do
          mvnDbTarball <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "integration-tests" </> "integration-tests-mvn.tar")
          Tar.extract (takeDirectory mvnDir) mvnDbTarball
          callProcess "mvn" [mvnRepoFlag, "-q", "compile"]
    , testCase "mvn exec:java@run-quickstart" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull1 ->
      withDevNull $ \devNull2 -> do
          sandboxPort :: Int <- fromIntegral <$> getFreePort
          withCreateProcess ((proc damlName ["sandbox", "--", "--port", show sandboxPort, "--", "--scenario", "Main:setup", "target/daml/iou.dar"]) { std_out = UseHandle devNull1 }) $
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
                  -- waitForProcess' will block on Windows so we explicitely kill the process.
                  terminateProcess ph
              -- waitForProcess' will block on Windows so we explicitely kill the process.
              terminateProcess ph
    ]
    where
        mvnRepoFlag = "-Dmaven.repo.local=" <> mvnDir

quickstartScalaTests :: FilePath -> TestTree
quickstartScalaTests quickstartDir = testGroup "quickstart-scala"
    [ testCase "daml new" $
          callProcessQuiet damlName ["new", projDir, "quickstart-scala"]
    , testCase "daml build " $ withCurrentDirectory projDir $
          callProcessQuiet damlName ["build"]
    , testCase "sandxox and sbt" $ withCurrentDirectory projDir $
      withDevNull $ \devNull1 -> do
          sandboxPort :: Int <- fromIntegral <$> getFreePort
          withCreateProcess ((proc damlName ["sandbox", "--port", show sandboxPort, "--scenario", "Main:setup", darPath]) { std_out = UseHandle devNull1 }) $
              \_ _ _ ph -> race_ (waitForProcess' "sandbox" [] ph) $ do
                  waitForConnectionOnPort (threadDelay 500000) sandboxPort
                  callProcess "sbt" ["application/runMain com.digitalasset.quickstart.iou.IouMain localhost " <> show sandboxPort]
                  terminateProcess ph

    ] where
        projName = "proj"
        projDir = quickstartDir </> projName
        darPath = "dist" </> projName <.> "dar"

-- | Ensure that daml clean removes precisely the files created by daml build.
cleanTests :: FilePath -> TestTree
cleanTests baseDir = testGroup "daml clean"
    [ cleanTestFor "skeleton"
    , cleanTestFor "quickstart-java"
    , cleanTestFor "quickstart-scala"
    ]
    where
        cleanTestFor :: String -> TestTree
        cleanTestFor templateName =
            testCase ("daml clean test for " <> templateName <> " template") $ do
                createDirectoryIfMissing True baseDir
                withCurrentDirectory baseDir $ do
                    let projectDir = baseDir </> ("proj-" <> templateName)
                    callProcessQuiet damlName ["new", projectDir, templateName]
                    withCurrentDirectory projectDir $ do
                        filesAtStart <- sort <$> listFilesRecursive "."
                        callProcessQuiet damlName ["build"]
                        callProcessQuiet damlName ["clean"]
                        filesAtEnd <- sort <$> listFilesRecursive "."
                        when (filesAtStart /= filesAtEnd) $
                            fail $ unlines
                                [ "daml clean did not remove all files produced by daml build."
                                , ""
                                , "    files at start:"
                                , unlines (map ("       "++) filesAtStart)
                                , "    files at end:"
                                , unlines (map ("       "++) filesAtEnd)
                                ]

-- | Since we run in bash and not in cmd.exe "daml" won’t look for "daml.cmd" so we use "daml.cmd" directly.
damlName :: String
damlName
    | isWindows = "daml.cmd"
    | otherwise = "daml"

damlInstallerName :: String
damlInstallerName
    | isWindows = "daml.exe"
    | otherwise = "daml"

-- | Like call process but hides stdout.
callProcessQuiet :: FilePath -> [String] -> IO ()
callProcessQuiet cmd args = do
    (exit, _out, err) <- readCreateProcessWithExitCode (proc cmd args) ""
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
