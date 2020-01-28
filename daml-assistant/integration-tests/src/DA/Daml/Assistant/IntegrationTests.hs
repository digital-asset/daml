-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.IntegrationTests (main) where

import Conduit hiding (connect)
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Monad
import Control.Monad.Fail (MonadFail)
import qualified Data.Aeson.Types as Aeson
import qualified Data.Conduit.Tar.Extra as Tar.Conduit.Extra
import qualified Data.Conduit.Zlib as Zlib
import Data.List.Extra
import qualified Data.Map as Map
import Data.Maybe (maybeToList)
import qualified Data.Text as T
import Data.Typeable
import Network.HTTP.Client
import Network.HTTP.Types
import Network.Socket
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import System.Process
import Test.Main
import Test.Tasty
import Test.Tasty.HUnit
import qualified Web.JWT as JWT

import DA.Bazel.Runfiles
import DA.Daml.Helper.Run
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
    tarPath <- locateRunfiles "tar_dev_env/bin"
    -- NOTE: `COMSPEC` env. variable on Windows points to cmd.exe, which is required to be present
    -- on the PATH as mvn.cmd executes cmd.exe
    mbComSpec <- getEnv "COMSPEC"
    let mbCmdDir = takeDirectory <$> mbComSpec
    let damlDir = tmpDir </> "daml"
    withEnv
        [ ("DAML_HOME", Just damlDir)
        , ("PATH", Just $ intercalate [searchPathSeparator] $ ((damlDir </> "bin") : tarPath : javaPath : mvnPath : oldPath) ++ maybeToList mbCmdDir)
        ] $ defaultMain (tests damlDir tmpDir)

tests :: FilePath -> FilePath -> TestTree
tests damlDir tmpDir = testGroup "Integration tests"
    [ testCase "install" $ do
        releaseTarball <- locateRunfiles (mainWorkspace </> "release" </> "sdk-release-tarball.tar.gz")
        createDirectory tarballDir
        runConduitRes
            $ sourceFileBS releaseTarball
            .| Zlib.ungzip
            .| Tar.Conduit.Extra.untar (Tar.Conduit.Extra.restoreFile throwError tarballDir)
        if isWindows
            then callProcessQuiet
                (tarballDir </> "daml" </> damlInstallerName)
                ["install", "--install-assistant=yes", "--set-path=no", tarballDir]
            else callCommandQuiet $ tarballDir </> "install.sh"
    , testCase "daml version" $ callCommandQuiet "daml version"
    , testCase "daml --help" $ callCommandQuiet "daml --help"
    , testCase "daml new --list" $ callCommandQuiet "daml new --list"
    , noassistantTests damlDir
    , packagingTests
    , quickstartTests quickstartDir mvnDir
    , cleanTests cleanDir
    , deployTest deployDir
    , codegenTests codegenDir
    ]
    where quickstartDir = tmpDir </> "q-u-i-c-k-s-t-a-r-t"
          cleanDir = tmpDir </> "clean"
          mvnDir = tmpDir </> "m2"
          tarballDir = tmpDir </> "tarball"
          deployDir = tmpDir </> "deploy"
          codegenDir = tmpDir </> "codegen"

throwError :: MonadFail m => T.Text -> T.Text -> m ()
throwError msg e = fail (T.unpack $ msg <> " " <> e)

-- | These tests check that it is possible to invoke (a subset) of damlc
-- commands outside of the assistant.
noassistantTests :: FilePath -> TestTree
noassistantTests damlDir = testGroup "no assistant"
    [ testCase "damlc build --init-package-db=no" $ withTempDir $ \projDir -> do
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
          let damlcPath = damlDir </> "sdk" </> sdkVersion </> "damlc" </> "damlc"
          callProcess damlcPath ["build", "--project-root", projDir, "--init-package-db", "no"]
    , testCase "damlc build --init-package-db=yes" $ withTempDir $ \tmpDir -> do
          let projDir = tmpDir </> "foobar"
          createDirectory projDir
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
          let damlcPath = damlDir </> "sdk" </> sdkVersion </> "damlc" </> "damlc"
          withCurrentDirectory tmpDir $
              callProcess damlcPath ["build", "--project-root", "foobar", "--init-package-db", "yes"]
    ]

-- Most of the packaging tests are in the a separate test suite in
-- //compiler/damlc/tests:packaging. This only has a couple of
-- integration tests.
packagingTests :: TestTree
packagingTests = testGroup "packaging"
     [ testCase "Build copy trigger" $ withTempDir $ \tmpDir -> do
        let projDir = tmpDir </> "copy-trigger"
        callCommandQuiet $ unwords ["daml", "new", projDir, "copy-trigger"]
        withCurrentDirectory projDir $ callCommandQuiet "daml build"
        let dar = projDir </> ".daml" </> "dist" </> "copy-trigger-0.0.1.dar"
        assertBool "copy-trigger-0.1.0.dar was not created." =<< doesFileExist dar
     , testCase "Build trigger with extra dependency" $ withTempDir $ \tmpDir -> do
        let myDepDir = tmpDir </> "mydep"
        createDirectoryIfMissing True (myDepDir </> "daml")
        writeFileUTF8 (myDepDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: mydep"
            , "version: \"1.0\""
            , "source: daml"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            ]
        writeFileUTF8 (myDepDir </> "daml" </> "MyDep.daml") $ unlines
          [ "daml 1.2"
          , "module MyDep where"
          ]
        withCurrentDirectory myDepDir $ callCommandQuiet "daml build -o mydep.dar"
        let myTriggerDir = tmpDir </> "mytrigger"
        createDirectoryIfMissing True (myTriggerDir </> "daml")
        writeFileUTF8 (myTriggerDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: mytrigger"
            , "version: \"1.0\""
            , "source: daml"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            , "  - daml-trigger"
            , "  - " <> myDepDir </> "mydep.dar"
            ]
        writeFileUTF8 (myTriggerDir </> "daml/Main.daml") $ unlines
            [ "daml 1.2"
            , "module Main where"
            , "import MyDep ()"
            , "import Daml.Trigger ()"
            ]
        withCurrentDirectory myTriggerDir $ callCommandQuiet "daml build -o mytrigger.dar"
        let dar = myTriggerDir </> "mytrigger.dar"
        assertBool "mytrigger.dar was not created." =<< doesFileExist dar
     , testCase "Build DAML script example"  $ withTempDir $ \tmpDir -> do
        let projDir = tmpDir </> "script-example"
        callCommandQuiet $ unwords ["daml", "new", projDir, "script-example"]
        withCurrentDirectory projDir $ callCommandQuiet "daml build"
        let dar = projDir </> ".daml/dist/script-example-0.0.1.dar"
        assertBool "script-example-0.0.1.dar was not created." =<< doesFileExist dar
    -- Note(MK): The hacks around daml-prim which were already not quite right, e.g.,
    -- we didn’t include daml-prim from all SDK versions, are broken completely
    -- now that we split daml-prim into multiple packages. Therefore, we
    -- disable this for now.

    -- See https://github.com/digital-asset/daml/issues/3704

    --  , testCaseSteps "Build migration package" $ \step -> withTempDir $ \tmpDir -> do
    --     -- it's important that we have fresh empty directories here!
    --     let projectA = tmpDir </> "a-1.0"
    --     let projectB = tmpDir </> "a-2.0"
    --     let projectUpgrade = tmpDir </> "upgrade"
    --     let projectRollback = tmpDir </> "rollback"
    --     let aDar = projectA </> "projecta.dar"
    --     let bDar = projectB </> "projectb.dar"
    --     let upgradeDar = projectUpgrade </> distDir </> "upgrade-0.0.1.dar"
    --     let rollbackDar= projectRollback </> distDir </> "rollback-0.0.1.dar"
    --     let bWithUpgradesDar = "a-2.0-with-upgrades.dar"
    --     step "Creating project a-1.0 ..."
    --     createDirectoryIfMissing True (projectA </> "daml")
    --     writeFileUTF8 (projectA </> "daml" </> "Main.daml") $ unlines
    --         [ "{-# LANGUAGE EmptyCase #-}"
    --         , "daml 1.2"
    --         , "module Main where"
    --         , "data OnlyA"
    --         , "data Both"
    --         , "template Foo"
    --         , "  with"
    --         , "    a : Int"
    --         , "    p : Party"
    --         , "  where"
    --         , "    signatory p"
    --         ]
    --     writeFileUTF8 (projectA </> "daml.yaml") $ unlines
    --         [ "sdk-version: " <> sdkVersion
    --         , "name: a"
    --         , "version: \"1.0\""
    --         , "source: daml"
    --         , "exposed-modules: [Main]"
    --         , "dependencies:"
    --         , "  - daml-prim"
    --         , "  - daml-stdlib"
    --         ]
    --     -- We use -o to test that we do not depend on the name of the dar
    --     withCurrentDirectory projectA $ callCommandQuiet $ "daml build -o " <> aDar
    --     assertBool "a-1.0.dar was not created." =<< doesFileExist aDar
    --     step "Creating project a-2.0 ..."
    --     createDirectoryIfMissing True (projectB </> "daml")
    --     writeFileUTF8 (projectB </> "daml" </> "Main.daml") $ unlines
    --         [ "daml 1.2"
    --         , "module Main where"
    --         , "data OnlyB"
    --         , "data Both"
    --         , "template Foo"
    --         , "  with"
    --         , "    a : Int"
    --         , "    p : Party"
    --         , "  where"
    --         , "    signatory p"
    --         ]
    --     writeFileUTF8 (projectB </> "daml.yaml") $ unlines
    --         [ "sdk-version: " <> sdkVersion
    --         , "name: a"
    --         , "version: \"2.0\""
    --         , "source: daml"
    --         , "exposed-modules: [Main]"
    --         , "dependencies:"
    --         , "  - daml-prim"
    --         , "  - daml-stdlib"
    --         ]
    --     -- We use -o to test that we do not depend on the name of the dar
    --     withCurrentDirectory projectB $ callCommandQuiet $ "daml build -o " <> bDar
    --     assertBool "a-2.0.dar was not created." =<< doesFileExist bDar
    --     step "Creating upgrade/rollback project"
    --     -- We use -o to verify that we do not depend on the
    --     callCommandQuiet $ unwords ["daml", "migrate", projectUpgrade, aDar, bDar]
    --     callCommandQuiet $ unwords ["daml", "migrate", projectRollback, bDar, aDar]
    --     step "Build migration project"
    --     withCurrentDirectory projectUpgrade $
    --         callCommandQuiet "daml build"
    --     assertBool "upgrade-0.0.1.dar was not created" =<< doesFileExist upgradeDar
    --     step "Build rollback project"
    --     withCurrentDirectory projectRollback $
    --         callCommandQuiet "daml build"
    --     assertBool "rollback-0.0.1.dar was not created" =<< doesFileExist rollbackDar
    --     step "Merging upgrade dar"
    --     callCommandQuiet $
    --       unwords
    --           [ "daml damlc merge-dars"
    --           , bDar
    --           , upgradeDar
    --           , "--dar-name"
    --           , bWithUpgradesDar
    --           ]
    --     assertBool "a-0.2-with-upgrades.dar was not created." =<< doesFileExist bWithUpgradesDar
    --   , testCaseSteps "Build migration package with generics" $ \step -> withTempDir $ \tmpDir -> do
    --     -- it's important that we have fresh empty directories here!
    --     let projectA = tmpDir </> "a-1.0"
    --     let projectB = tmpDir </> "a-2.0"
    --     let projectUpgrade = tmpDir </> "upgrade"
    --     let aDar = projectA </> "projecta.dar"
    --     let bDar = projectB </> "projectb.dar"
    --     let upgradeDar = projectUpgrade </> distDir </> "upgrade-0.0.1.dar"
    --     step "Creating project a-1.0 ..."
    --     createDirectoryIfMissing True (projectA </> "daml")
    --     writeFileUTF8 (projectA </> "daml" </> "Main.daml") $ unlines
    --         [ "{-# LANGUAGE EmptyCase #-}"
    --         , "daml 1.2"
    --         , "module Main where"
    --         , "data OnlyA"
    --         , "data Both"
    --         , "template Foo"
    --         , "  with"
    --         , "    a : Int"
    --         , "    p : Party"
    --         , "  where"
    --         , "    signatory p"
    --         ]
    --     writeFileUTF8 (projectA </> "daml.yaml") $ unlines
    --         [ "sdk-version: " <> sdkVersion
    --         , "name: a"
    --         , "version: \"1.0\""
    --         , "source: daml"
    --         , "exposed-modules: [Main]"
    --         , "dependencies:"
    --         , "  - daml-prim"
    --         , "  - daml-stdlib"
    --         ]
    --     -- We use -o to test that we do not depend on the name of the dar
    --     withCurrentDirectory projectA $ callCommandQuiet $ "daml build -o " <> aDar
    --     assertBool "a-1.0.dar was not created." =<< doesFileExist aDar
    --     step "Creating project a-2.0 ..."
    --     createDirectoryIfMissing True (projectB </> "daml")
    --     writeFileUTF8 (projectB </> "daml" </> "Main.daml") $ unlines
    --         [ "daml 1.2"
    --         , "module Main where"
    --         , "data OnlyB"
    --         , "data Both"
    --         , "template Foo"
    --         , "  with"
    --         , "    a : Int"
    --         , "    p : Party"
    --         , "    new : Optional Text"
    --         , "  where"
    --         , "    signatory p"
    --         ]
    --     writeFileUTF8 (projectB </> "daml.yaml") $ unlines
    --         [ "sdk-version: " <> sdkVersion
    --         , "name: a"
    --         , "version: \"2.0\""
    --         , "source: daml"
    --         , "exposed-modules: [Main]"
    --         , "dependencies:"
    --         , "  - daml-prim"
    --         , "  - daml-stdlib"
    --         ]
    --     -- We use -o to test that we do not depend on the name of the dar
    --     withCurrentDirectory projectB $ callCommandQuiet $ "daml build -o " <> bDar
    --     assertBool "a-2.0.dar was not created." =<< doesFileExist bDar
    --     step "Creating upgrade/rollback project"
    --     callCommandQuiet $ unwords ["daml", "migrate", projectUpgrade, aDar, bDar]
    --     step "Generate generic instances"
    --     writeFileUTF8 (projectUpgrade </> "daml" </> "Main.daml") $ unlines
    --        [ "daml 1.2"
    --        , "module Main where"
    --        , "import MainA qualified as A"
    --        , "import MainB qualified as B"
    --        , "import MainAGenInstances()"
    --        , "import MainBGenInstances()"
    --        , "import DA.Upgrade"
    --        , "import DA.Generics"
    --        , "instance Convertible A.Foo B.Foo"
    --        , "instance Convertible B.Foo A.Foo"
    --        ]
    --     callCommandQuiet $
    --         unwords
    --             [ "daml"
    --             , "damlc"
    --             , "generate-generic-src"
    --             , "--srcdir"
    --             , projectUpgrade </> "daml"
    --             , "--qualify"
    --             , "A"
    --             , aDar
    --             ]
    --     callCommandQuiet $
    --         unwords
    --             [ "daml"
    --             , "damlc"
    --             , "generate-generic-src"
    --             , "--srcdir"
    --             , projectUpgrade </> "daml"
    --             , "--qualify"
    --             , "B"
    --             , bDar
    --             ]
    --     step "Build migration project"
    --     withCurrentDirectory projectUpgrade $
    --         callCommandQuiet "daml build --generated-src"
    --     assertBool "upgrade-0.0.1.dar was not created" =<< doesFileExist upgradeDar

    -- , testCaseSteps "Build migration package in LF 1.dev with Numerics" $ \step -> withTempDir $ \tmpDir -> do
    --     -- it's important that we have fresh empty directories here!
    --     let projectA = tmpDir </> "a-1.0"
    --     let projectB = tmpDir </> "a-2.0"
    --     let projectUpgrade = tmpDir </> "upgrade"
    --     let projectRollback = tmpDir </> "rollback"
    --     let aDar = projectA </> "projecta.dar"
    --     let bDar = projectB </> "projectb.dar"
    --     let upgradeDar = projectUpgrade </> distDir </> "upgrade-0.0.1.dar"
    --     let rollbackDar= projectRollback </> distDir </> "rollback-0.0.1.dar"
    --     let bWithUpgradesDar = "a-2.0-with-upgrades.dar"
    --     step "Creating project a-1.0 ..."
    --     createDirectoryIfMissing True (projectA </> "daml")
    --     writeFileUTF8 (projectA </> "daml" </> "Main.daml") $ unlines
    --         [ "daml 1.2"
    --         , "module Main where"
    --         , "data OnlyA"
    --         , "data Both"
    --         , "template Foo"
    --         , "  with"
    --         , "    a : Numeric 5"
    --         , "    p : Party"
    --         , "  where"
    --         , "    signatory p"
    --         ]
    --     writeFileUTF8 (projectA </> "daml.yaml") $ unlines
    --         [ "sdk-version: " <> sdkVersion
    --         , "name: a"
    --         , "version: \"1.0\""
    --         , "source: daml"
    --         , "exposed-modules: [Main]"
    --         , "dependencies:"
    --         , "  - daml-prim"
    --         , "  - daml-stdlib"
    --         ]
    --     -- We use -o to test that we do not depend on the name of the dar
    --     withCurrentDirectory projectA $ callCommandQuiet $ "daml build --target 1.dev -o " <> aDar
    --     assertBool "a-1.0.dar was not created." =<< doesFileExist aDar
    --     step "Creating project a-2.0 ..."
    --     createDirectoryIfMissing True (projectB </> "daml")
    --     writeFileUTF8 (projectB </> "daml" </> "Main.daml") $ unlines
    --         [ "daml 1.2"
    --         , "module Main where"
    --         , "data OnlyB"
    --         , "data Both"
    --         , "template Foo"
    --         , "  with"
    --         , "    a : Numeric 5"
    --         , "    p : Party"
    --         , "  where"
    --         , "    signatory p"
    --         ]
    --     writeFileUTF8 (projectB </> "daml.yaml") $ unlines
    --         [ "sdk-version: " <> sdkVersion
    --         , "name: a"
    --         , "version: \"2.0\""
    --         , "source: daml"
    --         , "exposed-modules: [Main]"
    --         , "dependencies:"
    --         , "  - daml-prim"
    --         , "  - daml-stdlib"
    --         ]
    --     -- We use -o to test that we do not depend on the name of the dar
    --     withCurrentDirectory projectB $ callCommandQuiet $ "daml build --target 1.dev -o " <> bDar
    --     assertBool "a-2.0.dar was not created." =<< doesFileExist bDar
    --     step "Creating upgrade/rollback project"
    --     -- We use -o to verify that we do not depend on the
    --     callCommandQuiet $ unwords ["daml", "migrate", projectUpgrade, aDar, bDar]
    --     callCommandQuiet $ unwords ["daml", "migrate", projectRollback, bDar, aDar]
    --     step "Build migration project"
    --     withCurrentDirectory projectUpgrade $
    --         callCommandQuiet "daml build --target 1.dev"
    --     assertBool "upgrade-0.0.1.dar was not created" =<< doesFileExist upgradeDar
    --     step "Build rollback project"
    --     withCurrentDirectory projectRollback $
    --         callCommandQuiet "daml build --target 1.dev"
    --     assertBool "rollback-0.0.1.dar was not created" =<< doesFileExist rollbackDar
    --     step "Merging upgrade dar"
    --     callCommandQuiet $
    --       unwords
    --           [ "daml damlc merge-dars"
    --           , bDar
    --           , upgradeDar
    --           , "--dar-name"
    --           , bWithUpgradesDar
    --           ]
    --     assertBool "a-0.2-with-upgrades.dar was not created." =<< doesFileExist bWithUpgradesDar
    ]

quickstartTests :: FilePath -> FilePath -> TestTree
quickstartTests quickstartDir mvnDir = testGroup "quickstart"
    [ testCase "daml new" $
          callCommandQuiet $ unwords ["daml", "new", quickstartDir, "quickstart-java"]
    , testCase "daml build " $ withCurrentDirectory quickstartDir $
          callCommandQuiet "daml build"
    , testCase "daml test" $ withCurrentDirectory quickstartDir $
          callCommandQuiet "daml test"
    , testCase "daml damlc test --files" $ withCurrentDirectory quickstartDir $
          callCommandQuiet "daml damlc test --files daml/Main.daml"
    , testCase "daml damlc visual-web" $ withCurrentDirectory quickstartDir $
          callCommandQuiet $ unwords ["daml damlc visual-web .daml/dist/quickstart-0.0.1.dar -o visual.html -b"]
    , testCase "sandbox startup" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull -> do
          p :: Int <- fromIntegral <$> getFreePort
          let sandboxProc = (shell $ unwords ["daml", "sandbox", "--port", show p, ".daml/dist/quickstart-0.0.1.dar"]) { std_out = UseHandle devNull }
          withCreateProcess sandboxProc  $
              \_ _ _ ph -> race_ (waitForProcess' sandboxProc ph) $ do
              waitForConnectionOnPort (threadDelay 100000) p
              addr : _ <- getAddrInfo
                  (Just socketHints)
                  (Just "127.0.0.1")
                  (Just $ show p)
              bracket
                  (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
                  close
                  (\s -> connect s (addrAddress addr))
              -- waitForProcess' will block on Windows so we explicitly kill the process.
              terminateProcess ph
    , testCase "Navigator startup" $
    -- This test just checks that navigator starts up and returns a 200 response.
    -- Nevertheless this would have caught a few issues on rules_nodejs upgrades
    -- where we got a 404 instead.
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull1 -> do
      withDevNull $ \devNull2 -> do
          sandboxPort :: Int <- fromIntegral <$> getFreePort
          let sandboxProc = (shell $ unwords ["daml", "sandbox", "--port", show sandboxPort, ".daml/dist/quickstart-0.0.1.dar"]) { std_out = UseHandle devNull1 }
          withCreateProcess sandboxProc  $ \_ _ _ sandboxPh -> race_ (waitForProcess' sandboxProc sandboxPh) $ do
              waitForConnectionOnPort (threadDelay 100000) sandboxPort
              navigatorPort :: Int <- fromIntegral <$> getFreePort
              let navigatorProc = (shell $ unwords ["daml", "navigator", "server", "localhost", show sandboxPort, "--port", show navigatorPort]) { std_out = UseHandle devNull2 }
              withCreateProcess navigatorProc $ \_ _ _ navigatorPh -> race_ (waitForProcess' navigatorProc navigatorPh) $ do
                  -- waitForHttpServer will only return once we get a 200 response so we don’t need to do anything else.
                  waitForHttpServer (threadDelay 100000) ("http://localhost:" <> show navigatorPort) []
                  -- waitForProcess' will block on Windows so we explicitly kill the process.
                  terminateProcess navigatorPh
              terminateProcess sandboxPh
    , testCase "JSON API startup" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull1 -> do
      withDevNull $ \devNull2 -> do
          sandboxPort :: Int <- fromIntegral <$> getFreePort
          let sandboxProc = (shell $ unwords ["daml", "sandbox", "--port", show sandboxPort, ".daml/dist/quickstart-0.0.1.dar"]) { std_out = UseHandle devNull1 }
          withCreateProcess sandboxProc  $ \_ _ _ sandboxPh -> race_ (waitForProcess' sandboxProc sandboxPh) $ do
              waitForConnectionOnPort (threadDelay 100000) sandboxPort
              jsonApiPort :: Int <- fromIntegral <$> getFreePort
              let jsonApiProc = (shell $ unwords ["daml", "json-api", "--ledger-host", "localhost", "--ledger-port", show sandboxPort, "--http-port", show jsonApiPort]) { std_out = UseHandle devNull2 }
              withCreateProcess jsonApiProc $ \_ _ _ jsonApiPh -> race_ (waitForProcess' jsonApiProc jsonApiPh) $ do
                  let headers =
                          [ ("Authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWRnZXJJZCI6Ik15TGVkZ2VyIiwiYXBwbGljYXRpb25JZCI6ImZvb2JhciIsInBhcnR5IjoiQWxpY2UifQ.4HYfzjlYr1ApUDot0a6a4zB49zS_jrwRUOCkAiPMqo0")
                          ] :: RequestHeaders
                  waitForHttpServer (threadDelay 100000) ("http://localhost:" <> show jsonApiPort <> "/contracts/search") headers
                  req <- parseRequest $ "http://localhost:" <> show jsonApiPort <> "/contracts/search"
                  req <- pure req { requestHeaders = headers }
                  manager <- newManager defaultManagerSettings
                  resp <- httpLbs req manager
                  responseBody resp @?=
                      "{\"result\":[],\"status\":200}"
                  -- waitForProcess' will block on Windows so we explicitly kill the process.
                  terminateProcess jsonApiPh
              terminateProcess sandboxPh
    , testCase "mvn compile" $
      withCurrentDirectory quickstartDir $ do
          mvnDbTarball <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "integration-tests" </> "integration-tests-mvn.tar")
          runConduitRes
            $ sourceFileBS mvnDbTarball
            .| Tar.Conduit.Extra.untar (Tar.Conduit.Extra.restoreFile throwError mvnDir)
          callCommand $ unwords ["mvn", mvnRepoFlag, "-q", "compile"]
    , testCase "mvn exec:java@run-quickstart" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull1 ->
      withDevNull $ \devNull2 -> do
          sandboxPort :: Int <- fromIntegral <$> getFreePort
          let sandboxProc = (shell $ unwords ["daml", "sandbox", "--", "--port", show sandboxPort, "--", "--scenario", "Main:setup", ".daml/dist/quickstart-0.0.1.dar"]) { std_out = UseHandle devNull1 }
          withCreateProcess sandboxProc $
              \_ _ _ ph -> race_ (waitForProcess' sandboxProc ph) $ do
              waitForConnectionOnPort (threadDelay 500000) sandboxPort
              restPort :: Int <- fromIntegral <$> getFreePort
              let mavenProc = (shell $ unwords ["mvn", mvnRepoFlag, "-Dledgerport=" <> show sandboxPort, "-Drestport=" <> show restPort, "exec:java@run-quickstart"]) { std_out = UseHandle devNull2 }
              withCreateProcess mavenProc $
                  \_ _ _ ph -> race_ (waitForProcess' mavenProc ph) $ do
                  let url = "http://localhost:" <> show restPort <> "/iou"
                  waitForHttpServer (threadDelay 1000000) url []
                  threadDelay 5000000
                  manager <- newManager defaultManagerSettings
                  req <- parseRequest url
                  req <- pure req { requestHeaders = [(hContentType, "application/json")] }
                  resp <- httpLbs req manager
                  responseBody resp @?=
                      "{\"0\":{\"issuer\":\"EUR_Bank\",\"owner\":\"Alice\",\"currency\":\"EUR\",\"amount\":100.0000000000,\"observers\":[]}}"
                  -- waitForProcess' will block on Windows so we explicitly kill the process.
                  terminateProcess ph
              -- waitForProcess' will block on Windows so we explicitly kill the process.
              terminateProcess ph
    ]
    where
        mvnRepoFlag = "-Dmaven.repo.local=" <> mvnDir

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
                    callCommandQuiet $ unwords ["daml", "new", projectDir, templateName]
                    withCurrentDirectory projectDir $ do
                        filesAtStart <- sort <$> listFilesRecursive "."
                        callCommandQuiet "daml build"
                        callCommandQuiet "daml clean"
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

-- | Check we can generate language bindings.
codegenTests :: FilePath -> TestTree
codegenTests codegenDir = testGroup "daml codegen"
    [ codegenTestFor "ts" Nothing
    , codegenTestFor "java" Nothing
    , codegenTestFor "scala" (Just "com.cookiemonster.nomnomnom")
    ]
    where
        codegenTestFor :: String -> Maybe String -> TestTree
        codegenTestFor lang namespace =
            testCase lang $ do
                createDirectoryIfMissing True codegenDir
                withCurrentDirectory codegenDir $ do
                    let projectDir = codegenDir </> ("proj-" ++ lang)
                    callCommandQuiet $ unwords ["daml new", projectDir, "skeleton"]
                    withCurrentDirectory projectDir $ do
                        callCommandQuiet "daml build"
                        let darFile = projectDir</> ".daml/dist/proj-" ++ lang ++ "-0.0.1.dar"
                            outDir  = projectDir</> "generated" </> lang
                        callCommandQuiet $
                          unwords [ "daml", "codegen", lang
                                  , darFile ++ maybe "" ("=" ++) namespace
                                  , "-o", outDir]
                        contents <- listDirectory outDir
                        assertBool "bindings were written" (not $ null contents)

deployTest :: FilePath -> TestTree
deployTest deployDir = testCase "daml deploy" $ do
    createDirectoryIfMissing True deployDir
    withCurrentDirectory deployDir $ do
        callCommandQuiet $ unwords ["daml new", deployDir </> "proj1"]
        callCommandQuiet $ unwords ["daml new", deployDir </> "proj2", "quickstart-java"]
        withCurrentDirectory (deployDir </> "proj1") $ do
            callCommandQuiet "daml build"
            withDevNull $ \devNull -> do
                port :: Int <- fromIntegral <$> getFreePort
                let sharedSecret = "TheSharedSecret"
                let sandboxProc =
                        (shell $ unwords
                            ["daml sandbox"
                            , "--auth-jwt-hs256-unsafe=" <> sharedSecret
                            , "--port", show port
                            , ".daml/dist/proj1-0.0.1.dar"
                            ]) { std_out = UseHandle devNull }
                let tokenFile = deployDir </> "secretToken.jwt"
                -- The trailing newline is not required but we want to test that it is supported.
                writeFileUTF8 tokenFile ("Bearer " <> makeSignedJwt sharedSecret <> "\n")
                withCreateProcess sandboxProc  $ \_ _ _ ph ->
                    race_ (waitForProcess' sandboxProc ph) $ do
                        waitForConnectionOnPort (threadDelay 100000) port
                        withCurrentDirectory (deployDir </> "proj2") $ do
                            callCommandQuiet $ unwords
                                [ "daml deploy"
                                , "--access-token-file " <> tokenFile
                                , "--port", show port
                                , "--host localhost"
                                ]
                        -- waitForProcess' will block on Windows so we explicitly kill the process.
                        terminateProcess ph

makeSignedJwt :: String -> String
makeSignedJwt sharedSecret = do
  let urc = JWT.ClaimsMap $ Map.fromList [ ("admin", Aeson.Bool True)]
  let cs = mempty { JWT.unregisteredClaims = urc }
  let key = JWT.hmacSecret $ T.pack sharedSecret
  let text = JWT.encodeSigned key mempty cs
  T.unpack text


damlInstallerName :: String
damlInstallerName
    | isWindows = "daml.exe"
    | otherwise = "daml"

-- | Like call process but hides stdout.
runCreateProcessQuiet :: CreateProcess -> IO ()
runCreateProcessQuiet createProcess = do
    (exit, _out, err) <- readCreateProcessWithExitCode createProcess ""
    hPutStr stderr err
    unless (exit == ExitSuccess) $ throwIO $ ProcessExitFailure exit createProcess

-- | Like callProcess but hides stdout.
callProcessQuiet :: FilePath -> [String] -> IO ()
callProcessQuiet cmd args =
    runCreateProcessQuiet (proc cmd args)

-- | Like callCommand but hides stdout.
callCommandQuiet :: String -> IO ()
callCommandQuiet cmd =
    runCreateProcessQuiet (shell cmd)

data ProcessExitFailure = ProcessExitFailure !ExitCode !CreateProcess
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
waitForProcess' :: CreateProcess -> ProcessHandle -> IO ()
waitForProcess' cp ph = do
    e <- waitForProcess ph
    unless (e == ExitSuccess) $ throwIO $ ProcessExitFailure e cp

-- | Getting a dev-null handle in a cross-platform way seems to be somewhat tricky so we instead
-- use a temporary file.
withDevNull :: (Handle -> IO a) -> IO a
withDevNull a = withTempFile $ \f -> withFile f WriteMode a
