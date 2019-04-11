-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Assistant.Tests
    ( runTests
    ) where

import DAML.Assistant.Env
import DAML.Assistant.Install
import DAML.Assistant.Types
import DAML.Assistant.Util
import DAML.Project.Consts hiding (getDamlPath, getProjectPath)
import System.Directory
import System.Environment
import System.FilePath
import System.Info.Extra (isWindows)
import System.IO.Temp
import System.IO.Extra
import Data.List.Extra
import qualified Test.Tasty as Tasty
import qualified Test.Tasty.HUnit as Tasty
import qualified Test.Tasty.QuickCheck as Tasty
import qualified Data.Text as T
import Test.Tasty.QuickCheck ((==>))
import Test.Main (withEnv)
import System.Info (os)
import Data.Maybe
import Control.Exception.Safe
import Control.Monad
import Conduit
import qualified Data.Conduit.Zlib as Zlib
import qualified Data.Conduit.Tar as Tar
import qualified Data.SemVer as V

-- unix specific
import System.PosixCompat.Files (createSymbolicLink)

runTests :: IO ()
runTests = do
    setEnv "TASTY_NUM_THREADS" "1" -- we need this because we use withEnv in our tests
    Tasty.defaultMain $ Tasty.testGroup "DAML.Assistant"
        [ testAscendants
        , testGetDamlPath
        , testGetProjectPath
        , testGetSdk
        , testInstall
        ]

assertError :: Text -> Text -> IO a -> IO ()
assertError ctxPattern msgPattern action = do
    result <- try action
    case result of
        Left AssistantError{..} -> do
            Tasty.assertBool ("Error context pattern does not match error. Expected: " <> show ctxPattern <> ". Got: " <> show errContext <> ".") (ctxPattern `T.isInfixOf` fromMaybe "" errContext)
            Tasty.assertBool ("Error message pattern does not match error. Expected: " <> show msgPattern <> ". Got: " <> show errMessage <> ".") (msgPattern `T.isInfixOf` fromMaybe "" errMessage)
        Right _ ->
            Tasty.assertFailure "Expected assistant error."

testGetDamlPath :: Tasty.TestTree
testGetDamlPath = Tasty.testGroup "DAML.Assistant.Env.getDamlPath"
    [ Tasty.testCase "getDamlPath gets app user data directory by default" $ do
        withSystemTempDirectory "test-getDamlPath" $ \base -> do
            let baseVar | "linux"   <- os = "HOME"
                        | "darwin"  <- os = "HOME"
                        | "mingw32" <- os = "APPDATA"
                        | otherwise = error ("unknown os " <> os)
            let folder  | "linux"   <- os = ".daml"
                        | "darwin"  <- os = ".daml"
                        | "mingw32" <- os = "daml"
                        | otherwise = error ("unknown os " <> os)

            let expected = base </> folder
            createDirectory expected
            DamlPath got <- withEnv [ (baseVar, Just base)
                                    , (damlPathEnvVar, Nothing)
                                    ] getDamlPath
            Tasty.assertEqual "daml home path" expected got

    , Tasty.testCase "getDamlPath returns DAML_HOME" $ do
        withSystemTempDirectory "test-getDamlPath" $ \expected -> do
            DamlPath got <- withEnv [(damlPathEnvVar, Just expected)] getDamlPath
            Tasty.assertEqual "daml home path" expected got

    ]

testGetProjectPath :: Tasty.TestTree
testGetProjectPath = Tasty.testGroup "DAML.Assistant.Env.getProjectPath"
    [ Tasty.testCase "getProjectPath returns environment variable" $ do
        withSystemTempDirectory "test-getProjectPath" $ \dir -> do
            let expected = dir </> "project"
            setCurrentDirectory dir
            createDirectory expected
            Just got <- withEnv [(projectPathEnvVar, Just expected)] getProjectPath
            Tasty.assertEqual "project path" (ProjectPath expected) got
            return ()

    , Tasty.testCase "getProjectPath returns nothing" $ do
        -- This test assumes there's no daml.yaml above the temp directory.
        -- ... this might be an ok assumption, but maybe getProjectPath
        -- should also check that the project path is owned by the user,
        -- or something super fancy like that.
        withSystemTempDirectory "test-getProjectPath" $ \dir -> do
            setCurrentDirectory dir
            Nothing <- withEnv [(projectPathEnvVar, Nothing)] getProjectPath
            return ()

    , Tasty.testCase "getProjectPath returns current directory" $ do
        withSystemTempDirectory "test-getProjectPath" $ \dir -> do
            writeFileUTF8 (dir </> projectConfigName) ""
            setCurrentDirectory dir
            Just path <- withEnv [(projectPathEnvVar, Nothing)] getProjectPath
            Tasty.assertEqual "project path" (ProjectPath dir) path

    , Tasty.testCase "getProjectPath returns parent directory" $ do
        withSystemTempDirectory "test-getProjectPath" $ \dir -> do
            createDirectory (dir </> "foo")
            writeFileUTF8 (dir </> projectConfigName) ""
            setCurrentDirectory (dir </> "foo")
            Just path <- withEnv [(projectPathEnvVar, Nothing)] getProjectPath
            Tasty.assertEqual "project path" (ProjectPath dir) path

    , Tasty.testCase "getProjectPath returns grandparent directory" $ do
        withSystemTempDirectory "test-getProjectPath" $ \dir -> do
            createDirectoryIfMissing True (dir </> "foo" </> "bar")
            writeFileUTF8 (dir </> projectConfigName) ""
            setCurrentDirectory (dir </> "foo" </> "bar")
            Just path <- withEnv [(projectPathEnvVar, Nothing)] getProjectPath
            Tasty.assertEqual "project path" (ProjectPath dir) path

    , Tasty.testCase "getProjectPath prefers parent over grandparent" $ do
        withSystemTempDirectory "test-getProjectPath" $ \dir -> do
            createDirectoryIfMissing True (dir </> "foo" </> "bar")
            writeFileUTF8 (dir </> projectConfigName) ""
            writeFileUTF8 (dir </> "foo" </> projectConfigName) ""
            setCurrentDirectory (dir </> "foo" </> "bar")
            Just path <- withEnv [(projectPathEnvVar, Nothing)] getProjectPath
            Tasty.assertEqual "project path" (ProjectPath (dir </> "foo")) path

    ]

testGetSdk :: Tasty.TestTree
testGetSdk = Tasty.testGroup "DAML.Assistant.Env.getSdk"
    [ Tasty.testCase "getSdk returns DAML_SDK_VERSION and DAML_SDK" $ do
        withSystemTempDirectory "test-getSdk" $ \base -> do
            let damlPath = DamlPath (base </> "daml")
                projectPath = Nothing
                expected1 = "10.10.10"
                expected2 = base </> "sdk"

            createDirectory expected2
            (Just (SdkVersion got1), Just (SdkPath got2)) <-
                withEnv [ (sdkVersionEnvVar, Just expected1)
                        , (sdkPathEnvVar, Just expected2)
                        ] (getSdk damlPath projectPath)
            Tasty.assertEqual "sdk version" expected1 (V.toString got1)
            Tasty.assertEqual "sdk path" expected2 got2

    , Tasty.testCase "getSdk determines DAML_SDK from DAML_SDK_VERSION" $ do
        withSystemTempDirectory "test-getSdk" $ \base -> do
            let damlPath = DamlPath (base </> "daml")
                projectPath = Nothing
                expected1 = "0.12.5-version"
                expected2 = base </> "daml" </> "sdk" </> expected1

            createDirectoryIfMissing True (base </> "daml" </> "sdk")
            createDirectory expected2
            (Just (SdkVersion got1), Just (SdkPath got2)) <-
                withEnv [ (sdkVersionEnvVar, Just expected1)
                        , (sdkPathEnvVar, Nothing)
                        ] (getSdk damlPath projectPath)
            Tasty.assertEqual "sdk version" expected1 (V.toString got1)
            Tasty.assertEqual "sdk path" expected2 got2

    , Tasty.testCase "getSdk determines DAML_SDK_VERSION from DAML_SDK" $ do
        withSystemTempDirectory "test-getSdk" $ \base -> do
            let damlPath = DamlPath (base </> "daml")
                projectPath = Nothing
                expected1 = "0.3.4"
                expected2 = base </> "sdk2"

            createDirectory expected2
            writeFileUTF8 (expected2 </> sdkConfigName) ("version: " <> expected1 <> "\n")
            (Just (SdkVersion got1), Just (SdkPath got2)) <-
                withEnv [ (sdkVersionEnvVar, Nothing)
                        , (sdkPathEnvVar, Just expected2)
                        ] (getSdk damlPath projectPath)
            Tasty.assertEqual "sdk version" expected1 (V.toString got1)
            Tasty.assertEqual "sdk path" expected2 got2

    , Tasty.testCase "getSdk determines DAML_SDK and DAML_SDK_VERSION from project config" $ do
        withSystemTempDirectory "test-getSdk" $ \base -> do
            let damlPath = DamlPath (base </> "daml")
                projectPath = Just $ ProjectPath (base </> "project")
                expected1 = "10.10.2-version.af29bef"
                expected2 = base </> "daml" </> "sdk" </> expected1


            createDirectoryIfMissing True (base </> "daml" </> "sdk")
            createDirectory (base </> "project")
            writeFileUTF8 (base </> "project" </> projectConfigName)
                ("project:\n  sdk-version: " <> expected1)
            createDirectory expected2
            (Just (SdkVersion got1), Just (SdkPath got2)) <-
                withEnv [ (sdkVersionEnvVar, Nothing)
                        , (sdkPathEnvVar, Nothing)
                        ] (getSdk damlPath projectPath)
            Tasty.assertEqual "sdk version" expected1 (V.toString got1)
            Tasty.assertEqual "sdk path" expected2 got2


    , Tasty.testCase "getSdk: DAML_SDK overrides project config version" $ do
        withSystemTempDirectory "test-getSdk" $ \base -> do
            let damlPath = DamlPath (base </> "daml")
                projectPath = Just $ ProjectPath (base </> "project")
                expected1 = "0.9.8-ham"
                expected2 = base </> "sdk3"
                projVers = "5.2.1"

            createDirectoryIfMissing True (base </> "daml" </> "sdk" </> projVers)
            createDirectory (base </> "project")
            writeFileUTF8 (base </> "project" </> projectConfigName)
                ("project:\n  sdk-version: " <> projVers)
            createDirectory expected2
            writeFileUTF8 (expected2 </> sdkConfigName) ("version: " <> expected1 <> "\n")
            (Just (SdkVersion got1), Just (SdkPath got2)) <-
                withEnv [ (sdkVersionEnvVar, Nothing)
                        , (sdkPathEnvVar, Just expected2)
                        ] (getSdk damlPath projectPath)
            Tasty.assertEqual "sdk version" expected1 (V.toString got1)
            Tasty.assertEqual "sdk path" expected2 got2

    , Tasty.testCase "getSdk: DAML_SDK_VERSION overrides project config version" $ do
        withSystemTempDirectory "test-getSdk" $ \base -> do
            let damlPath = DamlPath (base </> "daml")
                projectPath = Just $ ProjectPath (base </> "project")
                expected1 = "0.0.0"
                expected2 = base </> "daml" </> "sdk" </> expected1
                projVers = "0.0.1"

            createDirectoryIfMissing True (base </> "daml" </> "sdk" </> projVers)
            createDirectory (base </> "project")
            writeFileUTF8 (base </> "project" </> projectConfigName)
                ("project:\n  sdk-version: " <> projVers)
            createDirectory expected2
            (Just (SdkVersion got1), Just (SdkPath got2)) <-
                withEnv [ (sdkVersionEnvVar, Just expected1)
                        , (sdkPathEnvVar, Nothing)
                        ] (getSdk damlPath projectPath)
            Tasty.assertEqual "sdk version" expected1 (V.toString got1)
            Tasty.assertEqual "sdk path" expected2 got2

    , Tasty.testCase "getSdk: Returns Nothings if .daml/sdk is missing." $ do
        withSystemTempDirectory "test-getSdk" $ \base -> do
            let damlPath = DamlPath (base </> "daml")
                projPath = Nothing
            createDirectoryIfMissing True (base </> "daml")
            (Nothing, Nothing) <- withEnv
                [ (sdkVersionEnvVar, Nothing)
                , (sdkPathEnvVar, Nothing)
                ] (getSdk damlPath projPath)
            pure ()
    ]


testAscendants :: Tasty.TestTree
testAscendants = Tasty.testGroup "DAML.Assistant.ascendants"
    [ Tasty.testCase "unit tests" $ do
        Tasty.assertEqual "empty path" ["."] (ascendants "")
        Tasty.assertEqual "curdir path" ["."] (ascendants ".")
        Tasty.assertEqual "root path" ["/"] (ascendants "/")
        Tasty.assertEqual "home path" ["~"] (ascendants "~")
        Tasty.assertEqual "foo/bar" ["foo/bar", "foo", "."] (ascendants "foo/bar")
        Tasty.assertEqual "foo/bar/" ["foo/bar/", "foo", "."] (ascendants "foo/bar/")
        Tasty.assertEqual "./foo/bar" ["./foo/bar", "./foo", "."] (ascendants "./foo/bar")
        Tasty.assertEqual "../foo/bar" ["../foo/bar", "../foo", ".."] (ascendants "../foo/bar")
        Tasty.assertEqual "~/foo/bar" ["~/foo/bar", "~/foo", "~"] (ascendants "~/foo/bar")
        Tasty.assertEqual "/foo/bar/baz" ["/foo/bar/baz", "/foo/bar", "/foo", "/"]
            (ascendants "/foo/bar/baz")
    , Tasty.testProperty "ascendants is nonempty"
        (\p -> notNull (ascendants p))
    , Tasty.testProperty "head . ascendants == id"
        (\p -> notNull p ==> head (ascendants p) == p)
    , Tasty.testProperty "head . ascendants == id (2)"
        (\p1 p2 -> let p = p1 </> p2 in notNull p1 && notNull p2 && isRelative p2 ==>
                   head (ascendants p) == p)
    , Tasty.testProperty "tail . ascendants == ascendants . takeDirectory"
        (\p1 p2 -> let p = dropTrailingPathSeparator (p1 </> p2)
                   in notNull p1 && notNull p2 && isRelative p2 ==>
                      tail (ascendants p) == ascendants (takeDirectory p))
    ]

testInstall :: Tasty.TestTree
testInstall = Tasty.testGroup "DAML.Assistant.Install"
    [ Tasty.testCase "initial install a tarball" $ do
        withSystemTempDirectory "test-install" $ \ base -> do
            let damlPath = DamlPath (base </> "daml")
                options = InstallOptions
                    { iTargetM = Just (RawInstallTarget "source.tar.gz")
                    , iActivate = ActivateInstall True
                    , iInitial = InitialInstall True
                    , iQuiet = QuietInstall True
                    , iForce = ForceInstall False
                    }

            setCurrentDirectory base
            createDirectoryIfMissing True "source"
            createDirectoryIfMissing True ("source" </> "daml")
            writeFileUTF8 ("source" </> sdkConfigName) "version: 0.0.0-test"
            writeFileUTF8 ("source" </> "daml" </> "daml") "" -- daml "binary" for --activate
            createSymbolicLink ("daml" </> "daml") ("source" </> "daml-link")
                -- check if symbolic links are handled correctly

            runConduitRes $
                yield "source"
                .| void Tar.tarFilePath
                .| Zlib.gzip
                .| sinkFile "source.tar.gz"

            install options damlPath
    , if isWindows
        then testInstallWindows
        else testInstallUnix
    ]

testInstallUnix :: Tasty.TestTree
testInstallUnix = Tasty.testGroup "unix-specific tests"
    [ Tasty.testCase "reject an absolute symlink in a tarball" $ do
        withSystemTempDirectory "test-install" $ \ base -> do
            let damlPath = DamlPath (base </> "daml")
                options = InstallOptions
                    { iTargetM = Just (RawInstallTarget "source.tar.gz")
                    , iActivate = ActivateInstall False
                    , iInitial = InitialInstall True
                    , iQuiet = QuietInstall True
                    , iForce = ForceInstall False
                    }

            setCurrentDirectory base
            createDirectoryIfMissing True "source"
            writeFileUTF8 ("source" </> sdkConfigName) "version: 0.0.0-test"
            createSymbolicLink (base </> "daml") ("source" </> "daml-link")
                -- absolute symlink

            runConduitRes $
                yield "source"
                .| void Tar.tarFilePath
                .| Zlib.gzip
                .| sinkFile "source.tar.gz"

            assertError "Extracting SDK release tarball."
                "Invalid SDK release: symbolic link target is absolute."
                (install options damlPath)

    , Tasty.testCase "reject an escaping symlink in a tarball" $ do
        withSystemTempDirectory "test-install" $ \ base -> do
            let damlPath = DamlPath (base </> "daml")
                options = InstallOptions
                    { iTargetM = Just (RawInstallTarget "source.tar.gz")
                    , iActivate = ActivateInstall False
                    , iInitial = InitialInstall True
                    , iQuiet = QuietInstall True
                    , iForce = ForceInstall False
                    }

            setCurrentDirectory base
            createDirectoryIfMissing True "source"
            writeFileUTF8 ("source" </> sdkConfigName) "version: 0.0.0-test"
            createSymbolicLink (".." </> "daml") ("source" </> "daml-link")
                -- escaping symlink

            runConduitRes $
                yield "source"
                .| void Tar.tarFilePath
                .| Zlib.gzip
                .| sinkFile "source.tar.gz"

            assertError "Extracting SDK release tarball."
                "Invalid SDK release: symbolic link target escapes tarball."
                (install options damlPath)
    ]

testInstallWindows :: Tasty.TestTree
testInstallWindows = Tasty.testGroup "windows-specific tests" []
