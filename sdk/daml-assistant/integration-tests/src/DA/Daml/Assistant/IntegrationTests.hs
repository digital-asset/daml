-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.IntegrationTests (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Concurrent
import Control.Concurrent.STM
import Control.Lens
import Control.Monad
import qualified Data.Aeson as Aeson
import Data.Aeson.Lens
import Data.List.Extra
import Data.Maybe (maybeToList, isJust)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as Vector
import Network.HTTP.Client
import Network.HTTP.Types
import Network.Socket.Extended
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

import DA.Bazel.Runfiles
import DA.Daml.Assistant.IntegrationTestUtils
import DA.Daml.Helper.Util (tokenFor, decodeCantonSandboxPort)
import DA.Test.Daml2jsUtils
import DA.Test.Process (callCommandIn, callCommandFailingIn, callCommandSilent, callCommandSilentIn, subprocessEnv)
import DA.Test.Util
import DA.PortFile
import SdkVersion (SdkVersioned, sdkVersion, withSdkVersions)

main :: IO ()
main = withSdkVersions $ do
    yarn : args <- getArgs
    withTempDir $ \tmpDir -> do
        oldPath <- getSearchPath
        javaPath <- locateRunfiles "local_jdk/bin"
        yarnPath <- takeDirectory <$> locateRunfiles (mainWorkspace </> yarn)
        -- NOTE(Sofia): We don't use `script` on Windows.
        mbScriptPath <- if isWindows
            then pure Nothing
            else Just <$> locateRunfiles "script_nix/bin"
        limitJvmMemory defaultJvmMemoryLimits
        withArgs args (withEnv
            [ ("PATH", Just $ intercalate [searchPathSeparator] $ concat
                [ [javaPath, yarnPath]
                , maybeToList mbScriptPath
                , oldPath
                ])
            , ("TASTY_NUM_THREADS", Just "2")
            ] $ defaultMain (tests tmpDir))

hardcodedToken :: String -> T.Text
hardcodedToken alice = tokenFor (T.pack alice)

authorizationHeaders :: String -> RequestHeaders
authorizationHeaders alice = [("Authorization", "Bearer " <> T.encodeUtf8 (hardcodedToken alice))]

withDamlServiceIn :: FilePath -> String -> [String] -> (ProcessHandle -> IO a) -> IO a
withDamlServiceIn path command args act = withDevNull $ \devNull -> do
    let proc' = (shell $ unwords $ ["daml", command, "--shutdown-stdin-close"] <> args)
          { std_out = UseHandle devNull
          , std_in = CreatePipe
          , cwd = Just path
          }
    withCreateProcess proc' $ \stdin _ _ ph -> do
        Just stdin <- pure stdin
        r <- act ph
        hClose stdin
        -- We tear things down gracefully instead of killing
        -- the process group so that waiting for the parent process
        -- ensures that all child processes are all dead too.
        -- Going via closing stdin works on Windows whereas tearing things
        -- down gracefully via SIGTERM isn’t as much of a thing so we use the former.
        _ <- waitForProcess ph
        pure r

data DamlStartResource = DamlStartResource
    { projDir :: FilePath
    , tmpDir :: FilePath
    , packageRef :: String
    , alice :: String
    , aliceHeaders :: RequestHeaders
    , startStdin :: Handle
    , stdoutChan :: TChan String
    , stop :: IO ()
    , sandboxPort :: PortNumber
    , jsonApiPort :: PortNumber
    }

damlStart :: SdkVersioned => FilePath -> IO DamlStartResource
damlStart tmpDir = do
    let projDir = tmpDir </> "assistant-integration-tests"
    createDirectoryIfMissing True (projDir </> "daml")
    let scriptOutputFile = "script-output.json"
    let packageName = "assistant-integration-tests"
    writeFileUTF8 (projDir </> "daml.yaml") $
        unlines
            [ "sdk-version: " <> sdkVersion
            , "name: " <> packageName
            , "version: \"1.0\""
            , "source: daml"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            , "  - daml-script"
            -- TODO(#14706): remove build-options once the default major version is 2
            , "init-script: Main:init"
            , "script-options:"
            , "  - --output-file"
            , "  - " <> scriptOutputFile
            , "codegen:"
            , "  js:"
            , "    output-directory: ui/daml.js"
            , "    npm-scope: daml.js"
            , "  java:"
            , "    output-directory: ui/java"
            , "build-options:"
            , "- --target=2.1"
            ]
    writeFileUTF8 (projDir </> "daml/Main.daml") $
        unlines
            [ "module Main where"
            , "import Daml.Script"
            , "template T with p : Party where signatory p"
            , "init : Script Party"
            , "init = do"
            , "  alice <- allocatePartyByHint (PartyIdHint \"Alice\")"
            , "  aliceId <- validateUserId \"alice\""
            , "  aliceUser <- createUser (User aliceId (Some alice)) [CanActAs alice]"
            , "  alice `submit` createCmd (T alice)"
            , "  pure alice"
            , "test : Int -> Script (Int, Int)"
            , "test x = pure (x, x + 1)"
            ]
    ports <- sandboxPorts
    jsonApiPort <- getFreePort
    env <- subprocessEnv []
    let startProc =
            (shell $ unwords
                [ "daml start"
                , "--sandbox-port", show $ ledger ports
                , "--sandbox-admin-api-port", show $ admin ports
                , "--sandbox-sequencer-public-port", show $ sequencerPublic ports
                , "--sandbox-sequencer-admin-port", show $ sequencerAdmin ports
                , "--sandbox-mediator-admin-port", show $ mediatorAdmin ports
                , "--json-api-port", show jsonApiPort
                ]
            ) {std_in = CreatePipe, std_out = CreatePipe, cwd = Just projDir, create_group = True, env = Just env}
    (Just startStdin, Just startStdout, _, startPh) <- createProcess startProc
    outChan <- newBroadcastTChanIO
    outReader <- forkIO $ forever $ do
        line <- hGetLine startStdout
        atomically $ writeTChan outChan line
    scriptOutput <- readPortFileWith Just startPh maxRetries (projDir </> scriptOutputFile)
    let alice = (read scriptOutput :: String)
    pure $
        DamlStartResource
            { projDir = projDir
            , tmpDir = tmpDir
            , packageRef = "#" <> packageName
            , sandboxPort = ledger ports
            , jsonApiPort = jsonApiPort
            , startStdin = startStdin
            , alice = alice
            , aliceHeaders = authorizationHeaders "alice"
            , stop = do
                interruptProcessGroupOf startPh
                killThread outReader
            , stdoutChan = outChan
            }

tests :: SdkVersioned => FilePath -> TestTree
tests tmpDir =
    withSdkResource $ \_ ->
        testGroup
            "Integration tests"
            [ testCase "daml version" $
                callCommandSilentIn tmpDir "daml version"
            , testCase "daml --help" $
                callCommandSilentIn tmpDir "daml --help"
            , testCase "daml new --list" $
                callCommandSilentIn tmpDir "daml new --list"
            , packagingTests tmpDir
            , withResource (damlStart (tmpDir </> "sandbox-canton-1")) stop damlStartTests
            , cleanTests cleanDir
            , templateTests
            , codegenTests codegenDir
            , cantonTests
            ]
  where
    cleanDir = tmpDir </> "clean"
    codegenDir = tmpDir </> "codegen"

-- Most of the packaging tests are in the a separate test suite in
-- //compiler/damlc/tests:packaging. This only has a couple of
-- integration tests.
packagingTests :: SdkVersioned => FilePath -> TestTree
packagingTests tmpDir =
    testGroup
        "packaging"
        [ testCase "Build Daml script example" $ do
              let projDir = tmpDir </> "script-example"
              callCommandSilent $ unwords ["daml", "new", projDir, "--template=script-example"]
              callCommandSilentIn projDir "daml build"
              let dar = projDir </> ".daml/dist/script-example-0.0.1.dar"
              assertFileExists dar
        -- TODO: re-enable this test when the script-example template no longer specifies 1.15
        {- , testCase "Build Daml script example with LF version 1.dev" $ do
              let projDir = tmpDir </> "script-example1"
              callCommandSilent $ unwords ["daml", "new", projDir, "--template=script-example"]
              callCommandSilentIn projDir "daml build --target 1.dev"
              let dar = projDir </> ".daml/dist/script-example-0.0.1.dar"
              assertFileExists dar -}
        , testCase "Package depending on daml-script can use data-dependencies" $ do
              callCommandSilent $ unwords ["daml", "new", tmpDir </> "data-dependency"]
              callCommandSilentIn (tmpDir </> "data-dependency") "daml build -o data-dependency.dar"
              createDirectoryIfMissing True (tmpDir </> "proj")
              writeFileUTF8 (tmpDir </> "proj" </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: proj"
                      , "version: 0.0.1"
                      , "source: ."
                      , "dependencies: [daml-prim, daml-stdlib, daml-script]"
                      , "data-dependencies: [" <>
                        show (tmpDir </> "data-dependency" </> "data-dependency.dar") <>
                        "]"
                      ]
              writeFileUTF8 (tmpDir </> "proj" </> "A.daml") $
                  unlines
                      [ "module A where"
                      , "import Daml.Script"
                      , "import Main"
                      , "f = setup >> allocateParty \"foobar\""
          -- This also checks that we get the same Script type within an SDK version.
                      ]
              callCommandSilentIn (tmpDir </> "proj") "daml build"
        , testCase "Unused dependency from daml-libs" $ do
              createDirectoryIfMissing True (tmpDir </> "unused-daml-libs")
              writeFileUTF8 (tmpDir </> "unused-daml-libs" </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: unused-daml-lib"
                      , "version: 0.0.1"
                      , "source: ."
                      , "dependencies: [daml-prim, daml-stdlib, daml-script]"
                      ]
              writeFileUTF8 (tmpDir </> "unused-daml-libs" </> "A.daml") "module A where"
              (_out, err) <- callCommandIn (tmpDir </> "unused-daml-libs") "daml build"
              assertBool ("Warning not found in\n" <> err) $
                "The following dependencies are not used:\n" `isInfixOf` err
                  && "(daml-script, " `isInfixOf` err
        , testCase "Unused data-dependency" $ do
              createDirectoryIfMissing True (tmpDir </> "unused-data-dependency-aux")
              writeFileUTF8 (tmpDir </> "unused-data-dependency-aux" </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: unused-data-dependency-aux"
                      , "version: 0.0.1"
                      , "source: ."
                      , "dependencies: [daml-prim, daml-stdlib]"
                      ]
              writeFileUTF8 (tmpDir </> "unused-data-dependency-aux" </> "A.daml") "module A where"
              createDirectoryIfMissing True (tmpDir </> "unused-data-dependency")
              writeFileUTF8 (tmpDir </> "unused-data-dependency" </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: unused-data-dependency"
                      , "version: 0.0.1"
                      , "source: ."
                      , "dependencies: [daml-prim, daml-stdlib]"
                      , "data-dependencies: [../unused-data-dependency-aux/.daml/dist/unused-data-dependency-aux-0.0.1.dar]"
                      ]
              writeFileUTF8 (tmpDir </> "unused-data-dependency" </> "A.daml") "module A where"
              callCommandSilentIn (tmpDir </> "unused-data-dependency-aux") "daml build"
              (_out, err) <- callCommandIn (tmpDir </> "unused-data-dependency") "daml build"
              assertBool ("Warning not found in\n" <> err) $
                "The following dependencies are not used:\n" `isInfixOf` err
                  && "(unused-data-dependency-aux, 0.0.1)" `isInfixOf` err
        , testCase "Depend on upgrading package" $ do
              createDirectoryIfMissing True (tmpDir </> "dep-on-upgrading-v1")
              writeFileUTF8 (tmpDir </> "dep-on-upgrading-v1" </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: dep-on-upgrading"
                      , "version: 0.0.1"
                      , "source: ."
                      , "dependencies: [daml-prim, daml-stdlib]"
                      ]
              writeFileUTF8 (tmpDir </> "dep-on-upgrading-v1" </> "A.daml") "module A where"
              createDirectoryIfMissing True (tmpDir </> "dep-on-upgrading-v2")
              writeFileUTF8 (tmpDir </> "dep-on-upgrading-v2" </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: dep-on-upgrading"
                      , "version: 0.0.2"
                      , "source: ."
                      , "module-prefixes:"
                      , "  dep-on-upgrading-0.0.1: V1"
                      , "upgrades: ../dep-on-upgrading-v1/.daml/dist/dep-on-upgrading-0.0.1.dar"
                      , "dependencies: [daml-prim, daml-stdlib]"
                      , "data-dependencies: [../dep-on-upgrading-v1/.daml/dist/dep-on-upgrading-0.0.1.dar]"
                      ]
              -- Need to import the module for it to be used
              writeFileUTF8 (tmpDir </> "dep-on-upgrading-v2" </> "A.daml") "module A where\nimport V1.A ()"
              callCommandSilentIn (tmpDir </> "dep-on-upgrading-v1") "daml build"
              (_out, err) <- callCommandFailingIn (tmpDir </> "dep-on-upgrading-v2") "daml build"
              assertBool ("Error not found in\n" <> err) $
                "Please remove the package " `isInfixOf` err
                  && "(dep-on-upgrading, 0.0.1)" `isInfixOf` err
        , testCase "Depends on daml-script when defining template" $ do
              createDirectoryIfMissing True (tmpDir </> "template-depend-on-script")
              writeFileUTF8 (tmpDir </> "template-depend-on-script" </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: template-depend-on-script"
                      , "version: 0.0.1"
                      , "source: ."
                      , "dependencies: [daml-prim, daml-stdlib, daml-script]"
                      ]
              writeFileUTF8 (tmpDir </> "template-depend-on-script" </> "A.daml") $
                  unlines
                      [ "module A where"
                      , "import Daml.Script"
                      , "template Name with p : Party where signatory p"
                      ]
              (_out, err) <- callCommandIn (tmpDir </> "template-depend-on-script") "daml build"
              assertBool ("Warning not found in\n" <> err) $
                "This package defines templates or interfaces, and depends on daml-script." `isInfixOf` err
                  && "(daml-script, " `isInfixOf` err
        , disallowedUtilityTest
              "Package with templates cannot be utility"
              "utility-with-templates"
              "template"
              "template Name with p : Party where signatory p"
        , disallowedUtilityTest
              "Package with interfaces cannot be utility"
              "utility-with-interfaces"
              "interface"
              $ unlines
                  [ "data View = View {}"
                  , "interface MyInterface where"
                  , "  viewtype View"
                  ]
        , disallowedUtilityTest
              "Package with exceptions cannot be utility"
              "utility-with-exceptions"
              "exception"
              "exception E with m : Text where message m"
        , testCase "Allowed definitions in forced utility package compile" $ do
              createDirectoryIfMissing True (tmpDir </> "allowed-util-defs")
              writeFileUTF8 (tmpDir </> "allowed-util-defs" </> "daml.yaml") $
                  unlines
                      [ "sdk-version: " <> sdkVersion
                      , "name: allowed-util-defs"
                      , "version: 0.0.1"
                      , "source: ."
                      , "build-options: [--force-utility-package=yes]"
                      , "dependencies: [daml-prim, daml-stdlib, daml-script]"
                      ]
              writeFileUTF8 (tmpDir </> "allowed-util-defs" </> "A.daml") $
                  unlines
                      [ "module A where"
                      , "import Daml.Script"
                      , -- Regular data types are permitted, but automatically made unserializable by the flag
                        "data MyData = MyData with a : Int"
                      , -- top level definitions are permitted, including scripts
                        "myVal : Int"
                      , "myVal = 3"
                      , "myScript : Script ()"
                      , "myScript = pure ()"
                      , -- non-serializable data types are permitted, naturally
                        "data MyFunc = MyFunc with f : Int -> Int"
                      ]
              callCommandSilentIn (tmpDir </> "allowed-util-defs") "daml build"
              
        ]
    where
        disallowedUtilityTest name dirName errName content = do
            testCase name $ do
                createDirectoryIfMissing True (tmpDir </> dirName)
                writeFileUTF8 (tmpDir </> dirName </> "daml.yaml") $
                    unlines
                        [ "sdk-version: " <> sdkVersion
                        , "name: " <> dirName
                        , "version: 0.0.1"
                        , "source: ."
                        , "build-options: [--force-utility-package=yes, --enable-interfaces=yes]"
                        , "dependencies: [daml-prim, daml-stdlib]"
                        ]
                writeFileUTF8 (tmpDir </> dirName </> "A.daml") $
                    "module A where\n" <> content
                (_out, err) <- callCommandFailingIn (tmpDir </> dirName) "daml build"
                assertBool ("Error not found in\n" <> err) $
                    ("No " <> errName <> " definitions permitted in forced utility packages (Module A)") `isInfixOf` err
          


-- We are trying to run as many tests with the same `daml start` process as possible to safe time.
damlStartTests :: SdkVersioned => IO DamlStartResource -> TestTree
damlStartTests getDamlStart =
    -- We use testCaseSteps to make sure each of these tests runs in sequence, not in parallel.
    testCaseSteps "daml start" $ \step -> do
        let subtest :: forall t. String -> IO t -> IO t
            subtest m p = step m >> p
        subtest "sandbox and json-api come up" $ do
            DamlStartResource {jsonApiPort, alice, aliceHeaders, packageRef} <- getDamlStart
            manager <- newManager defaultManagerSettings
            initialRequest <-
                parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/create"
            let createRequest =
                    initialRequest
                        { method = "POST"
                        , requestHeaders = aliceHeaders
                        , requestBody =
                            RequestBodyLBS $
                            Aeson.encode $
                            Aeson.object
                                [ "templateId" Aeson..= (packageRef ++ ":Main:T")
                                , "payload" Aeson..= [alice]
                                ]
                        }
            createResponse <- httpLbs createRequest manager
            statusCode (responseStatus createResponse) @?= 200
        subtest "daml start invokes codegen" $ do
            DamlStartResource {projDir} <- getDamlStart
            didGenerateJsCode <- doesFileExist (projDir </> "ui" </> "daml.js" </> "assistant-integration-tests-1.0" </> "package.json")
            didGenerateJavaCode <- doesFileExist (projDir </> "ui" </> "java" </> "da" </> "internal" </> "template" </> "Archive.java")
            didGenerateJsCode @?= True
            didGenerateJavaCode @?= True
        subtest "run a daml ledger command" $ do
            DamlStartResource {projDir, sandboxPort} <- getDamlStart
            callCommandSilentIn projDir $ unwords
                ["daml", "ledger", "allocate-party", "--port", show sandboxPort, "Bob"]
        subtest "Run init-script" $ do
            DamlStartResource {jsonApiPort, aliceHeaders, packageRef} <- getDamlStart
            initialRequest <- parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/query"
            let queryRequest = initialRequest
                    { method = "POST"
                    , requestHeaders = aliceHeaders
                    , requestBody =
                        RequestBodyLBS $
                        Aeson.encode $
                        Aeson.object ["templateIds" Aeson..= [packageRef ++ ":Main:T"]]
                    }
            manager <- newManager defaultManagerSettings
            queryResponse <- httpLbs queryRequest manager
            statusCode (responseStatus queryResponse) @?= 200
            preview (key "result" . _Array . to Vector.length) (responseBody queryResponse) @?= Just 2
        subtest "Daml Script --input-file and --output-file" $ do
            DamlStartResource {projDir, sandboxPort} <- getDamlStart
            let dar = projDir </> ".daml" </> "dist" </> "assistant-integration-tests-1.0.dar"
            writeFileUTF8 (projDir </> "input.json") "0"
            callCommandSilentIn projDir $ unwords
                [ "daml script"
                , "--dar " <> dar <> " --script-name Main:test"
                , "--input-file input.json --output-file output.json"
                , "--ledger-host localhost --ledger-port " <> show sandboxPort
                ]
            contents <- readFileUTF8 (projDir </> "output.json")
            lines contents @?= ["{", "  \"_1\": 0,", "  \"_2\": 1", "}"]
        subtest "run a daml deploy without project parties" $ do
            DamlStartResource {projDir, sandboxPort} <- getDamlStart
            copyFile (projDir </> "daml.yaml") (projDir </> "daml.yaml.back")
            writeFileUTF8 (projDir </> "daml.yaml") $ unlines
                [ "sdk-version: " <> sdkVersion
                , "name: proj1"
                , "version: 0.0.1"
                , "source: daml"
                , "dependencies:"
                , "  - daml-prim"
                , "  - daml-stdlib"
                , "  - daml-script"
                -- TODO(#14706): remove build-options once the default major version is 2
                , "build-options: [--target=2.1]"
                ]
            callCommandSilentIn projDir $ unwords ["daml", "deploy", "--host localhost", "--port", show sandboxPort]
            copyFile (projDir </> "daml.yaml.back") (projDir </> "daml.yaml")

-- | Ensure that daml clean removes precisely the files created by daml build.
cleanTests :: FilePath -> TestTree
cleanTests baseDir = testGroup "daml clean"
    [ cleanTestFor "skeleton"
    , cleanTestFor "quickstart-java"
    ]
    where
        cleanTestFor :: String -> TestTree
        cleanTestFor templateName =
            testCase ("daml clean test for " <> templateName <> " template") $ do
                createDirectoryIfMissing True baseDir
                let projectDir = baseDir </> ("proj-" <> templateName)
                callCommandSilentIn baseDir $ unwords ["daml", "new", projectDir, "--template", templateName]
                filesAtStart <- sort <$> listFilesRecursive projectDir
                callCommandSilentIn projectDir "daml build"
                callCommandSilentIn projectDir "daml clean"
                filesAtEnd <- sort <$> listFilesRecursive projectDir
                when (filesAtStart /= filesAtEnd) $ fail $ unlines
                    [ "daml clean did not remove all files produced by daml build."
                    , ""
                    , "    files at start:"
                    , unlines (map ("       "++) filesAtStart)
                    , "    files at end:"
                    , unlines (map ("       "++) filesAtEnd)
                    ]

templateTests :: TestTree
templateTests = testGroup "templates" $
    [ testCase name $ do
        withTempDir $ \tmpDir -> do
            let dir = tmpDir </> "foobar"
            callCommandSilentIn tmpDir $ unwords ["daml", "new", dir, "--template", name]
            callCommandSilentIn dir "daml build"
    | name <- templateNames
    ] <>
    [ testCase "quickstart-java, positional template" $ do
        withTempDir $ \tmpDir -> do
            let dir = tmpDir </> "foobar"
            -- Verify that the old syntax for `daml new` still works.
            callCommandSilentIn tmpDir $ unwords ["daml","new", dir, "quickstart-java"]
            contents <- readFileUTF8 $ dir </> "daml.yaml"
            assertInfixOf "name: quickstart" contents
    ]
  -- NOTE (MK) We might want to autogenerate this list at some point but for now
  -- this should be good enough.
  where templateNames =
            [ "daml-intro-choices"
            , "daml-intro-compose"
            , "daml-intro-constraints"
            , "daml-intro-contracts"
            , "daml-intro-daml-scripts"
            , "daml-intro-data"
--            , "daml-intro-exceptions"    -- warn for deprecated exceptions
            , "daml-intro-functional-101"
            , "daml-intro-parties"
--            , "daml-intro-test"          -- multi-package
            , "daml-patterns"
            , "quickstart-java"
            , "script-example"
            , "skeleton"
            ]

-- | Check we can generate language bindings.
codegenTests :: FilePath -> TestTree
codegenTests codegenDir = testGroup "daml codegen" (
    [ codegenTestFor "java" Nothing
    ] ++
    -- The '@daml/types' NPM package is not available on Windows which
    -- is required by 'daml2js'.
    [ codegenTestFor "js" Nothing | not isWindows ]
    )
    where
        codegenTestFor :: String -> Maybe String -> TestTree
        codegenTestFor lang namespace =
            testCase lang $ do
                createDirectoryIfMissing True codegenDir
                let projectDir = codegenDir </> ("proj-" ++ lang)
                callCommandSilentIn codegenDir $ unwords ["daml new", projectDir, "--template=skeleton"]
                callCommandSilentIn projectDir "daml build"
                let darFile = projectDir </> ".daml/dist/proj-" ++ lang ++ "-0.0.1.dar"
                    outDir  = projectDir </> "generated" </> lang
                when (lang == "js") $ do
                    let workspaces = Workspaces [makeRelative codegenDir outDir]
                    setupYarnEnv codegenDir workspaces [DamlTypes, DamlLedger]
                callCommandSilentIn projectDir $
                    unwords [ "daml", "codegen", lang
                            , darFile ++ maybe "" ("=" ++) namespace
                            , "-o", outDir]
                contents <- listDirectory (projectDir </> outDir)
                assertBool "bindings were written" (not $ null contents)

cantonTests :: TestTree
cantonTests = testGroup "daml sandbox"
    [ testCaseSteps "Can start Canton sandbox and run script" $ \step -> withTempDir $ \dir -> do
        step "Creating project"
        callCommandSilentIn dir $ unwords ["daml new", "skeleton", "--template=skeleton"]
        step "Building project"
        -- TODO(#14706): remove explicit target once the default major version is 2
        callCommandSilentIn (dir </> "skeleton") "daml build --target=2.1"
        step "Finding free ports"
        ledgerApiPort <- getFreePort
        adminApiPort <- getFreePort
        sequencerPublicApiPort <- getFreePort
        sequencerAdminApiPort <- getFreePort
        mediatorAdminApiPort <- getFreePort
        step "Staring Canton sandbox"
        let portFile = dir </> "canton-portfile.json"
        withDamlServiceIn (dir </> "skeleton") "sandbox"
            [ "--port", show ledgerApiPort
            , "--admin-api-port", show adminApiPort
            , "--sequencer-public-port", show sequencerPublicApiPort
            , "--sequencer-admin-port", show sequencerAdminApiPort
            , "--mediator-admin-port", show mediatorAdminApiPort
            , "--canton-port-file", portFile
            ] $ \ ph -> do
            -- wait for port file to be written
            _ <- readPortFileWith decodeCantonSandboxPort ph maxRetries portFile
            step "Uploading DAR"
            callCommandSilentIn (dir </> "skeleton") $ unwords
                ["daml ledger upload-dar --host=localhost --port=" <> show ledgerApiPort, ".daml/dist/skeleton-0.0.1.dar"]
            step "Running script"
            callCommandSilentIn (dir </> "skeleton") $ unwords
                [ "daml script"
                , "--dar", ".daml/dist/skeleton-0.0.1.dar"
                , "--script-name Main:setup"
                , "--ledger-host=localhost", "--ledger-port=" <> show ledgerApiPort
                ]
            step "Start canton-console"
            env <- getEnvironment
            let cmd = unwords
                    [ "daml canton-console"
                    , "--port", show ledgerApiPort
                    , "--admin-api-port", show adminApiPort
                    , "--domain-public-port", show sequencerPublicApiPort
                    , "--domain-admin-port", show sequencerAdminApiPort
                    ]
                -- NOTE (Sofia): We need to use `script` on Mac and Linux because of this Ammonite issue:
                --    https://github.com/com-lihaoyi/Ammonite/issues/276
                -- Also, script for Mac and script for Linux have incompatible CLIs for unfathomable reasons.
                -- Also, we need to set TERM to something, otherwise tput complains and crashes Ammonite.
                wrappedCmd
                    | isWindows = cmd
                    | isMac = "script -q -- tty.txt " <> cmd
                    | otherwise = concat ["script -q -c '", cmd, "'"]
                input =
                    [ "sandbox.health.is_running"
                    , "local.health.is_running"
                    , "exit" -- This "exit" is necessary on Linux, otherwise the REPL expects more input.
                             -- script on Linux doesn't transmit the EOF/^D to the REPL, unlike on Mac.
                    ]
                env' | isWindows || isJust (lookup "TERM" env) = Nothing
                     | otherwise = Just (("TERM", "xterm-256color") : env)
                proc' = (shell wrappedCmd) { cwd = Just dir, env = env' }
            output <- readCreateProcess proc' (unlines input)
            let outputLines = lines output
            -- NOTE (Sofia): We use `isInfixOf` extensively because
            --   the REPL output is full of color codes.
            res0 <- case find (isInfixOf "res0") outputLines of
                      Just res0 -> pure res0
                      _ -> fail output
            assertBool "sandbox participant is not running" ("true" `isInfixOf` res0)
            Just res1 <- pure (find (isInfixOf "res1") outputLines)
            assertBool "local domain is not running" ("true" `isInfixOf` res1)

    ]
