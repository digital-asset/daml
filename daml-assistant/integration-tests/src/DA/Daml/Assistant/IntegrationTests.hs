-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.IntegrationTests (main) where

import Conduit hiding (connect)
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception.Extra
import Control.Monad
import qualified Data.Aeson as Aeson
import qualified Data.Conduit.Tar.Extra as Tar.Conduit.Extra
import qualified Data.HashMap.Strict as HashMap
import Data.List.Extra
import qualified Data.Map as Map
import Data.Maybe (maybeToList)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Typeable (Typeable)
import Network.HTTP.Client
import Network.HTTP.Types
import Network.Socket
import System.Directory.Extra
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import qualified Web.JWT as JWT

import DA.Bazel.Runfiles
import DA.Daml.Assistant.FreePort (getFreePort, socketHints)
import DA.Daml.Assistant.IntegrationTestUtils
import DA.Daml.Helper.Util (waitForConnectionOnPort, waitForHttpServer)
import DA.PortFile
import DA.Test.Daml2jsUtils
import DA.Test.Process (callCommandSilent)
import DA.Test.Util
import SdkVersion

main :: IO ()
main = do
    yarn : args <- getArgs
    withTempDir $ \tmpDir -> do
    oldPath <- getSearchPath
    javaPath <- locateRunfiles "local_jdk/bin"
    mvnPath <- locateRunfiles "mvn_dev_env/bin"
    tarPath <- locateRunfiles "tar_dev_env/bin"
    yarnPath <- takeDirectory <$> locateRunfiles (mainWorkspace </> yarn)
    -- NOTE: `COMSPEC` env. variable on Windows points to cmd.exe, which is required to be present
    -- on the PATH as mvn.cmd executes cmd.exe
    mbComSpec <- getEnv "COMSPEC"
    let mbCmdDir = takeDirectory <$> mbComSpec
    limitJvmMemory defaultJvmMemoryLimits
    withArgs args (withEnv
        [ ("PATH", Just $ intercalate [searchPathSeparator] $ (tarPath : javaPath : mvnPath : yarnPath : oldPath) ++ maybeToList mbCmdDir)
        , ("TASTY_NUM_THREADS", Just "1")
        ] $ defaultMain (tests tmpDir))

tests :: FilePath -> TestTree
tests tmpDir = withSdkResource $ \_ -> testGroup "Integration tests"
    [ testCase "daml version" $ callCommandSilent "daml version"
    , testCase "daml --help" $ callCommandSilent "daml --help"
    , testCase "daml new --list" $ callCommandSilent "daml new --list"
    , packagingTests
    , quickstartTests quickstartDir mvnDir
    , cleanTests cleanDir
    , templateTests
    , codegenTests codegenDir
    ]
    where quickstartDir = tmpDir </> "q-u-i-c-k-s-t-a-r-t"
          cleanDir = tmpDir </> "clean"
          mvnDir = tmpDir </> "m2"
          codegenDir = tmpDir </> "codegen"

-- Most of the packaging tests are in the a separate test suite in
-- //compiler/damlc/tests:packaging. This only has a couple of
-- integration tests.
packagingTests :: TestTree
packagingTests = testGroup "packaging"
     [ testCase "Build copy trigger" $ withTempDir $ \tmpDir -> do
        let projDir = tmpDir </> "copy-trigger"
        callCommandSilent $ unwords ["daml", "new", projDir, "--template=copy-trigger"]
        withCurrentDirectory projDir $ callCommandSilent "daml build"
        let dar = projDir </> ".daml" </> "dist" </> "copy-trigger-0.0.1.dar"
        assertFileExists dar
     , testCase "Build copy trigger with LF version 1.dev" $ withTempDir $ \tmpDir -> do
        let projDir = tmpDir </> "copy-trigger"
        callCommandSilent $ unwords ["daml", "new", projDir, "--template=copy-trigger"]
        withCurrentDirectory projDir $ callCommandSilent "daml build --target 1.dev"
        let dar = projDir </> ".daml" </> "dist" </> "copy-trigger-0.0.1.dar"
        assertFileExists dar
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
          [ "module MyDep where"
          ]
        withCurrentDirectory myDepDir $ callCommandSilent "daml build -o mydep.dar"
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
            [ "module Main where"
            , "import MyDep ()"
            , "import Daml.Trigger ()"
            ]
        withCurrentDirectory myTriggerDir $ callCommandSilent "daml build -o mytrigger.dar"
        let dar = myTriggerDir </> "mytrigger.dar"
        assertFileExists dar
     , testCase "Build DAML script example" $ withTempDir $ \tmpDir -> do
        let projDir = tmpDir </> "script-example"
        callCommandSilent $ unwords ["daml", "new", projDir, "--template=script-example"]
        withCurrentDirectory projDir $ callCommandSilent "daml build"
        let dar = projDir </> ".daml/dist/script-example-0.0.1.dar"
        assertFileExists dar
     , testCase "Build DAML script example with LF version 1.dev" $ withTempDir $ \tmpDir -> do
        let projDir = tmpDir </> "script-example"
        callCommandSilent $ unwords ["daml", "new", projDir, "--template=script-example"]
        withCurrentDirectory projDir $ callCommandSilent "daml build --target 1.dev"
        let dar = projDir </> ".daml/dist/script-example-0.0.1.dar"
        assertFileExists dar
     , testCase "Package depending on daml-script and daml-trigger can use data-dependencies" $ withTempDir $ \tmpDir -> do
        callCommandSilent $ unwords ["daml", "new", tmpDir </> "data-dependency"]
        withCurrentDirectory (tmpDir </> "data-dependency") $ callCommandSilent "daml build -o data-dependency.dar"
        createDirectoryIfMissing True (tmpDir </> "proj")
        writeFileUTF8 (tmpDir </> "proj" </> "daml.yaml") $ unlines
          [ "sdk-version: " <> sdkVersion
          , "name: proj"
          , "version: 0.0.1"
          , "source: ."
          , "dependencies: [daml-prim, daml-stdlib, daml-script, daml-trigger]"
          , "data-dependencies: [" <> show (tmpDir </> "data-dependency" </> "data-dependency.dar") <> "]"
          ]
        writeFileUTF8 (tmpDir </> "proj" </> "A.daml") $ unlines
          [ "module A where"
          , "import Daml.Script"
          , "import Main"
          , "f = setup >> allocateParty \"foobar\""
          -- This also checks that we get the same Script type within an SDK version.
          ]
        withCurrentDirectory (tmpDir </> "proj") $ callCommandSilent "daml build"
     , testCase "DAML Script --input-file and --output-file" $ withTempDir $ \projDir -> do
           writeFileUTF8 (projDir </> "daml.yaml") $ unlines
               [ "sdk-version: " <> sdkVersion
               , "name: proj"
               , "version: 0.0.1"
               , "source: ."
               , "dependencies: [daml-prim, daml-stdlib, daml-script]"
               ]
           writeFileUTF8 (projDir </> "Main.daml") $ unlines
               [ "module Main where"
               , "import Daml.Script"
               , "test : Int -> Script (Int, Int)"
               , "test x = pure (x, x + 1)"
               ]
           withCurrentDirectory projDir $ do
               callCommandSilent "daml build -o script.dar"
               writeFileUTF8 (projDir </> "input.json") "0"
               p :: Int <- fromIntegral <$> getFreePort
               withDevNull $ \devNull ->
                   withCreateProcess (shell $ unwords ["daml sandbox --port " <> show p]) { std_out = UseHandle devNull } $ \_ _ _ _ -> do
                       waitForConnectionOnPort (threadDelay 100000) p
                       callCommand $ unwords
                           [ "daml script"
                           , "--dar script.dar --script-name Main:test"
                           , "--input-file input.json --output-file output.json"
                           , "--ledger-host localhost --ledger-port " <> show p
                           ]
           contents <- readFileUTF8 (projDir </> "output.json")
           lines contents @?=
               [ "{"
               , "  \"_1\": 0,"
               , "  \"_2\": 1"
               , "}"
               ]
     , testCase "Run init-script" $ withTempDir $ \tmpDir -> do
        let projDir = tmpDir </> "init-script-example"
        createDirectoryIfMissing True (projDir </> "daml")
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> sdkVersion
          , "name: init-script-example"
          , "version: \"1.0\""
          , "source: daml"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          , "  - daml-script"
          , "parties:"
          , "- Alice"
          , "init-script: Main:init"
          , "sandbox-options:"
          , "  - --wall-clock-time"
          ]
        writeFileUTF8 (projDir </> "daml/Main.daml") $ unlines
          [ "module Main where"
          , "import Daml.Script"
          , "template T with p : Party where signatory p"
          , "init : Script ()"
          , "init = do"
          , "  alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"Alice\")"
          , "  alice `submit` createCmd (T alice)"
          , "  pure ()"
          ]
        sandboxPort :: Int <- fromIntegral <$> getFreePort
        jsonApiPort :: Int <- fromIntegral <$> getFreePort
        let startProc = shell $ unwords
              [ "daml"
              , "start"
              , "--start-navigator"
              , "no"
              , "--sandbox-port"
              , show sandboxPort
              , "--json-api-port"
              , show jsonApiPort
              ]
        withCurrentDirectory projDir $
          withCreateProcess startProc $ \_ _ _ startPh ->
            race_ (waitForProcess' startProc startPh) $ do
              -- The hard-coded secret for testing is "secret".
              let token = JWT.encodeSigned (JWT.HMACSecret "secret") mempty mempty
                    { JWT.unregisteredClaims = JWT.ClaimsMap $
                          Map.fromList [("https://daml.com/ledger-api", Aeson.Object $ HashMap.fromList [("actAs", Aeson.toJSON ["Alice" :: T.Text]), ("ledgerId", "MyLedger"), ("applicationId", "foobar")])]
                    }
              let headers =
                    [ ("Authorization", "Bearer " <> T.encodeUtf8 token)
                    ] :: RequestHeaders
              waitForHttpServer (threadDelay 100000) ("http://localhost:" <> show jsonApiPort <> "/v1/query") headers
              initialRequest <- parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/query"
              let queryRequest = initialRequest
                    { method = "POST"
                    , requestHeaders = headers
                    , requestBody = RequestBodyLBS $ Aeson.encode $ Aeson.object
                        ["templateIds" Aeson..= [Aeson.String "Main:T"]]
                    }
              manager <- newManager defaultManagerSettings
              queryResponse <- httpLbs queryRequest manager
              statusCode (responseStatus queryResponse) @?= 200
              case Aeson.decode (responseBody queryResponse) of
                Just (Aeson.Object body)
                  | Just (Aeson.Array result) <- HashMap.lookup "result" body
                  -> length result @?= 1
                _ -> assertFailure "Expected JSON object in response body"
              -- waitForProcess' will block on Windows so we explicitly kill the process.
              terminateProcess startPh
     , testCase "sandbox-options is picked up" $ withTempDir $ \tmpDir -> do
        let projDir = tmpDir </> "sandbox-options"
        createDirectoryIfMissing True (projDir </> "daml")
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> sdkVersion
          , "name: sandbox-options"
          , "version: \"1.0\""
          , "source: daml"
          , "sandbox-options:"
          , "  - --wall-clock-time"
          , "  - --ledgerid=MyLedger"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          ]
        writeFileUTF8 (projDir </> "daml/Main.daml") $ unlines
          [ "module Main where"
          , "template T with p : Party where signatory p"
          ]
        sandboxPort :: Int <- fromIntegral <$> getFreePort
        jsonApiPort :: Int <- fromIntegral <$> getFreePort
        let startProc = shell $ unwords
              [ "daml"
              , "start"
              , "--start-navigator=no"
              , "--sandbox-port=" <> show sandboxPort
              , "--json-api-port=" <> show jsonApiPort
              ]
        withCurrentDirectory projDir $
          withCreateProcess startProc $ \_ _ _ startPh ->
            race_ (waitForProcess' startProc startPh) $ do
              let token = JWT.encodeSigned (JWT.HMACSecret "secret") mempty mempty
                    { JWT.unregisteredClaims = JWT.ClaimsMap $
                          Map.fromList [("https://daml.com/ledger-api", Aeson.Object $ HashMap.fromList [("actAs", Aeson.toJSON ["Alice" :: T.Text]), ("ledgerId", "MyLedger"), ("applicationId", "foobar")])]
                    }
              let headers =
                    [ ("Authorization", "Bearer " <> T.encodeUtf8 token)
                    ] :: RequestHeaders
              waitForHttpServer (threadDelay 100000) ("http://localhost:" <> show jsonApiPort <> "/v1/query") headers

              manager <- newManager defaultManagerSettings
              initialRequest <- parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/create"
              let createRequest = initialRequest
                    { method = "POST"
                    , requestHeaders = headers
                    , requestBody = RequestBodyLBS $ Aeson.encode $ Aeson.object
                        ["templateId" Aeson..= Aeson.String "Main:T"
                        ,"payload" Aeson..= [Aeson.String "Alice"]
                        ]
                    }
              createResponse <- httpLbs createRequest manager
              -- If the ledger id or wall clock time is not picked up this would fail.
              statusCode (responseStatus createResponse) @?= 200
              -- waitForProcess' will block on Windows so we explicitly kill the process.
              terminateProcess startPh
    , testCase "daml start --sandbox-port=0" $ withTempDir $ \tmpDir -> do
          writeFileUTF8 (tmpDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: sandbox-options"
              , "version: \"1.0\""
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "start-navigator: false"
              ]
          let startProc = shell $ unwords
                  [ "daml"
                  , "start"
                  , "--sandbox-port=0"
                  , "--sandbox-option=--ledgerid=MyLedger"
                  , "--json-api-port=0"
                  , "--json-api-option=--port-file=jsonapi.port"
                  ]
          withCurrentDirectory tmpDir $
              withCreateProcess startProc $ \_ _ _ ph -> do
              jsonApiPort <- readPortFile maxRetries "jsonapi.port"
              let token = JWT.encodeSigned (JWT.HMACSecret "secret") mempty mempty
                    { JWT.unregisteredClaims = JWT.ClaimsMap $
                          Map.fromList [("https://daml.com/ledger-api", Aeson.Object $ HashMap.fromList [("actAs", Aeson.toJSON ["Alice" :: T.Text]), ("ledgerId", "MyLedger"), ("applicationId", "foobar")])]
                    }
              let headers =
                    [ ("Authorization", "Bearer " <> T.encodeUtf8 token)
                    ] :: RequestHeaders
              initialRequest <- parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/parties/allocate"
              let queryRequest = initialRequest
                    { method = "POST"
                    , requestHeaders = headers
                    , requestBody = RequestBodyLBS $ Aeson.encode $ Aeson.object
                        [ "identifierHint" Aeson..= ("Alice" :: String)
                        ]
                    }
              manager <- newManager defaultManagerSettings
              queryResponse <- httpLbs queryRequest manager
              responseBody queryResponse @?= "{\"result\":{\"identifier\":\"Alice\",\"isLocal\":true},\"status\":200}"
              -- waitForProcess' will block on Windows so we explicitly kill the process.
              terminateProcess ph
    , testCase "run a daml ledger command" $ withTempDir $ \tmpDir -> do
          writeFileUTF8 (tmpDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: ledger-json-api"
              , "version: \"1.0\""
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              ]
          sandboxPort :: Int <- fromIntegral <$> getFreePort
          jsonApiPort :: Int <- fromIntegral <$> getFreePort
          let startProc = shell $ unwords
                [ "daml"
                , "start"
                , "--start-navigator no"
                , "--sandbox-port " <> show sandboxPort
                , "--json-api-port " <> show jsonApiPort
                ]
          withCurrentDirectory tmpDir $
            withCreateProcess startProc $ \_ _ _ startPh ->
              race_ (waitForProcess' startProc startPh) $ do
                let token =
                      JWT.encodeSigned
                        (JWT.HMACSecret "secret")
                        mempty
                        mempty
                          { JWT.unregisteredClaims =
                              JWT.ClaimsMap $
                              Map.fromList
                                [ ( "https://daml.com/ledger-api"
                                  , Aeson.Object $
                                    HashMap.fromList
                                      [ ("actAs", Aeson.toJSON ["Alice" :: T.Text])
                                      , ("ledgerId", "MyLedger")
                                      , ("applicationId", "foobar")
                                      ])
                                ]
                          }
                writeFileUTF8 (tmpDir </> "token.txt") $ T.unpack token
                let headers =
                      [("Authorization", "Bearer " <> T.encodeUtf8 token)] :: RequestHeaders
                waitForHttpServer
                  (threadDelay 100000)
                  ("http://localhost:" <> show jsonApiPort <> "/v1/query")
                  headers
                callCommand $
                  unwords
                    [ "daml"
                    , "ledger"
                    , "allocate-party"
                    , "--port"
                    , show sandboxPort
                    , "alice"
                    ]
                -- waitForProcess' will block on Windows so we explicitly kill the process.
                terminateProcess startPh
      , testCase "daml start invokes codegen" $ withTempDir $ \tmpDir -> do
            writeFileUTF8 (tmpDir </> "daml.yaml") $ unlines
                [ "sdk-version: " <> sdkVersion
                , "name: codegen"
                , "version: \"1.0\""
                , "source: ."
                , "dependencies:"
                , "- daml-prim"
                , "- daml-stdlib"
                , "start-navigator: false"
                , "codegen:"
                , "  js:"
                , "    output-directory: ui/daml.js"
                , "    npm-scope: daml.js"
                , "  java:"
                , "    output-directory: ui/java"
                , "  scala:"
                , "    output-directory: ui/scala"
                -- this configuration option shouldn't be mandatory according to docs, but it is.
                -- See https://github.com/digital-asset/daml/issues/7547.
                , "    package-prefix: com.digitalasset"
                ]
            let startProc = shell $ unwords
                    [ "daml"
                    , "start"
                    ]
            withCurrentDirectory tmpDir $
              withCreateProcess startProc $ \_ _ _ startPh -> do
              race_ (waitForProcess' startProc startPh) $ do
                -- 15 secs for all the codegens to complete
                threadDelay 15000000
                didGenerateJsCode <-
                  doesFileExist ("ui" </> "daml.js" </> "codegen-1.0" </> "package.json")
                didGenerateJavaCode <-
                  doesFileExist
                    ("ui" </> "java" </> "da" </> "internal" </> "template" </> "Archive.java")
                didGenerateScalaCode <- doesFileExist ("ui" </> "scala" </> "com" </> "digitalasset" </> "PackageIDs.scala" )
                didGenerateJsCode @?= True
                didGenerateJavaCode @?= True
                didGenerateScalaCode @?= True
                terminateProcess startPh
     , testCase "daml start with relative DAML_PROJECT path" $ withTempDir $ \tmpDir -> do
        let projDir = tmpDir </> "project"
        createDirectoryIfMissing True (projDir </> "daml")
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> sdkVersion
          , "name: sandbox-options"
          , "version: \"1.0\""
          , "source: daml"
          , "sandbox-options:"
          , "  - --wall-clock-time"
          , "  - --ledgerid=MyLedger"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          ]
        writeFileUTF8 (projDir </> "daml/Main.daml") $ unlines
          [ "module Main where"
          , "template T with p : Party where signatory p"
          ]
        sandboxPort :: Int <- fromIntegral <$> getFreePort
        jsonApiPort :: Int <- fromIntegral <$> getFreePort
        let startProc = shell $ unwords
              [ "daml"
              , "start"
              , "--start-navigator=no"
              , "--sandbox-port=" <> show sandboxPort
              , "--json-api-port=" <> show jsonApiPort
              ]
        withCurrentDirectory tmpDir $ withEnv [("DAML_PROJECT", Just "project")] $
          withCreateProcess startProc $ \_ _ _ startPh ->
            race_ (waitForProcess' startProc startPh) $ do
              let token = JWT.encodeSigned (JWT.HMACSecret "secret") mempty mempty
                    { JWT.unregisteredClaims = JWT.ClaimsMap $
                          Map.fromList [("https://daml.com/ledger-api", Aeson.Object $ HashMap.fromList [("actAs", Aeson.toJSON ["Alice" :: T.Text]), ("ledgerId", "MyLedger"), ("applicationId", "foobar")])]
                    }
              let headers =
                    [ ("Authorization", "Bearer " <> T.encodeUtf8 token)
                    ] :: RequestHeaders
              waitForHttpServer (threadDelay 100000) ("http://localhost:" <> show jsonApiPort <> "/v1/query") headers

              manager <- newManager defaultManagerSettings
              initialRequest <- parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/create"
              let createRequest = initialRequest
                    { method = "POST"
                    , requestHeaders = headers
                    , requestBody = RequestBodyLBS $ Aeson.encode $ Aeson.object
                        ["templateId" Aeson..= Aeson.String "Main:T"
                        ,"payload" Aeson..= [Aeson.String "Alice"]
                        ]
                    }
              createResponse <- httpLbs createRequest manager
              -- If the ledger id or wall clock time is not picked up this would fail.
              statusCode (responseStatus createResponse) @?= 200
              -- waitForProcess' will block on Windows so we explicitly kill the process.
              terminateProcess startPh
    ]

quickstartTests :: FilePath -> FilePath -> TestTree
quickstartTests quickstartDir mvnDir = testGroup "quickstart"
    [ testCase "daml new" $
          callCommandSilent $ unwords ["daml", "new", quickstartDir, "--template=quickstart-java"]
    , testCase "daml build" $ withCurrentDirectory quickstartDir $
          callCommandSilent "daml build"
    , testCase "daml test" $ withCurrentDirectory quickstartDir $
          callCommandSilent "daml test"
    , testCase "daml damlc test --files" $ withCurrentDirectory quickstartDir $
          callCommandSilent "daml damlc test --files daml/Main.daml"
    , testCase "daml damlc visual-web" $ withCurrentDirectory quickstartDir $
          callCommandSilent $ unwords ["daml damlc visual-web .daml/dist/quickstart-0.0.1.dar -o visual.html -b"]
    , testCase "Sandbox startup" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull -> do
          p :: Int <- fromIntegral <$> getFreePort
          let sandboxProc = (shell $ unwords ["daml", "sandbox", "--wall-clock-time", "--port", show p, ".daml/dist/quickstart-0.0.1.dar"]) { std_out = UseHandle devNull, std_in = CreatePipe }
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
    , testCase "Sandbox Classic startup" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull -> do
          p :: Int <- fromIntegral <$> getFreePort
          let sandboxProc = (shell $ unwords ["daml", "sandbox-classic", "--wall-clock-time", "--port", show p, ".daml/dist/quickstart-0.0.1.dar"]) { std_out = UseHandle devNull, std_in = CreatePipe }
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
          let sandboxProc = (shell $ unwords ["daml", "sandbox", "--wall-clock-time", "--port", show sandboxPort, ".daml/dist/quickstart-0.0.1.dar"]) { std_out = UseHandle devNull1, std_in = CreatePipe }
          withCreateProcess sandboxProc  $ \_ _ _ sandboxPh -> race_ (waitForProcess' sandboxProc sandboxPh) $ do
              waitForConnectionOnPort (threadDelay 100000) sandboxPort
              navigatorPort :: Int <- fromIntegral <$> getFreePort
              let navigatorProc = (shell $ unwords ["daml", "navigator", "server", "localhost", show sandboxPort, "--port", show navigatorPort]) { std_out = UseHandle devNull2, std_in = CreatePipe }
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
          let sandboxProc = (shell $ unwords ["daml", "sandbox", "--wall-clock-time", "--port", show sandboxPort, ".daml/dist/quickstart-0.0.1.dar"]) { std_out = UseHandle devNull1, std_in = CreatePipe }
          withCreateProcess sandboxProc  $ \_ _ _ sandboxPh -> race_ (waitForProcess' sandboxProc sandboxPh) $ do
              waitForConnectionOnPort (threadDelay 100000) sandboxPort
              jsonApiPort :: Int <- fromIntegral <$> getFreePort
              let jsonApiProc = (shell $ unwords ["daml", "json-api", "--ledger-host", "localhost", "--ledger-port", show sandboxPort, "--http-port", show jsonApiPort, "--allow-insecure-tokens"]) { std_out = UseHandle devNull2, std_in = CreatePipe }
              withCreateProcess jsonApiProc $ \_ _ _ jsonApiPh -> race_ (waitForProcess' jsonApiProc jsonApiPh) $ do
                  let headers =
                          [ ("Authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJsZWRnZXJJZCI6Ik15TGVkZ2VyIiwiYXBwbGljYXRpb25JZCI6ImZvb2JhciIsInBhcnR5IjoiQWxpY2UifQ.4HYfzjlYr1ApUDot0a6a4zB49zS_jrwRUOCkAiPMqo0")
                          ] :: RequestHeaders
                  waitForHttpServer (threadDelay 100000) ("http://localhost:" <> show jsonApiPort <> "/v1/query") headers
                  req <- parseRequest $ "http://localhost:" <> show jsonApiPort <> "/v1/query"
                  req <- pure req { requestHeaders = headers }
                  manager <- newManager defaultManagerSettings
                  resp <- httpLbs req manager
                  responseBody resp @?=
                      "{\"result\":[],\"status\":200}"
                  -- waitForProcess' will block on Windows so we explicitly kill the process.
                  terminateProcess jsonApiPh
              terminateProcess sandboxPh
    , testCase "trigger service startup" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull1 -> do
      withDevNull $ \devNull2 -> do
      withDevNull $ \devNull3 -> do
          sandboxPort :: Int <- fromIntegral <$> getFreePort
          let sandboxProc = (shell $ unwords ["daml", "sandbox", "--wall-clock-time", "--port", show sandboxPort, ".daml/dist/quickstart-0.0.1.dar"]) { std_out = UseHandle devNull1, std_in = CreatePipe }
          withCreateProcess sandboxProc  $ \_ _ _ sandboxPh -> race_ (waitForProcess' sandboxProc sandboxPh) $ do
              waitForConnectionOnPort (threadDelay 100000) sandboxPort
              triggerServicePort :: Int <- fromIntegral <$> getFreePort
              let triggerServiceProc = (shell $ unwords ["daml", "trigger-service", "--ledger-host", "localhost", "--ledger-port", show sandboxPort, "--http-port", show triggerServicePort, "--wall-clock-time"]) { std_out = UseHandle devNull2, std_err = UseHandle devNull3, std_in = CreatePipe }
              withCreateProcess triggerServiceProc $ \_ _ _ triggerServicePh -> race_ (waitForProcess' triggerServiceProc triggerServicePh) $ do
                let endpoint = "http://localhost:" <> show triggerServicePort <> "/v1/health"
                waitForHttpServer (threadDelay 100000) endpoint []
                req <- parseRequest endpoint
                manager <- newManager defaultManagerSettings
                resp <- httpLbs req manager
                responseBody resp @?= "{\"status\":\"pass\"}"
                -- waitForProcess' will block on Windows so we
                -- explicitly kill the process.
                terminateProcess triggerServicePh
              terminateProcess sandboxPh
    , testCase "mvn compile" $
      withCurrentDirectory quickstartDir $ do
          mvnDbTarball <- locateRunfiles (mainWorkspace </> "daml-assistant" </> "integration-tests" </> "integration-tests-mvn.tar")
          runConduitRes
            $ sourceFileBS mvnDbTarball
            .| Tar.Conduit.Extra.untar (Tar.Conduit.Extra.restoreFile throwError mvnDir)
          callCommand "daml codegen java"
          callCommand $ unwords ["mvn", mvnRepoFlag, "-q", "compile"]
    , testCase "mvn exec:java@run-quickstart" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull1 ->
      withDevNull $ \devNull2 -> do
          sandboxPort :: Int <- fromIntegral <$> getFreePort
          let sandboxProc = (shell $ unwords ["daml", "sandbox", "--", "--port", show sandboxPort, "--", "--static-time", ".daml/dist/quickstart-0.0.1.dar"]) { std_out = UseHandle devNull1, std_in = CreatePipe }
          withCreateProcess sandboxProc $
              \_ _ _ ph -> race_ (waitForProcess' sandboxProc ph) $ do
              waitForConnectionOnPort (threadDelay 500000) sandboxPort
              callCommandSilent $ unwords
                    [ "daml script"
                    , "--dar .daml/dist/quickstart-0.0.1.dar"
                    , "--script-name Main:initialize"
                    , "--static-time"
                    , "--ledger-host localhost"
                    , "--ledger-port", show sandboxPort
                    ]
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
                    callCommandSilent $ unwords ["daml", "new", projectDir, "--template", templateName]
                    withCurrentDirectory projectDir $ do
                        filesAtStart <- sort <$> listFilesRecursive "."
                        callCommandSilent "daml build"
                        callCommandSilent "daml clean"
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

templateTests :: TestTree
templateTests = testGroup "templates" $
    [ testCase name $ do
          withTempDir $ \dir -> withCurrentDirectory dir $ do
              callCommandSilent $ unwords ["daml", "new", "foobar", "--template", name]
              withCurrentDirectory (dir </> "foobar") $ callCommandSilent "daml build"
    | name <- templateNames
    ] <>
    [ testCase "quickstart-java, positional template" $ do
          -- Verify that the old syntax for `daml new` still works.
          withTempDir $ \dir -> withCurrentDirectory dir $ do
              callCommandSilent "daml new foobar quickstart-java"
              contents <- readFileUTF8 "foobar/daml.yaml"
              assertInfixOf "name: quickstart" contents
    ]



  -- NOTE (MK) We might want to autogenerate this list at some point but for now
  -- this should be good enough.
  where templateNames =
            [ "copy-trigger"
            -- daml-intro-1 - daml-intro-6 are not full projects.
            , "daml-intro-7"
            , "daml-patterns"
            , "quickstart-java"
            , "quickstart-scala"
            , "script-example"
            , "skeleton"
            , "create-daml-app"
            ]

-- | Check we can generate language bindings.
codegenTests :: FilePath -> TestTree
codegenTests codegenDir = testGroup "daml codegen" (
    [ codegenTestFor "java" Nothing
    , codegenTestFor "scala" (Just "com.cookiemonster.nomnomnom")
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
                withCurrentDirectory codegenDir $ do
                    let projectDir = codegenDir </> ("proj-" ++ lang)
                    callCommandSilent $ unwords ["daml new", projectDir, "--template=skeleton"]
                    withCurrentDirectory projectDir $ do
                        callCommandSilent "daml build"
                        let darFile = projectDir </> ".daml/dist/proj-" ++ lang ++ "-0.0.1.dar"
                            outDir  = projectDir </> "generated" </> lang
                        when (lang == "js") $ do
                            let workspaces = Workspaces [makeRelative codegenDir outDir]
                            setupYarnEnv codegenDir workspaces [DamlTypes, DamlLedger]
                        callCommandSilent $
                          unwords [ "daml", "codegen", lang
                                  , darFile ++ maybe "" ("=" ++) namespace
                                  , "-o", outDir]
                        contents <- listDirectory outDir
                        assertBool "bindings were written" (not $ null contents)

-- | Like `waitForProcess` but throws ProcessExitFailure if the process fails to start.
waitForProcess' :: CreateProcess -> ProcessHandle -> IO ()
waitForProcess' cp ph = do
    e <- waitForProcess ph
    unless (e == ExitSuccess) $ throwIO $ ProcessExitFailure e cp

data ProcessExitFailure = ProcessExitFailure !ExitCode !CreateProcess
    deriving (Show, Typeable)

instance Exception ProcessExitFailure
