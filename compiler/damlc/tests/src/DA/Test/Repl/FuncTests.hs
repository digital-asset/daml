-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Test.Repl.FuncTests (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Monad.Extra
import DA.Bazel.Runfiles
import DA.Cli.Damlc.Packaging
import DA.Cli.Damlc.DependencyDb
import DA.Daml.Compiler.Repl
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.ReplClient as ReplClient
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import DA.Daml.Project.Types
import Data.Either
import DA.Test.Sandbox
import DA.Test.Util
import Development.IDE.Core.IdeState.Daml
import Development.IDE.Types.Location
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger
import GHC.IO.Handle
import SdkVersion
import System.Directory
import System.FilePath
import System.IO.Extra
import System.IO.Silently
import System.Process
import Test.Hspec
import Text.Regex.TDFA

-- NOTE (MK) This test suite tests the repl as a library rather than creating a separate
-- process for each test. This is significantly faster but comes with a few challenges:
--
-- 1. repline (and for the most part haskeline) do not provide a way to supply custom handles
-- 2. This means that we have to temporarily redirect stdin and stdout to capture them.
-- 3. Tasty interleaves its own output with the output of tests. This breaks any attempts
--    at capturing stdout.
-- 4. hspec in single-threaded mode luckily works as we want to so for now, this is our only
--    hspec test suite. Another option would be to write a reporter for tasty that prints to
--    stderr.

main :: IO ()
main =
    hspec $
        describe "repl func tests" $
            [minBound @LF.MajorVersion .. maxBound] `forM_` \major ->
                context ("LF version " <> LF.renderMajorVersion major) $
                    aroundAll
                        (withInteractionTester major)
                        (sequence_ functionalTests)

type InteractionTester = [Step] -> Expectation

withInteractionTester :: LF.MajorVersion -> ActionWith InteractionTester -> IO ()
withInteractionTester major action = do
    let prettyMajor = LF.renderMajorVersion major
    let lfVersion =
            case major of
                LF.V1 -> LF.versionDefault
                -- TODO(#17366): test with the latest stable version of LF2 once there is one
                LF.V2 -> LF.version2_dev
    let options =
            (defaultOptions Nothing)
                { optScenarioService = EnableScenarioService False
                , optDamlLfVersion = lfVersion
                }
    setNumCapabilities 1
    limitJvmMemory defaultJvmMemoryLimits
    scriptDar <- locateRunfiles $ case major of
        LF.V1 -> mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar"
        LF.V2 -> mainWorkspace </> "daml-script" </> "daml3" </> "daml3-script.dar"
    testDars <- forM ["repl-test", "repl-test-two"] $ \name ->
        locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> name <> "-v" <> prettyMajor <.> "dar")
    replDir <- locateRunfiles (mainWorkspace </> "compiler/repl-service/server")
    forM_ [stdin, stdout, stderr] $ \h -> hSetBuffering h LineBuffering
    let replJar = replDir </> "repl-service.jar"
    -- We have to keep draining the service stdout otherwise it might get blocked
    -- if the buffer is full. Therefore we write asynchronously to a Chan
    -- and read from there.
    serviceLineChan <- newChan
    -- Turning the repl client into a resource is a bit annoying since it requires
    -- pulling apart functions like `withGrpcClient`. Therefore we just
    -- allocate the resources before handing over to tasty and accept that
    -- it will spin up sandbox and the repl client.
    withTempDir $ \tmpDir ->
        withBinaryFile nullDevice WriteMode $ \devNull ->
        bracket
            ( createCantonSandbox
                tmpDir
                devNull
                defaultSandboxConf{dars = testDars, devVersionSupport = LF.isDevVersion lfVersion}
            )
            destroySandbox
            $ \SandboxResource{sandboxPort} ->
        ReplClient.withReplClient
             ReplClient.Options
                { optServerJar = replJar
                , optLedgerConfig = Just ("localhost", show sandboxPort)
                , optMbAuthTokenFile = Nothing
                , optMbApplicationId = Nothing
                , optMbSslConfig = Nothing
                , optMaxInboundMessageSize = Nothing
                , optTimeMode = ReplClient.ReplWallClock
                , optMajorLfVersion = major
                , optStdout = CreatePipe
                }
            $ \replHandle ->
            -- TODO We could share some of this setup with the actual repl code in damlc.
            withTempDir $ \dir ->
            withCurrentDirectory dir $ do
            Just serviceOut <- pure (ReplClient.hStdout replHandle)
            hSetBuffering serviceOut LineBuffering
            withAsync (drainHandle serviceOut serviceLineChan) $ \_ -> do
                initPackageConfig options scriptDar testDars
                logger <- Logger.newStderrLogger Logger.Warning "repl-tests"
                replLogger <- newReplLogger
                withDamlIdeState options logger (replEventLogger replLogger) $ \ideState ->
                    action (testInteraction replHandle replLogger serviceLineChan options ideState) `finally`
                        -- We need to kill the process to avoid getting stuck in hGetLine on Windows.
                        ReplClient.hTerminate replHandle

initPackageConfig :: Options -> FilePath -> [FilePath] -> IO ()
initPackageConfig options scriptDar dars = do
    writeFileUTF8 "daml.yaml" $ unlines $
        [ "sdk-version: " <> sdkVersion
        , "name: repl"
        , "version: 0.0.1"
        , "source: ."
        , "dependencies:"
        , "- daml-prim"
        , "- daml-stdlib"
        , "- " <> show scriptDar
        , "data-dependencies:"
        ] ++ ["- " <> show dar | dar <- dars]
    withPackageConfig (ProjectPath ".") $ \PackageConfigFields {..} -> do
        dir <- getCurrentDirectory
        installDependencies
            (toNormalizedFilePath' dir)
            options
            pSdkVersion
            pDependencies
            pDataDependencies
        createProjectPackageDb (toNormalizedFilePath' dir) options pModulePrefixes

drainHandle :: Handle -> Chan String -> IO ()
drainHandle handle chan = forever $ do
    line <- hGetLine handle
    writeChan chan line

functionalTests :: [SpecWith InteractionTester]
functionalTests =
    [ testInteraction' "create and query"
          [ input "alice <- allocateParty \"Alice\""
          , input "debug =<< query @T alice"
          , matchServiceOutput "^.*: \\[\\]$"
          , input "_ <- submit alice $ createCmd (T alice alice)"
          , input "debug =<< query @T alice"
          , matchServiceOutput "^.*: \\[\\([0-9a-f]+,T {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
          ]
    , testInteraction' "propose and accept"
          [ input "alice <- allocateParty \"Alice\""
          , input "bob <- allocateParty \"Bob\""
          , input "_ <- submit alice $ createCmd (TProposal alice bob)"
          , input "props <- query @TProposal bob"
          , input "debug props"
          , matchServiceOutput "^.*: \\[\\([0-9a-f]+,TProposal {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
          , input "forA props $ \\(prop, _) -> submit bob $ exerciseCmd prop Accept"
          , matchOutput "^\\[[0-9a-f]+\\]$"
          , input "debug =<< query @T bob"
          , matchServiceOutput "^.*: \\[\\([0-9a-f]+,T {proposer = '[^']+', accepter = '[^']+'}.*\\)\\]$"
          , input "debug =<< query @TProposal bob"
          , matchServiceOutput "^.*: \\[\\]$"
          ]
    , testInteraction' "shadowing"
          [ input "x <- pure 1"
          , input "debug x"
          , matchServiceOutput "^.*: 1$"
          , input "x <- pure $ x + x"
          , input "debug x"
          , matchServiceOutput "^.*: 2$"
          ]
    , testInteraction' "parse error"
          [ input "eaiu\\1"
          , matchOutput "^parse error.*$"
          , input "debug 1"
          , matchServiceOutput "^.*: 1"
          ]
    , testInteraction' "Tuple patterns"
          [ input "(a, b) <- pure (1, 2)"
          , input "(b, c) <- pure (3, 4)"
          , input "debug a"
          , matchServiceOutput "^.*: 1"
          , input "debug b"
          , matchServiceOutput "^.*: 3"
          , input "debug c"
          , matchServiceOutput "^.*: 4"
          ]
    , testInteraction' "Partial patterns"
          -- TODO (MK) We do not test failing patterns yet
          -- since `error` calls aren’t handled nicely atm.
          [ input "Some (x, Some y) <- pure (Some (1, Some 2))"
          , input "debug x"
          , matchServiceOutput "^.*: 1"
          , input "debug y"
          , matchServiceOutput "^.*: 2"
          ]
    , testInteraction' "type error"
          [ input "1 + \"\""
          -- TODO Make this less noisy
          , matchOutput "^File:.*$"
          , matchOutput "^Hidden:.*$"
          , matchOutput "^Range:.*$"
          , matchOutput "^Source:.*$"
          , matchOutput "^Severity:.*$"
          , matchOutput "^Message:.*$"
          , matchOutput "^.*error.*$"
          , matchOutput "^.*expected type .*Int.* with actual type .*Text.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , matchOutput "^.*$"
          , input "debug 1"
          , matchServiceOutput "^.*: 1"
          ]
    , testInteraction' "script error"
          [ input "alice <- allocateParty \"Alice\""
          , input "bob <- allocateParty \"Bob\""
          , input "submit alice (createCmd (T alice bob))"
          , matchOutput "^.*requires authorizers.*but only.*were given.*$"
          , input "debug 1"
          , matchServiceOutput "^.*: 1"
          ]
    , testInteraction' "server error"
          [ input "alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"alice_doubly_allocated\")"
          , input "alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"alice_doubly_allocated\")"
          , matchOutput "^.*INVALID_ARGUMENT\\(8,alice_do\\): The submitted command has invalid arguments: Party already exists.*$"
          , input "debug 1"
          , matchServiceOutput "^.*: 1"
          ]
    , testInteraction' "module imports"
          [ input "debug (days 1)"
          , matchOutput "^File:.*$"
          , matchOutput "^Hidden:.*$"
          , matchOutput "^Range:.*$"
          , matchOutput "^Source:.*$"
          , matchOutput "^Severity:.*$"
          , matchOutput "^Message: $"
          , matchOutput "^.*error: Variable not in scope: days.*$"
          , input "import DA.Time.Misspelled"
          , matchOutput "^File:.*$"
          , matchOutput "^Hidden:.*$"
          , matchOutput "^Range:.*$"
          , matchOutput "^Source:.*$"
          , matchOutput "^Severity:.*$"
          , matchOutput "^Message:.*$"
          , matchOutput "^.*Line0.daml.*$"
          , matchOutput "^.*Could not find module.*$"
          , matchOutput "^.*It is not a module.*$"
          , input "import DA.Time"
          , input "debug (days 1)"
          , matchServiceOutput "^.*: RelTime {microseconds = 86400000000}$"
          ]
    , testInteraction' ":module"
          [ input ":module + DA.Time DA.Assert"
          , input "assertEq (days 1) (days 1)"
          , input ":module - DA.Time"
          , input "assertEq (days 1) (days 1)"
          , matchOutput "^File:.*$"
          , matchOutput "^Hidden:.*$"
          , matchOutput "^Range:.*$"
          , matchOutput "^Source:.*$"
          , matchOutput "^Severity:.*$"
          , matchOutput "^Message: $"
          , matchOutput "^.*error: Variable not in scope: days.*$"
          ]
    , testInteraction' ":show imports"
          [ input ":module - ReplTest ReplTest2 Colliding"
          , input ":module + DA.Assert"
          , input ":show imports"
          , matchOutput "^import Daml.Script -- implicit$"
          , matchOutput "^import DA.Assert$"
          , input "import DA.Assert (assertEq)"
          , input ":show imports"
          , matchOutput "^import Daml.Script -- implicit$"
          , matchOutput "^import DA.Assert \\( assertEq \\)$"
          , matchOutput "^import DA.Assert$"
          , input ":module - DA.Assert"
          , input "import DA.Time (days)"
          , input ":show imports"
          , matchOutput "^import Daml.Script -- implicit$"
          , matchOutput "^import DA.Time \\( days \\)$"
          , input "import DA.Time (days, hours)"
          , input ":show imports"
          , matchOutput "^import Daml.Script -- implicit$"
          , matchOutput "^import DA.Time \\( days, hours \\)$"
          , input "import DA.Time (hours)"
          , input ":show imports"
          , matchOutput "^import Daml.Script -- implicit$"
          , matchOutput "^import DA.Time \\( hours \\)$"
          , matchOutput "^import DA.Time \\( days, hours \\)$"
          ]
    , testInteraction' "error call"
          [ input "error \"foobar\""
          , matchOutput "Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@[a-f0-9]+{ message = \"foobar\" }$"
          ]
    , testInteraction' "abort call"
          [ input "abort \"foobar\""
          , matchOutput "Error: Unhandled Daml exception: DA.Exception.GeneralError:GeneralError@[a-f0-9]+{ message = \"foobar\" }$"
          ]
    , testInteraction' "record dot syntax"
          [ input "alice <- allocatePartyWithHint \"Alice\" (PartyIdHint \"alice\")"
          , input "bob <- allocatePartyWithHint \"Bob\" (PartyIdHint \"bob\")"
          , input "proposal <- pure (T alice bob)"
          , input "debug proposal.proposer"
          , matchServiceOutput "'alice::[a-f0-9]+'"
          , input "debug proposal.accepter"
          , matchServiceOutput "'bob::[a-f0-9]+'"
          ]
    , testInteraction' "symbols from different DARs"
          [ input "party <- allocatePartyWithHint \"Party\" (PartyIdHint \"two_dars_party\")"
          , input "proposal <- pure (T party party)"
          , input "debug proposal"
          , matchServiceOutput "^.*: T {proposer = 'two_dars_party::[a-f0-9]+', accepter = 'two_dars_party::[a-f0-9]+'}"
          , input "t2 <- pure (T2 party)"
          , input "debug t2"
          , matchServiceOutput "^.*: T2 {owner = 'two_dars_party::[a-f0-9]+'}"
          ]
    , testInteraction' "repl output"
          [ input "pure ()" -- no output
          , input "pure (1 + 1)"
          , matchOutput "^2$"
          , input "pure (\\x -> x)" -- no output
          , input "1 + 2"
          , matchOutput "^3$"
          , input "\\x -> x"
          , matchOutput "^File:.*$"
          , matchOutput "^Hidden:.*$"
          , matchOutput "^Range:.*$"
          , matchOutput "^Source:.*$"
          , matchOutput "^Severity:.*$"
          , matchOutput "^Message:.*$"
          , matchOutput "^.*error.*$"
          , matchOutput "^.*No instance for \\(Show.*$"
          , matchOutput "^.*"
          , matchOutput "^.*"
          , matchOutput "^.*"
          , matchOutput "^.*"
          , matchOutput "^.*"
          ]
    , testInteraction' "let bindings"
          [ input "let x = 1 + 1"
          , input "x"
          , matchOutput "2"
          , input "let Some (x, y) = Some (23, 42)"
          , input "x"
          , matchOutput "23"
          , input "y"
          , matchOutput "42"
          , input "let f x = x + 1"
          , input "f 42"
          , matchOutput "43"
          ]
    , testInteraction' ":json"
          [ input ":json [1, 2, 3]"
          , matchOutput "\\[1,2,3\\]"
          , input ":json D with x = 1, y = 2"
          , matchOutput "{\"x\":1,\"y\":2}"
          , input ":json \\x -> x"
          , matchOutput "^Cannot convert non-serializable value to JSON$"
          , input ":json let x = 1"
          , matchOutput "^Expected an expression but got: let x = 1$"
          , input ":json x <- pure 1"
          , matchOutput "^Expected an expression but got: x <- pure 1$"
          , input "let x = 1"
          , input ":json x"
          , matchOutput "1"
          ]
    , testInteraction' "collision"
      -- Test that collisions are handled correctly if users qualify names.
          [ input "let x = ReplTest.NameCollision \"abc\""
          , input "x"
          , matchOutput "NameCollision {field = \"abc\"}"
          , input "y <- pure $ Colliding.NameCollision 42"
          , input "y"
          , matchOutput "NameCollision {field = 42}"
          ]
    , testInteraction' "long type"
      -- Test types which will result in line breaks when prettyprinted
          [ input "let y = ReplTest.NameCollision \"eau\""
          , input "let x = \\f g h -> f (g (h (ReplTest.NameCollision \"a\"))) (g (ReplTest.NameCollision \"b\")) : Script ()"
          , input "1"
          , matchOutput "1"
          ]
    , testInteraction' "closure"
          [ input "import qualified DA.Map as Map"
          , input "let m = Map.fromList [(1, 2)]"
          , input "m"
          , matchOutput "Map \\[\\(1,2\\)\\]"
          , input "let lookup1 k = Map.lookup k m"
          , input "lookup1 1"
          , matchOutput "^Some 2$"
          ]
    , testInteraction' "out of scope type"
          [  -- import a function to build a map but not the type itself
            input "import DA.Map (fromList)"
          , input "let m = fromList [(0,0)]"
          , input "m"
          , matchOutput "Map \\[\\(0,0\\)\\]"
          ]
    ]
  where
    testInteraction' testName steps =
        it testName $ \test -> (test steps :: Expectation)

testInteraction
    :: ReplClient.Handle
    -> ReplLogger
    -> Chan String
    -> Options
    -> IdeState
    -> InteractionTester
testInteraction replClient replLogger serviceOut options ideState steps = do
    let (inLines, outAssertions) = processSteps steps
    -- On Windows we cannot dup2 between a file handle and a pipe.
    -- Therefore, we redirect to files, run the action and assert afterwards.
    out <- withTempFile $ \stdinFile -> do
        writeFileUTF8 stdinFile (unlines inLines)
        withFile stdinFile ReadMode $ \readIn ->
            redirectingHandle stdin readIn $ do
            Right () <- ReplClient.clearResults replClient
            let imports = [(LF.PackageName name, Nothing) | name <- ["repl-test", "repl-test-two"]]
            capture_ $ runRepl imports options replClient replLogger ideState
        -- Write output to a file so we can conveniently read individual characters.
    withTempFile $ \clientOutFile -> do
        writeFileUTF8 clientOutFile out
        withFile clientOutFile ReadMode $ \clientOut ->
            forM_ outAssertions $ \case
                Prompt -> readPrompt clientOut
                MatchRegex regex regexStr producer -> do
                    line <- case producer of
                        ReplClient -> hGetLine clientOut
                        ReplService -> readChan serviceOut
                    unless (matchTest regex line) $
                        expectationFailure (show line <> " did not match " <> show regexStr)

-- Walk over the steps separating them into the input to the repl and the output assertions.
processSteps :: [Step] -> ([String], [OutputAssertion])
processSteps = partitionEithers . concatMap f
  where f (Input s) = [Left s, Right Prompt]
        f (MatchOutput a b c) = [Right (MatchRegex a b c)]

data OutputAssertion = Prompt | MatchRegex Regex String Producer

data Step
    = Input String
    -- ^ Input a line into the repl
    | MatchOutput Regex String Producer
    -- ^ Match a line of output against a given regex.
    -- The String is used for error messages since Regex
    -- does not have a Show instance.

data Producer = ReplClient | ReplService

input :: String -> Step
input = Input

matchOutput :: String -> Step
matchOutput s = MatchOutput (makeRegex s) s ReplClient

matchServiceOutput :: String -> Step
matchServiceOutput s = MatchOutput (makeRegex s) s ReplService

readPrompt :: Handle -> Expectation
readPrompt h = do
    res <- replicateM 6 (hGetChar h)
    res `shouldBe` "daml> "

-- | Based on similar functions in @main-tester@ and @silently@
--
-- We need to redirect stdin which those packages don't provide for us.
redirectingHandle :: Handle -> Handle -> IO r -> IO r
redirectingHandle from to action = do
  let redirect = do
        save <- hDuplicate from
        hDuplicateTo to from
        return save
      restore save = do
        hDuplicateTo save from
        hClose save
  bracket redirect restore (const action)
