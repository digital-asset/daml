-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Control.Applicative
import Control.Exception
import DA.Test.Process
import DA.Test.FreePort
import Data.List.Extra (replace)
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.SemVer (Version)
import qualified Data.SemVer as SemVer
import Data.Tagged (Tagged (..))
import Data.Time.Clock.POSIX (getPOSIXTime)
import Sandbox (maxRetries, nullDevice, readCantonPortFile)
import System.Directory.Extra (withCurrentDirectory)
import System.Environment (lookupEnv)
import System.Environment.Blank (setEnv)
import System.FilePath ((</>), takeBaseName)
import System.IO.Extra (IOMode(ReadWriteMode), hClose, newTempDir, openBinaryFile, withTempDir, writeFileUTF8)
import System.Process
import Test.Tasty (TestTree,askOption,defaultMainWithIngredients,defaultIngredients,includingOptions,testGroup,withResource)
import Test.Tasty.Options (IsOption(..), OptionDescription(..), mkOptionCLParser)
import Test.Tasty.HUnit
import qualified Bazel.Runfiles
import qualified Data.Aeson as Aeson
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Web.JWT as JWT

-- TODO(#22977) Rename platform and SDK to something clearer
data Tools = Tools
  { sdk :: FilePath
  , platformBinary :: FilePath
  , platformArgs :: [String]
  , isSdkDpm :: Bool
  , isPlatformDpm :: Bool
  }

withSandbox :: IO Tools -> Maybe String -> (IO Int -> TestTree) -> TestTree
withSandbox getTools mbSecret f =
    withResource (openBinaryFile nullDevice ReadWriteMode) hClose $ \getDevNull ->
    withResource newTempDir snd $ \getTempDir ->
      let createSandbox = do
              Tools{..} <- getTools
              (tmpDir, _) <- getTempDir
              let portFile = tmpDir </> "portfile"
              devNull <- getDevNull
              portArgs <- mapM (\argName-> fmap ((<>) argName . show) getFreePort) $ portArgNames isPlatformDpm
              let secretArgs = concat [["-C", "canton.participants.sandbox.ledger-api.auth-services.0.type=unsafe-jwt-hmac-256", "-C", "canton.participants.sandbox.ledger-api.auth-services.0.secret=" <> secret] | Just secret <- [mbSecret]]
              let args = map (tweakArg portFile) (platformArgs <> secretArgs <> portArgs)
              mask $ \unmask -> do
                  ph@(_, _, _, handle) <- createProcess (proc platformBinary args) { std_out = UseHandle devNull, create_group = True }
                  let waitForStart = do
                          port <- readCantonPortFile handle maxRetries portFile
                          pure (port, ph)
                  unmask (waitForStart `onException` cleanupProcess ph)
          destroySandbox (_, ph@(_, _, _, handle)) = do
            interruptProcessGroupOf handle
            cleanupProcess ph
      in withResource createSandbox destroySandbox (f . fmap fst)
  where
    tweakArg portFile = replace "__PORTFILE__" portFile
    portArgNames isDpm = [if isDpm then "--ledger-api-port=" else "--port=", "--admin-api-port=", "--sequencer-public-port=", "--sequencer-admin-port=", "--mediator-admin-port=", "--json-api-port="]

newtype SdkOption = SdkOption FilePath
instance IsOption SdkOption where
  defaultValue = SdkOption "sdk"
  parseValue = Just . SdkOption
  optionName = Tagged "sdk"
  optionHelp = Tagged "runfiles path to the sdk executable"

newtype PlatformOption = PlatformOption FilePath
instance IsOption PlatformOption where
  defaultValue = PlatformOption "platform"
  parseValue = Just . PlatformOption
  optionName = Tagged "platform"
  optionHelp = Tagged "runfiles path to the platform executable"

newtype SandboxArgsOption = SandboxArgsOption { unSandboxArgsOption :: [String] }
instance IsOption SandboxArgsOption where
  defaultValue = SandboxArgsOption []
  parseValue = Just . SandboxArgsOption . (:[])
  optionName = Tagged "sandbox-arg"
  optionHelp = Tagged "extra arguments to pass to sandbox executable"
  optionCLParser = concatMany (mkOptionCLParser mempty)
    where concatMany = fmap (SandboxArgsOption . concat) . some . fmap unSandboxArgsOption

withTools :: (IO Tools -> TestTree) -> TestTree
withTools tests = do
  askOption $ \(SdkOption damlPath) -> do
  askOption $ \(PlatformOption sandboxPath) -> do
  askOption $ \(SandboxArgsOption platformArgs) -> do
  let createRunfiles :: IO (FilePath -> FilePath)
      createRunfiles = do
        runfiles <- Bazel.Runfiles.create
        mainWorkspace <- fromMaybe "compatibility" <$> lookupEnv "TEST_WORKSPACE"
        pure (\path -> Bazel.Runfiles.rlocation runfiles $ mainWorkspace </> path)
  withResource createRunfiles (\_ -> pure ()) $ \locateRunfiles -> do
  let tools = do
        sdk <- locateRunfiles <*> pure damlPath
        platformBinary <- locateRunfiles <*> pure sandboxPath
        let isSdkDpm = takeBaseName sdk == "dpm"
            isPlatformDpm = takeBaseName platformBinary == "dpm"
        pure Tools
          { sdk
          , platformBinary
          , platformArgs
          , isSdkDpm
          , isPlatformDpm
          }
  tests tools

-- | This is the version of daml-helper.
newtype SdkVersion = SdkVersion Version
  deriving Eq

instance Ord SdkVersion where
    SdkVersion x <= SdkVersion y
      | y == SemVer.initial = True -- 0.0.0 is >= than anything else
      | x == SemVer.initial = False -- 0.0.0 not <= than anything other than 0.0.0
      | otherwise = x <= y -- regular semver comparison

instance IsOption SdkVersion where
  defaultValue = SdkVersion SemVer.initial
  -- Tasty seems to force the value somewhere so we cannot just set this
  -- to `error`. However, this will always be set.
  parseValue = either (const Nothing) (Just . SdkVersion) . SemVer.fromText . T.pack
  optionName = Tagged "sdk-version"
  optionHelp = Tagged "The SDK version number"

instance Show SdkVersion where
  show (SdkVersion ver) = SemVer.toString ver

-- | This is the version of canton.
newtype PlatformVersion = PlatformVersion Version
  deriving Eq

instance Ord PlatformVersion where
    PlatformVersion x <= PlatformVersion y
      | y == SemVer.initial = True -- 0.0.0 is >= than anything else
      | x == SemVer.initial = False -- 0.0.0 not <= than anything other than 0.0.0
      | otherwise = x <= y -- regular semver comparison

instance IsOption PlatformVersion where
  defaultValue = PlatformVersion SemVer.initial
  -- Tasty seems to force the value somewhere so we cannot just set this
  -- to `error`. However, this will always be set.
  parseValue = either (const Nothing) (Just . PlatformVersion) . SemVer.fromText . T.pack
  optionName = Tagged "platform-version"
  optionHelp = Tagged "The platform version number"

instance Show PlatformVersion where
  show (PlatformVersion ver) = SemVer.toString ver

main :: IO ()
main = do
  -- We manipulate global state via the working directory
  -- so running tests in parallel will cause trouble.
  setEnv "TASTY_NUM_THREADS" "1" True
  let options =
        [ Option @SdkOption Proxy
        , Option @PlatformOption Proxy
        , Option @SandboxArgsOption Proxy
        , Option @SdkVersion Proxy
        , Option @PlatformVersion Proxy
        ]
  let ingredients = defaultIngredients ++ [includingOptions options]
  defaultMainWithIngredients ingredients $
    withTools $ \getTools -> do
    askOption $ \sdkVersion -> do
    askOption $ \platformVersion -> do
    testGroup "Deployment"
      [ authenticatedUploadTest sdkVersion platformVersion getTools
      ]

-- | Test `daml script --access-token-file`
-- We want to test an elevated permission ledger-api endpoint over simple queries, so we use thw `--upload-dar` flag in dpm script
-- This uses the --access-token-file to upload the test dar to the participant
-- We are testing that the logic for sending this token on our ledger client (i.e. daml-script) is compatible with different canton versions
authenticatedUploadTest :: SdkVersion -> PlatformVersion -> IO Tools -> TestTree
authenticatedUploadTest sdkVersion platformVersion getTools = do
  withSandbox getTools (Just sharedSecret) $ \getSandboxPort -> testGroup "authentication"
    [ testCase "Bearer prefix" $ do
          Tools{..} <- getTools
          port <- getSandboxPort
          withTempDir $ \deployDir -> do
            withCurrentDirectory deployDir $ do
              let tokenFile = deployDir </> "secretToken.jwt"
              expiration <- JWT.numericDate . (+180) <$> getPOSIXTime
              -- The trailing newline is not required but we want to test that it is supported.
              writeFileUTF8 tokenFile ("Bearer " <> makeSignedAdminJwt sharedSecret expiration <> "\n")
              writeMinimalPackage sdkVersion platformVersion
              let origDar = ".daml/dist/proj1-0.0.1.dar"
              callProcessSilent sdk ["damlc", "build"]

              callProcessSilent sdk
                [ "script", "--script-name", "Main:test", "--upload-dar=yes"
                , "--dar", origDar
                , "--access-token-file", tokenFile
                , "--ledger-host", "localhost", "--ledger-port", show port
                ]
    , testCase "no Bearer prefix" $ do
          Tools{..} <- getTools
          port <- getSandboxPort
          withTempDir $ \deployDir -> do
            withCurrentDirectory deployDir $ do
              let tokenFile = deployDir </> "secretToken.jwt"
              expiration <- JWT.numericDate . (+180) <$> getPOSIXTime
              -- The trailing newline is not required but we want to test that it is supported.
              writeFileUTF8 tokenFile (makeSignedAdminJwt sharedSecret expiration <> "\n")
              writeMinimalPackage sdkVersion platformVersion
              let origDar = ".daml/dist/proj1-0.0.1.dar"
              callProcessSilent sdk ["damlc", "build"]
              callProcessSilent sdk
                [ "script", "--script-name", "Main:test", "--upload-dar=yes"
                , "--dar", origDar
                , "--access-token-file", tokenFile
                , "--ledger-host", "localhost", "--ledger-port", show port
                ]
    ]
  where
    sharedSecret = "TheSharedSecret"

makeSignedJwt :: String -> String -> Maybe JWT.NumericDate -> String
makeSignedJwt sharedSecret user expiration = do
    let urc =
            JWT.ClaimsMap $
            Map.fromList
                [ ("scope", Aeson.String "daml_ledger_api")
                ]
    let cs = mempty {
        JWT.sub = JWT.stringOrURI $ T.pack user
        , JWT.unregisteredClaims = urc
        , JWT.exp = expiration
    }
    let key = JWT.hmacSecret $ T.pack sharedSecret
    let text = JWT.encodeSigned key mempty cs
    T.unpack text

makeSignedAdminJwt :: String -> Maybe JWT.NumericDate -> String
makeSignedAdminJwt sharedSecret expiration = makeSignedJwt sharedSecret "participant_admin" expiration

semverFromTextUnsafe :: (SemVer.Version -> a) -> T.Text -> a
semverFromTextUnsafe wrap = either error wrap . SemVer.fromText

deriveBuildOptions :: SdkVersion -> PlatformVersion -> [String]
-- Need to limit LF version chosen by SDK when platform doesn't support it
-- LF 2.3 support added in 3.5, so when running 3.5+ SDK with <3.5 platform, we use LF 2.2
deriveBuildOptions sdkVersion platformVersion |
  sdkVersion >= semverFromTextUnsafe SdkVersion "3.5.0" && platformVersion < semverFromTextUnsafe PlatformVersion "3.5.0" = ["  - --target=2.2"]
deriveBuildOptions _ _ = []

-- | Write `daml.yaml` and `Main.daml` files in the current directory.
writeMinimalPackage :: SdkVersion -> PlatformVersion -> IO ()
writeMinimalPackage sdkVersion platformVersion = do
  writeFileUTF8 "daml.yaml" $ unlines $
      [ "sdk-version: " <> show sdkVersion
      , "name: proj1"
      , "version: 0.0.1"
      , "source: ."
      , "dependencies:"
      , "  - daml-prim"
      , "  - daml-stdlib"
      , "  - daml-script"
      , "build-options:"
      ] <> deriveBuildOptions sdkVersion platformVersion
  writeFileUTF8 "Main.daml" $ unlines
    [ "module Main where"
    , "import Daml.Script"
    , "template T with p : Party where signatory p"
    , "test = script $ pure \"hello\""
    ]
