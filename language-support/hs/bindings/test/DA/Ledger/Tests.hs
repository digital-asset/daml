-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

module DA.Ledger.Tests (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Monad
import Control.Monad.IO.Class(liftIO)
import DA.Bazel.Runfiles
import DA.Daml.LF.Proto3.Archive (decodeArchivePackageId)
import DA.Daml.LF.Reader(DalfManifest(..), Dalfs(..), readDalfs, readDalfManifest)
import DA.Ledger as Ledger
import DA.Test.Sandbox
import Data.List (isInfixOf,(\\))
import Data.IORef
import Prelude hiding(Enum)
import System.Environment.Blank (setEnv)
import System.FilePath
import Test.Tasty as Tasty (TestName,TestTree,testGroup,withResource,defaultMain)
import Test.Tasty.HUnit as Tasty(assertFailure,assertBool,assertEqual,testCase)
import qualified "zip-archive" Codec.Archive.Zip as Zip
import qualified DA.Daml.LF.Ast as LF
import qualified Data.ByteString as BS (readFile)
import qualified Data.ByteString.Lazy as BSL (readFile,toStrict)
import qualified Data.ByteString.UTF8 as BS (ByteString,fromString)
import qualified Data.Set as Set
import qualified Data.Text.Lazy as TL(Text,pack,unpack,fromStrict,splitOn)
import Data.Text (unpack)
import Data.Either.Extra(maybeToEither)
import qualified Data.Aeson as Aeson
import Data.Aeson.KeyMap(KeyMap)
import qualified Data.Aeson.KeyMap as KeyMap
import Data.Aeson.Key(fromString)
import Data.Aeson(decode)

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    testDar <- locateRunfiles darPath
    Tasty.defaultMain $ testGroup "Ledger bindings"
        [ sharedSandboxTests testDar
        , authenticatingSandboxTests testDar
        , tMeteringReportJson
        ]

type SandboxTest = WithSandbox -> TestTree

-- (RH) flakyTests are those tests that relie on DA.Ledger.Stream, nonFlakyTests do not
nonFlakyTests :: [WithSandbox -> TestTree]
flakyTests :: [WithSandbox -> TestTree]
nonFlakyTests =
    [ tListPackages
    , tGetPackage
    , tGetPackageBad
    , tGetPackageStatusRegistered
    , tGetPackageStatusUnknown
    , tGetParticipantId
    , tUploadDarFileBad
    , tAllocateParty
    , tMeteringReport
    ]
flakyTests =
    [ tUploadDarFileGood
    ]

-- (RH) We disable all test that rely on stream to reduce flakyness
testFlaky :: Bool
testFlaky = False

sharedSandboxTests :: FilePath -> TestTree
sharedSandboxTests testDar = testGroupWithSandbox testDar Nothing "shared sandbox"
  ( nonFlakyTests ++ if testFlaky then flakyTests else [] )

authenticatingSandboxTests :: FilePath -> TestTree
authenticatingSandboxTests testDar =
  testGroupWithSandbox testDar mbSecret "shared authenticating sandbox"
  [ tListPackages
  , tAllocateParty
  , tUploadDarFileGood
  ]
  where mbSecret = Just (Secret "Brexit-is-a-very-silly-idea")

-- | Drops the `::[0-9a-f]+` after a party name, so we can determinstically compare
dropPartyDetailHash :: PartyDetails -> PartyDetails
dropPartyDetailHash p = p {party = Party $ dropHashSuffix $ unParty $ party p}

-- | Drops the `::[0-9a-f]+` suffix from a given string
dropHashSuffix :: TL.Text -> TL.Text
dropHashSuffix = head . TL.splitOn "::"

run :: WithSandbox -> (DarMetadata -> TestId -> LedgerService ()) -> IO ()
run withSandbox f =
  withSandbox $ \sandbox maybeAuth darMetadata testId -> runWithSandbox sandbox maybeAuth testId (f darMetadata testId)

tListPackages :: SandboxTest
tListPackages withSandbox = testCase "listPackages" $ run withSandbox $ \DarMetadata{mainPackageId,manifest} _testId -> do
    pids <- listPackages
    liftIO $ do
        -- Canton loads the `AdminWorkflows` package at boot, hence the + 1
        assertEqual "#packages" (length (dalfPaths manifest) + 1) (length pids)
        assertBool "The pid is listed" (mainPackageId `elem` pids)

tGetPackage :: SandboxTest
tGetPackage withSandbox = testCase "getPackage" $ run withSandbox $ \DarMetadata{mainPackageId} _testId -> do
    Just (Package bs) <- getPackage mainPackageId
    liftIO $ assertBool "contents" ("currency" `isInfixOf` show bs)

tGetPackageBad :: SandboxTest
tGetPackageBad withSandbox = testCase "getPackage/bad" $ run withSandbox $ \_darMetadata _testId -> do
    let pid = PackageId "xxxxxxxxxxxxxxxxxxxxxx"
    Nothing <- getPackage pid
    return ()

tGetPackageStatusRegistered :: SandboxTest
tGetPackageStatusRegistered withSandbox = testCase "getPackageStatus/Registered" $ run withSandbox $ \DarMetadata{mainPackageId} _testId -> do
    status <- getPackageStatus mainPackageId
    liftIO $ assertBool "status" (status == PackageStatusREGISTERED)

tGetPackageStatusUnknown :: SandboxTest
tGetPackageStatusUnknown withSandbox = testCase "getPackageStatus/Unknown" $ run withSandbox $ \_darMetadata _testId -> do
    let pid = PackageId "xxxxxxxxxxxxxxxxxxxxxx"
    status <- getPackageStatus pid
    liftIO $ assertBool "status" (status == PackageStatusUNKNOWN)

tUploadDarFileBad :: SandboxTest
tUploadDarFileBad withSandbox = testCase "tUploadDarFileBad" $ run withSandbox $ \_darMetadata _testId -> do
    let bytes = BS.fromString "not-the-bytes-for-a-darfile"
    Left err <- uploadDarFileGetPid bytes
    liftIO $ assertTextContains err "Dar file is corrupt"

-- This test flakes with a `NO_DOMAIN_FOR_SUBMISSION`
-- The error class in scala includes a reasons for each domain, but that isn't forwarded here, and is difficult to obtain
-- I'd assume the domain is somehow still busy from the upload in some way?
tUploadDarFileGood :: SandboxTest
tUploadDarFileGood withSandbox = testCase "tUploadDarFileGood" $ run withSandbox $ \_darMetadata _testId -> do
    bytes <- liftIO getBytesForUpload
    before <- listKnownPackages
    pid <- uploadDarFileGetPid bytes >>= either (liftIO . assertFailure) return
    after <- listKnownPackages
    let getPid PackageDetails{pid} = pid
    liftIO $ assertEqual "new pids"
        (Set.fromList (map getPid after) Set.\\ Set.fromList (map getPid before))
        (Set.singleton pid)

getBytesForUpload :: IO BS.ByteString
getBytesForUpload = do
        let extraDarFilename = "language-support/hs/bindings/for-upload.dar"
        file <- locateRunfiles (mainWorkspace </> extraDarFilename)
        BS.readFile file

-- Would be nice if the underlying service returned the pid on successful upload.
uploadDarFileGetPid :: BS.ByteString -> LedgerService (Either String PackageId)
uploadDarFileGetPid bytes = do
    before <- listPackages
    uploadDarFile bytes >>= \case -- call the actual service
        Left m -> return $ Left m
        Right () -> do
            after <- listPackages
            [newPid] <- return (after \\ before) -- see what new pid appears
            return $ Right newPid

tGetParticipantId :: SandboxTest
tGetParticipantId withSandbox = testCase "tGetParticipantId" $ run withSandbox $ \_darMetadata _testId -> do
    id <- getParticipantId
    liftIO $ assertEqual "participant" "my-ledger-id" $ dropHashSuffix $ unParticipantId id

tAllocateParty :: SandboxTest
tAllocateParty withSandbox = testCase "tAllocateParty" $ run withSandbox $ \_darMetadata (TestId testId) -> do
    let party = Party (TL.pack $ "me" <> show testId)
    before <- map dropPartyDetailHash <$> listKnownParties
    let displayName = "Only Me"
    let request = AllocatePartyRequest { partyIdHint = unParty party, displayName }
    deats <- dropPartyDetailHash <$> allocateParty request
    let expected = PartyDetails { party, displayName, isLocal = True }
    liftIO $ assertEqual "deats" expected deats
    after <- map dropPartyDetailHash <$> listKnownParties
    liftIO $ assertEqual "new parties"
        (Set.fromList after Set.\\ Set.fromList before)
        (Set.singleton expected)

tMeteringReportJson :: TestTree
tMeteringReportJson = testCase "tMeteringReportJson" $ do
    let sample = "{ \"s\": \"abc\", \"b\": true, \"n\": null, \"d\": 2.3, \"a\": [1,2,3], \"o\": { \"x\": 1, \"y\": 2 } }"
    let expected = decode sample :: Maybe Aeson.Value
    let struct = fmap toRawStructValue expected
    let actual = fmap toRawAesonValue struct
    assertEqual "MeteringReport Serialization" expected actual

expectObject :: Aeson.Value -> Either String (KeyMap Aeson.Value)
expectObject (Aeson.Object keyMap) = Right keyMap
expectObject other = Left $ "Expected object not " <> show other

expectString :: Aeson.Value -> Either String String
expectString (Aeson.String string) = Right (unpack string)
expectString other = Left $ "Expected string not " <> show other

expectField :: String -> KeyMap Aeson.Value -> Either String Aeson.Value
expectField key keyMap = maybeToEither ("Did not find " <> key <> " in " <> show (KeyMap.keys keyMap)) (KeyMap.lookup (fromString key) keyMap)

tMeteringReport :: SandboxTest
tMeteringReport withSandbox = testCase "tMeteringReport" $ run withSandbox $ \_ _testId -> do
    let timestamp = Timestamp {seconds = 3600, nanos = 0}  -- Must be rounded to hour
    let expected = timestampToIso8601 timestamp
    reportValue <- getMeteringReport timestamp Nothing Nothing
    let actual = do
        report <- expectObject reportValue
        requestValue <- expectField "request" report
        request <- expectObject requestValue
        fromValue <- expectField "from" request
        expectString fromValue

    liftIO $ assertEqual "report from date" (Right expected) actual

----------------------------------------------------------------------
-- misc ledger ops/commands

newtype TestId = TestId Int
  deriving Show

newtype Secret = Secret { getSecret :: String }

nextTestId :: TestId -> TestId
nextTestId (TestId i) = TestId (i + 1)

alice,bob :: TestId -> Party
alice (TestId i) = Party $ TL.pack $ "Alice" <> show i
bob (TestId i) = Party $ TL.pack $ "Bob" <> show i

----------------------------------------------------------------------
-- runWithSandbox

runWithSandbox :: forall a. Port -> Maybe Secret -> TestId -> LedgerService a -> IO a
runWithSandbox port mbSecret tid ls = runLedgerService ls' timeout (configOfPort port)
    where timeout = 60 :: TimeoutSeconds
          ls' :: LedgerService a
          ls' = case mbSecret of
            Nothing -> ls
            Just secret -> do
              let tok = Ledger.Token ("Bearer " <> makeSignedJwt' secret tid)
              setToken tok ls

makeSignedJwt' :: Secret -> TestId -> String
makeSignedJwt' secret tid =
    makeSignedJwt (getSecret secret) [TL.unpack $ unParty $ p tid | p <- [alice, bob]]

----------------------------------------------------------------------
-- misc expectation combinators

assertTextContains :: String -> String -> IO ()
assertTextContains text frag =
    unless (frag `isInfixOf` text) (assertFailure msg)
    where msg = "expected frag: " ++ frag ++ "\n contained in: " ++ text

darPath :: FilePath
darPath = mainWorkspace </> "language-support/hs/bindings/for-tests.dar"

testGroupWithSandbox :: FilePath -> Maybe Secret -> TestName -> [WithSandbox -> TestTree] -> TestTree
testGroupWithSandbox testDar mbSecret name tests =
    withCantonSandbox defaultSandboxConf { dars = [ testDar ], timeMode = Static, mbLedgerId = Just "my-ledger-id" } $ \getSandboxPort ->
    withResource (readDarMetadata testDar) (const $ pure ()) $ \getDarMetadata ->
    withResource (newIORef $ TestId 0) (const $ pure ()) $ \getTestCounter ->
    let run :: WithSandbox
        run f = do
            port <- getSandboxPort
            darMetadata <- getDarMetadata
            testCounter <- getTestCounter
            testId <- atomicModifyIORef testCounter (\x -> (nextTestId x, x))
            f (Port port) mbSecret darMetadata testId
    in testGroup name $ map (\f -> f run) tests

readDarMetadata :: FilePath -> IO DarMetadata
readDarMetadata dar = do
    archive <- Zip.toArchive <$> BSL.readFile dar
    Dalfs { mainDalf } <- either fail pure $ readDalfs archive
    manifest <- either fail pure $ readDalfManifest archive
    LF.PackageId pId <- either (fail . show) pure $ decodeArchivePackageId (BSL.toStrict mainDalf)
    pure $ DarMetadata (PackageId $ TL.fromStrict pId) manifest

data DarMetadata = DarMetadata
  { mainPackageId :: PackageId
  , manifest :: DalfManifest
  }

type WithSandbox = (Port -> Maybe Secret -> DarMetadata -> TestId -> IO ()) -> IO ()
