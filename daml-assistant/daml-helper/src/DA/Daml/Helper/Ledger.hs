-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}

module DA.Daml.Helper.Ledger (
    LedgerFlags(..),
    LedgerApi(..),
    L.ClientSSLConfig(..),
    L.ClientSSLKeyCertPair(..),
    L.TimeoutSeconds,
    JsonFlag(..),
    runDeploy,
    runLedgerListParties,
    runLedgerAllocateParties,
    runLedgerUploadDar,
    runLedgerFetchDar,
    runLedgerNavigator,
    ) where

import Control.Exception.Safe (Exception,SomeException,catch,throw)
import Control.Monad.Extra hiding (fromMaybeM)
import DA.Ledger (LedgerService,Party(..),PartyDetails(..),Token)
import qualified DA.Ledger as L
import Data.Aeson ((.=))
import qualified Data.Aeson as A
import Data.Aeson.Text
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import Data.List.Extra as List
import Data.String (IsString)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TL
import GHC.Generics
import Network.HTTP.Simple
import System.Environment
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process.Typed

import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Compiler.Fetch (LedgerArgs(..),runWithLedgerArgs,fetchDar)

import DA.Daml.Helper.Util
import DA.Daml.Project.Util (fromMaybeM)

data LedgerApi
  = Grpc
  | HttpJson
  deriving (Show, Eq)

data LedgerFlags = LedgerFlags
  { api :: LedgerApi
  , hostM :: Maybe String
  , portM :: Maybe Int
  , tokFileM :: Maybe FilePath
  , sslConfigM :: Maybe L.ClientSSLConfig
  , timeout :: L.TimeoutSeconds
  }

getTokFromFile :: Maybe FilePath -> IO (Maybe Token)
getTokFromFile tokFileM = do
  case tokFileM of
    Nothing -> return Nothing
    Just tokFile -> do
      -- This postprocessing step which allows trailing newlines
      -- matches the behavior of the Scala token reader in
      -- com.daml.auth.TokenHolder.
      tok <- intercalate "\n" . lines <$> readFileUTF8 tokFile
      return (Just (L.Token tok))

getHostAndPortDefaults :: LedgerFlags -> IO LedgerArgs
getHostAndPortDefaults LedgerFlags{hostM,portM,tokFileM,sslConfigM,timeout} = do
    host <- fromMaybeM getProjectLedgerHost hostM
    port <- fromMaybeM getProjectLedgerPort portM
    tokM <- getTokFromFile tokFileM
    return LedgerArgs {..}

-- | Allocate project parties and upload project DAR file to ledger.
runDeploy :: LedgerFlags -> IO ()
runDeploy flags = do
    hp <- getHostAndPortDefaults flags
    putStrLn $ "Deploying to " <> show hp
    let flags' = flags
            { hostM = Just (host hp)
            , portM = Just (port hp) }
    runLedgerAllocateParties flags' []
    runLedgerUploadDar flags' Nothing
    putStrLn "Deploy succeeded."

newtype JsonFlag = JsonFlag { unJsonFlag :: Bool }

-- | Fetch list of parties from ledger.
runLedgerListParties :: LedgerFlags -> JsonFlag -> IO ()
runLedgerListParties flags (JsonFlag json) = do
    hp <- getHostAndPortDefaults flags
    unless json . putStrLn $ "Listing parties at " <> show hp
    xs <- listParties (api flags) hp
    if json then do
        TL.putStrLn . encodeToLazyText . A.toJSON $
            [ A.object
                [ "party" .= TL.toStrict (unParty party)
                , "display_name" .= TL.toStrict displayName
                , "is_local" .= isLocal
                ]
            | PartyDetails {..} <- xs
            ]
    else if null xs then
        putStrLn "no parties are known"
    else
        mapM_ print xs

-- | Allocate parties on ledger. If list of parties is empty,
-- defaults to the project parties.
runLedgerAllocateParties :: LedgerFlags -> [String] -> IO ()
runLedgerAllocateParties flags partiesArg = do
    parties <- if notNull partiesArg
        then pure partiesArg
        else getProjectParties
    hp <- getHostAndPortDefaults flags
    putStrLn $ "Checking party allocation at " <> show hp
    mapM_ (allocatePartyIfRequired hp) parties

-- | Allocate a party if it doesn't already exist (by display name).
allocatePartyIfRequired :: LedgerArgs -> String -> IO ()
allocatePartyIfRequired hp name = do
    partyM <- lookupParty hp name
    party <- flip fromMaybeM partyM $ do
        putStrLn $ "Allocating party for '" <> name <> "' at " <> show hp
        allocateParty hp name
    putStrLn $ "Allocated " <> show party <> " for '" <> name <> "' at " <> show hp

-- | Give better help to the user if the upload-dar commands fails
data ExceptionDuringUploadDar = ExceptionDuringUploadDar SomeException
instance Exception ExceptionDuringUploadDar

instance Show ExceptionDuringUploadDar where
  show (ExceptionDuringUploadDar e) =
    unlines [ "An exception was thrown when during the upload-dar command"
            , "- " <> show e
            , "One reason for this to occur is if the size of DAR file being uploaded exceeds the gRPC maximum message size. The default value for this is 4Mb, but it may be increased when the ledger is (re)started. Please check with your ledger operator."
            ]
-- | Upload a DAR file to the ledger. (Defaults to project DAR)
runLedgerUploadDar :: LedgerFlags -> Maybe FilePath -> IO ()
runLedgerUploadDar flags darPathM = do
    hp <- getHostAndPortDefaults flags
    darPath <- flip fromMaybeM darPathM $ do
        doBuild
        getDarPath
    putStrLn $ "Uploading " <> darPath <> " to " <> show hp
    bytes <- BS.readFile darPath
    uploadDarFile hp bytes `catch` \e -> throw (ExceptionDuringUploadDar e)
    putStrLn "DAR upload succeeded."

-- | Fetch the packages reachable from a main package-id, and reconstruct a DAR file.
runLedgerFetchDar :: LedgerFlags -> String -> FilePath -> IO ()
runLedgerFetchDar flags pidString saveAs = do
    let pid = LF.PackageId $ T.pack pidString
    hp <- getHostAndPortDefaults flags
    putStrLn $ "Fetching " <> show (LF.unPackageId pid) <> " from " <> show hp <> " into " <> saveAs
    n <- fetchDar hp pid saveAs
    putStrLn $ "DAR fetch succeeded; contains " <> show n <> " packages."

run :: LedgerArgs -> LedgerService a -> IO a
run = runWithLedgerArgs

listParties :: LedgerApi -> LedgerArgs -> IO [PartyDetails]
listParties api la =
  case api of
    Grpc -> run la L.listKnownParties
    HttpJson -> listPartiesJson la

lookupParty :: LedgerArgs -> String -> IO (Maybe Party)
lookupParty hp name = do
    xs <- listParties Grpc hp
    let text = TL.pack name
    let pred PartyDetails{displayName,party} = if text == displayName then Just party else Nothing
    return $ List.firstJust pred xs

allocateParty :: LedgerArgs -> String -> IO Party
allocateParty hp name = run hp $ do
    let text = TL.pack name
    let request = L.AllocatePartyRequest
            { partyIdHint = text
            , displayName = text }
    PartyDetails{party} <- L.allocateParty request
    return party

uploadDarFile :: LedgerArgs -> BS.ByteString -> IO ()
uploadDarFile hp bytes = run hp $ do
    L.uploadDarFile bytes >>= either fail return

-- | Run navigator against configured ledger. We supply Navigator with
-- the list of parties from the ledger, but in the future Navigator
-- should fetch the list of parties itself.
runLedgerNavigator :: LedgerFlags -> [String] -> IO ()
runLedgerNavigator flags remainingArguments = do
    logbackArg <- getLogbackArg (damlSdkJarFolder </> "navigator-logback.xml")
    hostAndPort <- getHostAndPortDefaults flags
    putStrLn $ "Opening navigator at " <> show hostAndPort
    partyDetails <- listParties Grpc hostAndPort

    withTempDir $ \confDir -> do
        let navigatorConfPath = confDir </> "ui-backend.conf"
            navigatorArgs = concat
                [ ["server"]
                , [host hostAndPort, show (port hostAndPort)]
                , remainingArguments
                ]

        writeFileUTF8 navigatorConfPath (T.unpack $ navigatorConfig partyDetails)
        unsetEnv "DAML_PROJECT" -- necessary to prevent config contamination
        withJar damlSdkJar [logbackArg] ("navigator" : navigatorArgs ++ ["-c", confDir </> "ui-backend.conf"]) $ \ph -> do
            exitCode <- waitExitCode ph
            exitWith exitCode

  where
    navigatorConfig :: [PartyDetails] -> T.Text
    navigatorConfig partyDetails = T.unlines . concat $
        [ ["users", "  {"]
        , [ T.concat
            [ "    "
            , T.pack . show $
                if TL.null displayName
                    then unParty party
                    else displayName
            , " { party = "
            , T.pack (show (unParty party))
            , " }"
            ]
          | PartyDetails{..} <- partyDetails
          ]
        , ["  }"]
        ]

----------------
-- HTTP JSON API
----------------

newtype Method = Method
  { unMethod :: BS.ByteString
  } deriving IsString

newtype Path = Path
  { unPath :: BS.ByteString
  } deriving IsString

-- | Run a request against the HTTP JSON API.
httpJsonRequest :: A.FromJSON a => LedgerArgs -> Method -> Path -> IO (Response a)
httpJsonRequest LedgerArgs {..} method path = do
  resp <-
    httpJSON $
    setRequestPort port $
    setRequestHost (BSC.pack host) $
    setRequestMethod (unMethod method) $
    setRequestPath (unPath path) $
    setRequestHeader
      "authorization"
      [BSC.pack $ sanitizeToken tok | Just (L.Token tok) <- [tokM]]
      defaultRequest
  let status = getResponseStatusCode resp
  unless (status == 200) $
    fail $ "Request failed with error code " <> show status
  pure resp

-- This matches how the com.daml.ledger.api.auth.client.LedgerCallCredentials
-- behaves.
sanitizeToken :: String -> String
sanitizeToken tok
  | "Bearer " `isPrefixOf` tok = tok
  | otherwise = "Bearer " <> tok

listPartiesJson :: LedgerArgs -> IO [PartyDetails]
listPartiesJson args = do
  body <- getResponseBody <$> httpJsonRequest args "GET" "/v1/parties"
  pure $ result body

data HttpJsonResponseBody = HttpJsonResponseBody
  { status :: Int
  , result :: [PartyDetails]
  } deriving (Generic)

instance A.FromJSON HttpJsonResponseBody
