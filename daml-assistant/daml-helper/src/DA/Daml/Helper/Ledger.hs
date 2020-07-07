-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Helper.Ledger (
    LedgerFlags(..),
    L.ClientSSLConfig(..),
    L.ClientSSLKeyCertPair(..),
    JsonFlag(..),
    runDeploy,
    runLedgerListParties,
    runLedgerAllocateParties,
    runLedgerUploadDar,
    runLedgerFetchDar,
    runLedgerNavigator
    ) where

import Control.Monad.Extra hiding (fromMaybeM)
import DA.Ledger (LedgerService,PartyDetails(..),Party(..),Token)
import Data.Aeson
import Data.Aeson.Text
import Data.List.Extra as List
import qualified DA.Ledger as L
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TL
import System.Environment
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process.Typed

import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Compiler.Fetch (LedgerArgs(..),runWithLedgerArgs, fetchDar)

import DA.Daml.Helper.Util
import DA.Daml.Project.Util (fromMaybeM)

data LedgerFlags = LedgerFlags
  { hostM :: Maybe String
  , portM :: Maybe Int
  , tokFileM :: Maybe FilePath
  , sslConfigM :: Maybe L.ClientSSLConfig
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
getHostAndPortDefaults LedgerFlags{hostM,portM,tokFileM,sslConfigM} = do
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
    xs <- listParties hp
    if json then do
        TL.putStrLn . encodeToLazyText . toJSON $
            [ object
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

-- | Upload a DAR file to the ledger. (Defaults to project DAR)
runLedgerUploadDar :: LedgerFlags -> Maybe FilePath -> IO ()
runLedgerUploadDar flags darPathM = do
    hp <- getHostAndPortDefaults flags
    darPath <- flip fromMaybeM darPathM $ do
        doBuild
        getDarPath
    putStrLn $ "Uploading " <> darPath <> " to " <> show hp
    bytes <- BS.readFile darPath
    uploadDarFile hp bytes
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

listParties :: LedgerArgs -> IO [PartyDetails]
listParties hp = run hp L.listKnownParties

lookupParty :: LedgerArgs -> String -> IO (Maybe Party)
lookupParty hp name = do
    xs <- listParties hp
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
    partyDetails <- listParties hostAndPort

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
