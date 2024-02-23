-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE RankNTypes #-}

module DA.Daml.Helper.Ledger (
    LedgerFlags(..),
    RemoteDalf(..),
    defaultLedgerFlags,
    sandboxLedgerFlags,
    LedgerArgs(..),
    defaultLedgerArgs,
    getDefaultArgs,
    L.ClientSSLConfig(..),
    L.ClientSSLKeyCertPair(..),
    L.TimeoutSeconds,
    JsonFlag(..),
    runDeploy,
    runLedgerListParties,
    runLedgerAllocateParties,
    runLedgerUploadDar,
    runLedgerUploadDar',
    runLedgerFetchDar,
    runLedgerExport,
    -- runLedgerReset,
    runLedgerGetDalfs,
    runLedgerListPackages,
    runLedgerListPackages0,
    runLedgerMeteringReport,
    -- exported for testing
    downloadAllReachablePackages,
    ) where

import Control.Exception (SomeException(..), catch)
import Control.Applicative ((<|>))
import Control.Lens (toListOf)
import Control.Monad.Extra hiding (fromMaybeM)
-- import Control.Monad.IO.Class (liftIO)
import Data.Aeson ((.=), encode)
import qualified Data.Aeson as A
import Data.Aeson.Text
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import Data.List.Extra
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import Data.Maybe
import Data.String (fromString)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TL
-- import qualified Data.UUID as UUID
-- import qualified Data.UUID.V4 as UUID
import Network.GRPC.Unsafe.ChannelArgs (Arg(..))
import Numeric.Natural
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process.Typed

-- import Com.Daml.Ledger.Api.V1.TransactionFilter
import DA.Daml.Compiler.Dar (createArchive, createDarFile)
import DA.Daml.Helper.Util
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Ast.Optics as LF (packageRefs)
import qualified DA.Daml.LF.Proto3.Archive as LFArchive
import DA.Daml.Project.Util (fromMaybeM)
import qualified DA.Ledger as L
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger
import DA.Ledger.Types (ApplicationId(..))
import Data.Aeson.Encode.Pretty (encodePretty)
import Data.Time.Calendar (Day(..))
import qualified Data.Aeson as Aeson

import SdkVersion.Class (SdkVersioned, unresolvedBuiltinSdkVersion)

data LedgerFlags = LedgerFlags
  { fSslConfigM :: Maybe L.ClientSSLConfig
  , fTimeout :: L.TimeoutSeconds
  -----------------------------------------
  -- The following values get defaults from the project config by
  -- running `getDefaultLedgerFlags`
  , fHostM :: Maybe String
  , fPortM :: Maybe Int
  , fTokFileM :: Maybe FilePath
  , fMaxReceiveLengthM :: Maybe Natural
  }

defaultLedgerFlags :: LedgerFlags
defaultLedgerFlags = LedgerFlags
  { fSslConfigM = Nothing
  , fTimeout = 60
  , fHostM = Nothing
  , fPortM = Nothing
  , fTokFileM = Nothing
  , fMaxReceiveLengthM = Nothing
  }

sandboxLedgerFlags :: Int -> LedgerFlags
sandboxLedgerFlags port = defaultLedgerFlags
  { fHostM = Just "localhost"
  , fPortM = Just port
  }

defaultLedgerArgs :: LedgerArgs
defaultLedgerArgs = LedgerArgs
  { sslConfigM = Nothing
  , timeout = 10
  , port = 6865
  , host = "localhost"
  , tokM = Nothing
  , grpcArgs = []
  }

data LedgerArgs = LedgerArgs
  { sslConfigM :: Maybe L.ClientSSLConfig
  , timeout :: L.TimeoutSeconds
  -----------------------------------------
  , host :: String
  , port :: Int
  , tokM :: Maybe L.Token
  , grpcArgs :: [Arg]
  }

showHostAndPort :: LedgerArgs -> String
showHostAndPort LedgerArgs{host,port} = host <> ":" <> show port

--
-- Get default values from project config
-----------------------------------------
getDefaultArgs :: LedgerFlags -> IO LedgerArgs
getDefaultArgs LedgerFlags { fSslConfigM
                           , fTimeout
                           , fHostM
                           , fPortM
                           , fTokFileM
                           , fMaxReceiveLengthM
                           } = do
  host <- fromMaybeM getProjectLedgerHost fHostM
  port <- fromMaybeM getProjectLedgerPort fPortM
  pTokFileM <- getProjectLedgerAccessToken
  tokM <- getTokFromFile (fTokFileM <|> pTokFileM)
  return $
    LedgerArgs
      { port = port
      , host = host
      , tokM = tokM
      , timeout = fTimeout
      , sslConfigM = fSslConfigM
      , grpcArgs = MaxReceiveMessageLength <$> maybeToList fMaxReceiveLengthM
      }

getTokFromFile :: Maybe FilePath -> IO (Maybe L.Token)
getTokFromFile tokFileM = do
  case tokFileM of
    Nothing -> return Nothing
    Just tokFile -> do
      -- This postprocessing step which allows trailing newlines
      -- matches the behavior of the Scala token reader in
      -- com.daml.auth.TokenHolder.
      tok <- intercalate "\n" . lines <$> readFileUTF8 tokFile
      return (Just (L.Token tok))

--
-- Ledger command implementations
---------------------------------

-- | Allocate project parties and upload project DAR file to ledger.
runDeploy :: LedgerFlags -> IO ()
runDeploy flags = do
    args <- getDefaultArgs flags
    putStrLn $ "Deploying to " <> showHostAndPort args
    runLedgerAllocateParties flags []
    runLedgerUploadDar flags Nothing
    putStrLn "Deploy succeeded."

-- | Allocate parties on ledger. If list of parties is empty,
-- defaults to the project parties.
runLedgerAllocateParties :: LedgerFlags -> [String] -> IO ()
runLedgerAllocateParties flags partiesArg = do
    args <- getDefaultArgs flags
    parties <-
      if notNull partiesArg
        then pure partiesArg
        else getProjectParties
    putStrLn $ "Checking party allocation at " <> showHostAndPort args
    mapM_ (allocatePartyIfRequired args) parties
    where
      allocatePartyIfRequired args name = do
        partyM <- lookupParty args name
        party <-
          flip fromMaybeM partyM $ do
            putStrLn $
              "Allocating party for '" <> name <> "' at " <> showHostAndPort args
            allocateParty args name
        putStrLn $
          "Allocated " <> show party <> " for '" <> name <> "' at " <>
          showHostAndPort args

-- | Upload a DAR file to the ledger. (Defaults to project DAR)
runLedgerUploadDar :: LedgerFlags -> Maybe FilePath -> IO ()
runLedgerUploadDar flags mbDar = do
  args <- getDefaultArgs flags
  runLedgerUploadDar' args mbDar

runLedgerUploadDar' :: LedgerArgs -> Maybe FilePath -> IO ()
runLedgerUploadDar' args darPathM  = do
  darPath <-
    flip fromMaybeM darPathM $ do
      doBuild
      getDarPath
  putStrLn $ "Uploading " <> darPath <> " to " <> showHostAndPort args
  bytes <- BS.readFile darPath
  result <-
    uploadDarFile args bytes `catch` \(e :: SomeException) -> do
      putStrLn $
        unlines
          [ "An exception was thrown during the upload-dar command"
          , "- " <> show e
          , "One reason for this to occur is if the size of DAR file being uploaded exceeds the gRPC maximum message size. The default value for this is 4Mb, but it may be increased when the ledger is (re)started. Please check with your ledger operator."
          ]
      exitFailure
  case result of
    Left err -> do
      putStrLn $ "upload-dar did not succeed: " <> show err
      exitFailure
    Right () -> putStrLn "DAR upload succeeded."

uploadDarFile :: LedgerArgs -> BS.ByteString -> IO (Either String ())
uploadDarFile args bytes =
  runWithLedgerArgs args $ do L.uploadDarFile bytes

newtype JsonFlag = JsonFlag { unJsonFlag :: Bool }

-- | Fetch list of parties from ledger.
runLedgerListParties :: LedgerFlags -> JsonFlag -> IO ()
runLedgerListParties flags (JsonFlag json) = do
    args <- getDefaultArgs flags
    unless json . putStrLn $ "Listing parties at " <> showHostAndPort args
    xs <- listParties args
    if json then do
        TL.putStrLn . encodeToLazyText . A.toJSON $
            [ A.object
                [ "party" .= TL.toStrict (L.unParty party)
                , "display_name" .= TL.toStrict displayName
                , "is_local" .= isLocal
                ]
            | L.PartyDetails {..} <- xs
            ]
    else if null xs then
        putStrLn "no parties are known"
    else
        mapM_ print xs

-- | Fetch the packages reachable from a main package-id, and reconstruct a DAR file.
runLedgerFetchDar :: SdkVersioned => LedgerFlags -> String -> FilePath -> IO ()
runLedgerFetchDar flags pidString saveAs = do
    args <- getDefaultArgs flags
    let pid = LF.PackageId $ T.pack pidString
    putStrLn $
      "Fetching " <> show (LF.unPackageId pid) <> " from " <> showHostAndPort args <>
      " into " <>
      saveAs
    n <- fetchDar args pid saveAs
    putStrLn $ "DAR fetch succeeded; contains " <> show n <> " packages."

-- | Reconstruct a DAR file by downloading packages from a ledger. Returns how many packages fetched.
fetchDar :: SdkVersioned => LedgerArgs -> LF.PackageId -> FilePath -> IO Int
fetchDar args rootPid saveAs = do
  loggerH <- Logger.newStderrLogger Logger.Info "fetch-dar"
  pkgs <- downloadAllReachablePackages (downloadPackage args) [rootPid] []
  let rootPkg = fromMaybe (error "damlc: fetchDar: internal error") $ pkgs Map.! rootPid
  -- It's always a `Just` because we exclude no package ids.
  let (dalf,pkgId) = LFArchive.encodeArchiveAndHash rootPkg
  let dalfDependencies :: [(T.Text,BS.ByteString,LF.PackageId)] =
        [ (txt,bs,pkgId)
        | Just pkg <- Map.elems (Map.delete rootPid pkgs)
        , let txt = recoverPackageName pkg
        , let (bsl,pkgId) = LFArchive.encodeArchiveAndHash pkg
        , let bs = BSL.toStrict bsl
        ]
  let (pName,pVersion) = do
        let LF.Package {packageMetadata} = rootPkg
        case packageMetadata of
          LF.PackageMetadata{packageName,packageVersion} -> (packageName,Just packageVersion)
  let pSdkVersion = unresolvedBuiltinSdkVersion
  let srcRoot = error "unexpected use of srcRoot when there are no sources"
  let za = createArchive pName pVersion pSdkVersion pkgId dalf dalfDependencies srcRoot [] [] []
  createDarFile loggerH saveAs za
  return $ Map.size pkgs

recoverPackageName :: LF.Package -> T.Text
recoverPackageName pkg = do
  let LF.Package{packageMetadata = LF.PackageMetadata{packageName}} = pkg
  LF.unPackageName packageName

-- | Download all Packages reachable from a PackageId; fail if any don't exist or can't be decoded.
downloadAllReachablePackages ::
       (LF.PackageId -> IO LF.Package)
    -- ^ An IO action to download a package.
    -> [LF.PackageId]
    -- ^ The roots of the dependency tree we want to download.
    -> [LF.PackageId]
    -- ^ Exclude these package ids from downloading, because they are already present.
    -> IO (Map LF.PackageId (Maybe LF.Package))
    -- ^ Return a map of dependency package ids and maybe the downloaded package or Nothing, if the
    -- package is needed but already present.
downloadAllReachablePackages downloadPkg pids exclPids =
    loop Map.empty (Set.fromList pids)
  where
    loop ::
           Map LF.PackageId (Maybe LF.Package)
        -> Set.Set LF.PackageId
        -> IO (Map LF.PackageId (Maybe LF.Package))
    loop acc pkgIds = do
        case Set.minView pkgIds of
            Nothing -> return acc
            Just (pid, morePids) ->
                if pid `Map.member` acc
                    then loop acc morePids
                    else do
                        if pid `Set.member` Set.fromList exclPids
                            then loop (Map.insert pid Nothing acc) morePids
                            else do
                                pkg <- downloadPkg pid
                                loop
                                    (Map.insert pid (Just pkg) acc)
                                    (packageRefs pkg `Set.union` morePids)
      where
        packageRefs pkg =
            Set.fromList
                [pid | LF.PRImport pid <- toListOf LF.packageRefs pkg, not $ pid `Map.member` acc]

-- | Download the Package identified by a PackageId; fail if it doesn't exist or can't be decoded.
downloadPackage :: LedgerArgs -> LF.PackageId -> IO LF.Package
downloadPackage args pid = do
  bs <-
      do
        runWithLedgerArgs args $ do
          mbPkg <- L.getPackage $ convPid pid
          case mbPkg of
            Nothing -> fail $ "Unable to download package with identity: " <> show pid
            Just (L.Package bs) -> pure bs
  let mode = LFArchive.DecodeAsMain
  case LFArchive.decodePackage mode pid bs of
    Left err -> fail $ show err
    Right pkg -> return pkg
  where
    convPid :: LF.PackageId -> L.PackageId
    convPid (LF.PackageId text) = L.PackageId $ TL.fromStrict text

data RemoteDalf = RemoteDalf
    { remoteDalfName :: LF.PackageName
    , remoteDalfVersion :: LF.PackageVersion
    , remoteDalfBs :: BS.ByteString
    , remoteDalfIsMain :: Bool
    , remoteDalfPkgId :: LF.PackageId
    }
-- | Fetch remote packages.
runLedgerGetDalfs ::
       LedgerFlags
    -> [LF.PackageId]
       -- ^ Packages to be fetched.
    -> [LF.PackageId]
       -- ^ Packages that should _not_ be fetched because they are already present.
    -> IO [RemoteDalf]
       -- ^ Returns the fetched packages.
runLedgerGetDalfs lflags pkgIds exclPkgIds
    | null pkgIds = pure []
    | otherwise = do
        args <- getDefaultArgs lflags
        m <- downloadAllReachablePackages (downloadPackage args) pkgIds exclPkgIds
        pure
            [ RemoteDalf {..}
            | (_pid, Just pkg) <- Map.toList m
            , let (bsl, pid) = LFArchive.encodeArchiveAndHash pkg
            , let LF.Package {packageMetadata} = pkg
            , let remoteDalfPkgId = pid
            , let remoteDalfName = LF.packageName packageMetadata
            , let remoteDalfBs = BSL.toStrict bsl
            , let remoteDalfIsMain = pid `Set.member` Set.fromList pkgIds
            , let remoteDalfVersion = LF.packageVersion packageMetadata
            ]

runLedgerListPackages :: LedgerFlags -> IO [LF.PackageId]
runLedgerListPackages lflags = do
    args <- getDefaultArgs lflags
    runWithLedgerArgs args $ do
            pkgIds <- L.listPackages
            pure [LF.PackageId $ TL.toStrict $ L.unPackageId pid | pid <- pkgIds]

-- | Same as runLedgerListPackages, but print output.
runLedgerListPackages0 :: LedgerFlags -> IO ()
runLedgerListPackages0 flags = do
    pkgs <- runLedgerListPackages flags
    rdalfs <- runLedgerGetDalfs flags pkgs []
    putStrLn "Available packages: "
    putStrLn $
        unlines $
        map T.unpack $
        [ LF.unPackageId remoteDalfPkgId <> " " <> suffix nameM versionM
        | RemoteDalf {..} <- rdalfs
        , nameM <- [remoteDalfName]
        , versionM <- [remoteDalfVersion]
        ]
  where
    suffix name version =
        "(" <> LF.unPackageName name <> "-" <> LF.unPackageVersion version <> ")"

listParties :: LedgerArgs -> IO [L.PartyDetails]
listParties args =
    runWithLedgerArgs args L.listKnownParties

lookupParty :: LedgerArgs -> String -> IO (Maybe L.Party)
lookupParty args name = do
    xs <- listParties args
    let text = TL.pack name
    let pred L.PartyDetails{displayName,party} = if text == displayName then Just party else Nothing
    return $ firstJust pred xs

allocateParty :: LedgerArgs -> String -> IO L.Party
allocateParty args name = do
  let text = TL.pack name
  L.PartyDetails {party} <-
      runWithLedgerArgs args $
        L.allocateParty $ L.AllocatePartyRequest {partyIdHint = text, displayName = text}
  return party

-- TODO[SW] Implementation not ported to ledger-api-v1, as it wasn't fully working in the first place, and we're moving away from
-- hs-bindings stream support. We may add this back later in a different form.
-- runLedgerReset :: LedgerFlags -> IO ()
-- runLedgerReset flags = do
--   putStrLn "Resetting ledger."
--   args <- getDefaultArgs flags
--   reset args

-- reset :: LedgerArgs -> IO ()
-- reset args = do
--     runWithLedgerArgs args $ do
--         parties <- map L.party <$> L.listKnownParties
--         unless (null parties) $ do
--           ledgerId <- L.getLedgerIdentity
--           activeContracts <-
--             L.getActiveContracts
--               ledgerId
--               (TransactionFilter $
--                Map.fromList [(L.unParty p, Just $ Filters Nothing) | p <- parties])
--               (L.Verbosity False)
--           let chunks = chunksOf 100 activeContracts
--           forM_ chunks $ \chunk -> do
--             let cmds cmdId =
--                   L.Commands
--                     { coms =
--                         [ L.ExerciseCommand
--                           { tid = tid
--                           , cid = cid
--                           , choice = L.Choice "Archive"
--                           , arg = L.VRecord $ L.Record Nothing []
--                           }
--                         | (_offset, _mbWid, events) <- chunk
--                         , L.CreatedEvent {cid, tid} <- events
--                         ]
--                     , lid = ledgerId
--                     , wid = Nothing
--                     , aid = L.ApplicationId "ledger-reset"
--                     , cid = L.CommandId $ TL.fromStrict $ UUID.toText cmdId
--                     , actAs = parties
--                     , readAs = []
--                     , dedupPeriod = Nothing
--                     , minLeTimeAbs = Nothing
--                     , minLeTimeRel = Nothing
--                     , sid = Nothing
--                     }
--             let noCommands = null [ x | (_offset, _mbWid, events) <- chunk, x <- events ]
--             unless noCommands $ do
--               cmdId <- liftIO UUID.nextRandom
--               errOrEmpty <- L.submit $ cmds cmdId
--               case errOrEmpty of
--                 Left err -> liftIO $ putStrLn $ "Failed to archive active contracts: " <> err
--                 Right () -> pure ()

-- | Run export against configured ledger.
runLedgerExport :: LedgerFlags -> [String] -> IO ()
runLedgerExport flags remainingArguments = do
    logbackArg <- getLogbackArg (damlSdkJarFolder </> "export-logback.xml")
    let isHelp = any (\x -> x `elem` ["-h", "--help"]) remainingArguments
    ledgerFlags <- if isHelp then
        -- Don't use getDefaultArgs here so that --help can be used outside a daml project.
        pure []
      else do
        args <- getDefaultArgs flags
        let maxSizeArgs size = ["--max-inbound-message-size", show size]
            extras = maybe [] maxSizeArgs $ fMaxReceiveLengthM flags
        -- TODO[AH]: Use parties from daml.yaml by default.
        -- TODO[AH]: Use SDK version from daml.yaml by default.
        pure $ ["--host", host args, "--port", show (port args)] <> extras
    withJar
      damlSdkJar
      [logbackArg]
      ("export" : remainingArguments ++ ledgerFlags) $ \ph -> do
        exitCode <- waitExitCode ph
        exitWith exitCode

--
-- Interface with the Haskell bindings
--------------------------------------

runWithLedgerArgs :: LedgerArgs -> L.LedgerService a -> IO a
runWithLedgerArgs LedgerArgs{host,port,tokM,timeout, sslConfigM, grpcArgs} ls = do
    let ls' = case tokM of Nothing -> ls; Just tok -> L.setToken tok ls
    let ledgerClientConfig =
            L.configOfHostAndPort
                (L.Host $ fromString host)
                (L.Port port)
                grpcArgs
                sslConfigM
    L.runLedgerService ls' timeout ledgerClientConfig

-- | Report on Ledger Use.
runLedgerMeteringReport :: LedgerFlags -> Day -> Maybe Day -> Maybe ApplicationId -> Bool -> IO ()
runLedgerMeteringReport flags fromIso toIso application compactOutput = do
    args <- getDefaultArgs flags
    report <- meteringReport args fromIso toIso application
    let encodeFn = if compactOutput then encode else encodePretty
    let encoded = encodeFn report
    let bsc = BSL.toStrict encoded
    let output = BSC.unpack bsc
    putStrLn output

meteringReport :: LedgerArgs -> Day -> Maybe Day -> Maybe ApplicationId -> IO Aeson.Value
meteringReport args from to application =
    runWithLedgerArgs args $
        do L.getMeteringReport (L.utcDayToTimestamp from) (fmap L.utcDayToTimestamp to) application
