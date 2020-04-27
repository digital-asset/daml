-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.Fetch (
  LedgerArgs(..), runWithLedgerArgs,
  createDarFile,
  fetchDar
  ) where

import Control.Lens (toListOf)
import Data.List.Extra (nubSort)
import Data.String (fromString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL

import DA.Daml.Package.Config (PackageSdkVersion(..))
import DA.Daml.Compiler.Dar (createDarFile,createArchive)
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Ast.Optics as LF (packageRefs)
import qualified DA.Daml.LF.Proto3.Archive as LFArchive
import qualified DA.Ledger as L
import qualified SdkVersion

data LedgerArgs = LedgerArgs
  { host :: String
  , port :: Int
  , tokM :: Maybe L.Token
  , sslConfigM :: Maybe L.ClientSSLConfig
  }

instance Show LedgerArgs where
  show LedgerArgs{host,port} = host <> ":" <> show port

-- | Reconstruct a DAR file by downloading packages from a ledger. Returns how many packages fetched.
fetchDar :: LedgerArgs -> LF.PackageId -> FilePath -> IO Int
fetchDar ledgerArgs rootPid saveAs = do
  xs <- downloadAllReachablePackages ledgerArgs rootPid
  [pkg] <- pure [ pkg | (pid,pkg) <- xs, pid == rootPid ]
  let (dalf,pkgId) = LFArchive.encodeArchiveAndHash pkg
  let dalfDependencies :: [(T.Text,BS.ByteString,LF.PackageId)] =
        [ (txt,bs,pkgId)
        | (pid,pkg) <- xs, pid /= rootPid
        , let txt = recoverPackageName pkg ("dep",pid)
        , let (bsl,pkgId) = LFArchive.encodeArchiveAndHash pkg
        , let bs = BSL.toStrict bsl
        ]
  let (pName,pVersion) = do
        let LF.Package {packageMetadata} = pkg
        case packageMetadata of
          Nothing -> (LF.PackageName $ T.pack "reconstructed",Nothing)
          Just LF.PackageMetadata{packageName,packageVersion} -> (packageName,Just packageVersion)
  let pSdkVersion = PackageSdkVersion SdkVersion.sdkVersion
  let srcRoot = error "unexpected use of srcRoot when there are no sources"
  let za = createArchive pName pVersion pSdkVersion pkgId dalf dalfDependencies srcRoot [] [] []
  createDarFile saveAs za
  return $ length xs

recoverPackageName :: LF.Package -> (String,LF.PackageId) -> T.Text
recoverPackageName pkg (tag,pid)= do
  let LF.Package {packageMetadata} = pkg
  case packageMetadata of
    Just LF.PackageMetadata{packageName} -> LF.unPackageName packageName
    -- fallback, manufacture a name from the pid
    Nothing -> T.pack (tag <> "-" <> T.unpack (LF.unPackageId pid))


-- | Download all Packages reachable from a PackageId; fail if any don't exist or can't be decoded.
downloadAllReachablePackages :: LedgerArgs -> LF.PackageId -> IO [(LF.PackageId,LF.Package)]
downloadAllReachablePackages ledgerArgs pid = loop [] [pid]
  where
    loop :: [(LF.PackageId,LF.Package)] -> [LF.PackageId] -> IO [(LF.PackageId,LF.Package)]
    loop acc = \case
      [] -> return acc
      pid:morePids ->
        if pid `elem` [ pid | (pid,_) <- acc ]
        then loop acc morePids
        else do
          pkg <- downloadPackage ledgerArgs pid
          loop ((pid,pkg):acc) (packageRefs pkg ++ morePids)

packageRefs :: LF.Package -> [LF.PackageId]
packageRefs pkg = nubSort [ pid | LF.PRImport pid <- toListOf LF.packageRefs pkg ]

-- | Download the Package identified by a PackageId; fail if it doesn't exist or can't be decoded.
downloadPackage :: LedgerArgs -> LF.PackageId -> IO LF.Package
downloadPackage ledgerArgs pid = do
  let ls :: L.LedgerService (Maybe L.Package) = do
        lid <- L.getLedgerIdentity
        L.getPackage lid $ convPid pid
  runWithLedgerArgs ledgerArgs ls >>= \case
    Nothing -> fail $ "Unable to download package with identity: " <> show pid
    Just (L.Package bs) -> do
      let mode = LFArchive.DecodeAsMain
      case LFArchive.decodePackage mode pid bs of
        Left err -> fail $ show err
        Right pkg -> return pkg
  where
    convPid :: LF.PackageId -> L.PackageId
    convPid (LF.PackageId text) = L.PackageId $ TL.fromStrict text

runWithLedgerArgs :: LedgerArgs -> L.LedgerService a -> IO a
runWithLedgerArgs args ls = do
    let LedgerArgs{host,port,tokM} = args
    let ls' = case tokM of Nothing -> ls; Just tok -> L.setToken tok ls
    let timeout = 30 :: L.TimeoutSeconds
    let ledgerClientConfig =
            L.configOfHostAndPort
                (L.Host $ fromString host)
                (L.Port port)
                (sslConfigM args)
    L.runLedgerService ls' timeout ledgerClientConfig
