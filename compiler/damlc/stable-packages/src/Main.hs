-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main
    ( main
    ) where

import DA.Daml.LF.Ast
import DA.Daml.LF.Proto3.Archive.Encode
import DA.Daml.StablePackages
import Data.ByteString qualified as BS
import Data.Map.Strict qualified as MS
import Data.Text qualified as T
import Data.Text.Extended (writeFileUtf8)
import Options.Applicative

data Opts
    = PackageListCmd GenPackageListOpts
    | PackageCmd GenPackageOpts

data GenPackageListOpts = GenPackageListOpts
  { optListOutputPath :: FilePath
  }

data GenPackageOpts = GenPackageOpts
  { optMajorVersion :: MajorVersion
  -- ^ The major version of LF for which to generate a package
  , optModule :: ModuleName
  -- ^ The module that we generate as a standalone package
  , optModuleDeps :: [ModuleDep]
  -- ^ Dependencies of this module, i.e., modules that we reference.
  -- We don’t want to hardcode package ids in here
  -- (even though we could since they must be stable)
  -- so we require that users pass in a mapping from module names
  -- to package ids.
  , optOutputPath :: FilePath
  } deriving Show

data ModuleDep = ModuleDep
  { depModuleName :: ModuleName
  , depPackageId :: PackageId
  } deriving Show

packageListOptsParser :: Parser GenPackageListOpts
packageListOptsParser =
    subparser $
    command "gen-package-list" $
    info parser mempty
  where
    parser = GenPackageListOpts <$> option str (short 'o')


packageOptsParser :: Parser GenPackageOpts
packageOptsParser =
  GenPackageOpts
    <$> option majorVersionReader (long "major-version")
    <*> option modNameReader (long "module")
    <*> many (option modDepReader (long "module-dep" <> help "Module.Name:packageid"))
    <*> option str (short 'o')
  where
    majorVersionReader = maybeReader parseMajorVersion
    modNameReader = maybeReader (Just . ModuleName . T.splitOn "." . T.pack)
    modDepReader = maybeReader $ \s ->
      case T.splitOn ":" (T.pack s) of
        [modName, packageId] -> Just ModuleDep
          { depModuleName = ModuleName (T.splitOn "." modName)
          , depPackageId = PackageId packageId
          }
        _ -> Nothing

optParser :: Parser Opts
optParser =
    PackageListCmd <$> packageListOptsParser <|> PackageCmd <$> packageOptsParser

main :: IO ()
main = do
    opts <- execParser (info optParser idm)
    case opts of
        PackageCmd GenPackageOpts{..} ->
            case (optMajorVersion, optModule) `MS.lookup` stablePackageByModuleName of
                Nothing ->
                    fail $ "Unknown module: " <> show optModule
                Just (_, pkg) ->
                    writePackage pkg optOutputPath
        PackageListCmd GenPackageListOpts{..} ->
            writeFileUtf8 optListOutputPath $ T.unlines
              [ "module DA.Daml.StablePackagesList (stablePackages) where"
              , "import DA.Daml.LF.Ast (PackageId(..))"
              , "import qualified Data.Set as Set"
              , "stablePackages :: Set.Set PackageId"
              , "stablePackages = Set.fromList"
              , "  [" <> T.intercalate ", " (map (T.pack . show) $ MS.keys allStablePackages) <> "]"
              ]

writePackage :: Package -> FilePath -> IO ()
writePackage pkg path = do
  BS.writeFile path $ encodeArchive pkg
