-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module GenerateStablePackage
    ( main
    ) where

import qualified Data.ByteString as BS
import qualified Data.Map.Strict as MS
import Options.Applicative
import qualified Data.Text as T

import DA.Daml.LF.Ast
import DA.Daml.LF.Proto3.Archive
import DA.Daml.StablePackages

data Opts = Opts
  { optModule :: ModuleName
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

optParser :: Parser Opts
optParser =
  Opts
    <$> option modNameReader (long "module")
    <*> many (option modDepReader (long "module-dep" <> help "Module.Name:packageid"))
    <*> option str (short 'o')
  where
    modNameReader = maybeReader (Just . ModuleName . T.splitOn "." . T.pack)
    modDepReader = maybeReader $ \s ->
      case T.splitOn ":" (T.pack s) of
        [modName, packageId] -> Just ModuleDep
          { depModuleName = ModuleName (T.splitOn "." modName)
          , depPackageId = PackageId packageId
          }
        _ -> Nothing

main :: IO ()
main = do
    Opts{..} <- execParser (info optParser idm)
    case MS.lookup optModule stablePackageByModuleName of
        Nothing ->
            fail $ "Unknown module: " <> show optModule
        Just pkg ->
            writePackage pkg optOutputPath

writePackage :: Package -> FilePath -> IO ()
writePackage pkg path = do
  BS.writeFile path $ encodeArchive pkg
