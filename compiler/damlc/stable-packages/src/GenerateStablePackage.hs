-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module GenerateStablePackage (main) where

import qualified Data.ByteString as BS
import qualified Data.NameMap as NM
import Options.Applicative
import qualified Data.Text as T

import DA.Daml.LF.Ast
import DA.Daml.LF.Proto3.Archive
import DA.Daml.LFConversion.UtilLF

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
  case optModule of
    ModuleName ["GHC", "Types"] ->
      writePackage ghcTypes optOutputPath
    ModuleName ["GHC", "Prim"] ->
      writePackage ghcPrim optOutputPath
    _ -> fail $ "Unknown module: " <> show optModule

lfVersion :: Version
lfVersion = version1_6

writePackage :: Module -> FilePath -> IO ()
writePackage mod path = do
  let pkg = Package lfVersion (NM.fromList [mod])
  BS.writeFile path $ encodeArchive pkg

ghcTypes :: Module
ghcTypes = Module
  { moduleName = modName
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleDataTypes = NM.fromList [dataOrdering]
  , moduleValues = NM.empty
  , moduleTemplates = NM.empty
  }
  where
    modName = mkModName ["GHC", "Types"]
    cons = ["LT", "EQ", "GT"]
    dataOrdering = DefDataType
      { dataLocation= Nothing
      , dataTypeCon = mkTypeCon ["Ordering"]
      , dataSerializable = IsSerializable True
      , dataParams = []
      , dataCons = DataEnum $ map mkVariantCon cons
      }

ghcPrim :: Module
ghcPrim = Module
  { moduleName = modName
  , moduleSource = Nothing
  , moduleFeatureFlags = daml12FeatureFlags
  , moduleDataTypes = NM.fromList [dataVoid]
  , moduleValues = NM.fromList [valVoid]
  , moduleTemplates = NM.empty
  }
  where
    modName = mkModName ["GHC", "Prim"]
    qual = Qualified PRSelf modName
    conName = mkVariantCon "Void#"
    dataVoid = DefDataType
      { dataLocation= Nothing
      , dataTypeCon = mkTypeCon ["Void#"]
      , dataSerializable = IsSerializable False
      , dataParams = []
      , dataCons = DataEnum [conName]
      }
    valVoid = DefValue
      { dvalLocation = Nothing
      , dvalBinder = (mkVal "void#", TCon (qual (dataTypeCon dataVoid)))
      , dvalNoPartyLiterals= HasNoPartyLiterals True
      , dvalIsTest = IsTest False
      , dvalBody = EEnumCon (qual (dataTypeCon dataVoid)) conName
      }

