{-# OPTIONS -Wwarn #-}
module Main where

import Data.Bifunctor
import qualified Data.ByteString as BS
import qualified Data.NameMap as NM
import qualified Data.Text as T
import System.Environment
import qualified "zip" Codec.Archive.Zip as Zip

import DA.Daml.LF.Ast
import DA.Daml.LF.Proto3.Archive
import DA.Daml.LFConversion.UtilLF
import DA.Daml.Compiler.Dar
import DA.Daml.Package.Config

main :: IO ()
main = do
    [file] <- getArgs
    let (bs, hash) = encodeArchiveAndHash $
          Package
            versionDev
            mod
            (Just PackageMetadata
              { packageName = PackageName "verapplied"
              , packageVersion = PackageVersion "0.0.1"
              }
            )
    Zip.createArchive file $ createArchive
        (PackageName "overapplied")
        (Just $ PackageVersion "0.0.1")
        (PackageSdkVersion "0.0.0")
        hash
        bs
        []
        (error "unused")
        []
        []
        []
  where
    mod = NM.singleton Module
      { moduleName = ModuleName ["Overapplied"]
      , moduleSource = Nothing
      , moduleFeatureFlags = daml12FeatureFlags
      , moduleTemplates = NM.empty
      , moduleSynonyms = NM.empty
      , moduleDataTypes = NM.empty
      , moduleValues = NM.singleton DefValue
          { dvalLocation = Nothing
          , dvalBinder = (ExprValName "overapplied", TScenario TUnit)
          , dvalNoPartyLiterals = HasNoPartyLiterals True
          , dvalIsTest = IsTest True
          , dvalBody =
            ETmApp
              (ETmApp (ETyApp (EBuiltin BEError) (TUnit :-> TScenario TUnit))
                      (EBuiltin $ BEText "abc"))
              (ETmApp (ETyApp (EBuiltin BEError) TUnit)
                      (EBuiltin $ BEText "def"))
          }
      }
