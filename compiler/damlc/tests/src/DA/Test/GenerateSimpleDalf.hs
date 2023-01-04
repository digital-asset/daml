-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Test.GenerateSimpleDalf (main) where

import qualified Data.ByteString.Lazy as BSL
import qualified Data.NameMap as NM
import qualified Data.Text.IO as T
import System.Environment

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Ast.Util
import DA.Daml.LF.Ast.Version
import DA.Daml.LF.Ast.World
import DA.Daml.LF.Proto3.Archive
import DA.Daml.LF.TypeChecker

-- | This tool generates a simple DALF file and writes it to the first
-- argument given on the command line. This DALF is intended to be used
-- as a test case for the plain DALF import feature.
-- If the flag --with-archive-choice is given, The DALF will contain an "Archive" choice.
main :: IO ()
main = do
    args <- getArgs
    let (file,withArchiveChoice) =
            case args of
                [file] -> (file,False)
                ["--with-archive-choice",file] -> (file,True)
                _ -> error $ "unexpected command line args: " <> show args
    let version = versionDefault
    let modName = ModuleName ["Module"]
    let modRef = Qualified PRSelf modName
    let tplFields = map FieldName ["this", "arg"]
    let tplRec = DefDataType
            { dataLocation = Nothing
            , dataTypeCon = TypeConName ["Template"]
            , dataSerializable = IsSerializable True
            , dataParams = []
            , dataCons = DataRecord $ map (, TParty) tplFields
            }
    let tplParam = ExprVarName "arg"
    let tplParties =
            let cons f = ECons TParty (ERecProj (TypeConApp (modRef (dataTypeCon tplRec)) []) f (EVar tplParam))
            in foldr cons (ENil TParty) tplFields
    let chcArg = DefDataType
            { dataLocation = Nothing
            , dataTypeCon = TypeConName ["Choice"]
            , dataSerializable = IsSerializable True
            , dataParams = []
            , dataCons = DataVariant [(VariantConName "Choice", TUnit)]
            }
    let chcArg2 = DefDataType
            { dataLocation = Nothing
            , dataTypeCon = TypeConName ["Choice2"]
            , dataSerializable = IsSerializable True
            , dataParams = []
            , dataCons = DataRecord [ (FieldName "choiceArg", TUnit) ]
            }
    let emptyRec = DefDataType
            { dataLocation = Nothing
            , dataTypeCon = TypeConName ["EmptyRecord"]
            , dataSerializable = IsSerializable True
            , dataParams = []
            , dataCons = DataRecord []
            }
    let chc = TemplateChoice
            { chcLocation = Nothing
            , chcName = ChoiceName "NotChoice"
            , chcConsuming = True
            , chcControllers = tplParties
            , chcObservers = Nothing
            , chcSelfBinder = ExprVarName "this"
            , chcArgBinder = (ExprVarName "self", TCon (modRef (dataTypeCon chcArg)))
            , chcReturnType = TUnit
            , chcUpdate = EUpdate $ UPure TUnit EUnit
            }
    let chc2 = TemplateChoice
            { chcLocation = Nothing
            , chcName = ChoiceName "Choice2"
            , chcConsuming = True
            , chcControllers = tplParties
            , chcObservers = Nothing
            , chcSelfBinder = ExprVarName "this"
            , chcArgBinder = (ExprVarName "self", TCon (modRef (dataTypeCon chcArg2)))
            , chcReturnType = TUnit
            , chcUpdate = EUpdate $ UPure TUnit EUnit
            }
    let arc = TemplateChoice
            { chcLocation = Nothing
            , chcName = ChoiceName "Archive"
            , chcConsuming = True
            , chcControllers = tplParties
            , chcObservers = Nothing
            , chcSelfBinder = ExprVarName "this"
            , chcArgBinder = (ExprVarName "self", TCon (modRef (dataTypeCon emptyRec)))
            , chcReturnType = TUnit
            , chcUpdate = EUpdate $ UPure TUnit EUnit
            }
    let tpl = Template
            { tplLocation = Nothing
            , tplTypeCon = TypeConName ["Template"]
            , tplParam = tplParam
            , tplPrecondition = mkBool True
            , tplSignatories = tplParties
            , tplObservers = ENil TParty
            , tplAgreement = mkEmptyText
            , tplChoices = NM.fromList ([chc,chc2] <> [arc | withArchiveChoice])
            , tplKey = Nothing
            , tplImplements = NM.empty
            }
    let mod = Module
            { moduleName = ModuleName ["Module"]
            , moduleSource = Nothing
            , moduleSynonyms = NM.fromList []
            , moduleDataTypes = NM.fromList ([tplRec, chcArg, chcArg2] <> [emptyRec | withArchiveChoice])
            , moduleValues = NM.empty
            , moduleTemplates = NM.fromList [tpl]
            , moduleExceptions = NM.empty
            , moduleInterfaces = NM.empty
            , moduleFeatureFlags = FeatureFlags
            }
    case checkModule (initWorld [] version) version mod of
        [] -> pure ()
        diags -> error $ show diags
    let pkg = Package
            { packageLfVersion = version
            , packageModules = NM.fromList [mod]
            , packageMetadata = Just $ PackageMetadata (PackageName "simple-dalf") (PackageVersion "1.0.0")
            }
    let (bytes, PackageId hash) = encodeArchiveAndHash pkg
    BSL.writeFile file bytes
    T.putStrLn hash
    pure ()
