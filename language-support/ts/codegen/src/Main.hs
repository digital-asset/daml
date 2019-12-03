-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module Main (main) where

import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Daml.LF.Reader as DAR
import qualified DA.Pretty
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BSL
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified Data.Set.Lens as Set
import qualified Data.Text.Extended as T
import qualified "zip-archive" Codec.Archive.Zip as Zip

import Control.Monad.Extra
import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Optics
import Data.List
import Options.Applicative
import System.Directory
import System.FilePath

data Options = Options
    { optInputDar :: FilePath
    , optOutputDir :: FilePath
    }

optionsParser :: Parser Options
optionsParser = Options
    <$> argument str
        (  metavar "DAR-FILE"
        <> help "DAR file to generate TypeScript bindings for"
        )
    <*> strOption
        (  short 'o'
        <> metavar "DIR"
        <> help "Output directory for the generated TypeScript files"
        )

optionsParserInfo :: ParserInfo Options
optionsParserInfo = info (optionsParser <**> helper)
    (  fullDesc
    <> progDesc "Generate TypeScript bindings from a DAR"
    )

main :: IO ()
main = do
    opts@Options{..} <- execParser optionsParserInfo
    dar <- B.readFile optInputDar
    dalfs <- either fail pure $ DAR.readDalfs $ Zip.toArchive $ BSL.fromStrict dar
    forM_ (DAR.mainDalf dalfs : DAR.dalfs dalfs) $ \dalf -> do
        (pkgId, pkg) <- either (fail . show)  pure $ Archive.decodeArchive Archive.DecodeAsMain (BSL.toStrict dalf)
        daml2ts opts pkgId pkg

daml2ts :: Options -> PackageId -> Package -> IO ()
daml2ts Options{..} pkgId pkg = do
    let outputDir = optOutputDir </> T.unpack (unPackageId pkgId)
    createDirectoryIfMissing True outputDir
    T.writeFileUtf8 (outputDir </> "packageId.ts") $ T.unlines
        ["export default '" <> unPackageId pkgId <> "';"]
    forM_ (packageModules pkg) $ \mod -> do
        whenJust (genModule mod) $ \modTxt -> do
            let outputFile = outputDir </> joinPath (map T.unpack (unModuleName (moduleName mod))) <.> "ts"
            putStrLn $ "Generating " ++ outputFile
            createDirectoryIfMissing True (takeDirectory outputFile)
            T.writeFileUtf8 outputFile modTxt

dup :: a -> (a, a)
dup x = (x, x)

genModule :: Module -> Maybe T.Text
genModule mod
  | null serDefs = Nothing
  | otherwise =
    let curModName = moduleName mod
        pkgRootPath
          | lenModName == 1 = "."
          | otherwise = T.intercalate "/" (replicate (lenModName - 1) "..")
          where
            lenModName = length (unModuleName curModName)
        tpls = moduleTemplates mod
        (defSers, refs) = unzip (map (genDefDataType curModName tpls) serDefs)
        header =
            ["// Generated from " <> T.intercalate "/" (unModuleName curModName) <> ".daml"
            ,"/* eslint-disable @typescript-eslint/camelcase */"
            ,"/* eslint-disable @typescript-eslint/no-use-before-define */"
            ,"import * as daml from '@digitalasset/daml-json-types';"
            ,"import * as jtv from '@mojotech/json-type-validation';"
            ]
        imports =
            ["import * as " <> modNameStr <> " from '" <> pkgRootPath <> "/" <> pkgRefStr <> T.intercalate "/" (unModuleName modName) <> "';"
            | modRef@(pkgRef, modName) <- Set.toList ((PRSelf, curModName) `Set.delete` Set.unions refs)
            , let pkgRefStr = case pkgRef of
                    PRSelf -> ""
                    PRImport pkgId -> "../" <> unPackageId pkgId <> "/"
            , let modNameStr = genModuleRef modRef
            ]
        templateId
          | null (moduleTemplates mod) = []
          | otherwise =
            ["import packageId from '" <> pkgRootPath <> "/packageId';"
            ,"const moduleName = '" <> T.intercalate "." (unModuleName curModName) <> "';"
            ,"const templateId = (entityName: string): daml.TemplateId => ({packageId, moduleName, entityName});"
            ]
        defs = map (\(def, ser) -> def ++ ser) defSers
    in
    Just $ T.unlines $ intercalate [""] $ filter (not . null) $ header : imports : templateId : defs
  where
    serDefs = filter (getIsSerializable . dataSerializable) (NM.toList (moduleDataTypes mod))

genDefDataType :: ModuleName -> NM.NameMap Template -> DefDataType -> (([T.Text], [T.Text]), Set.Set ModuleRef)
genDefDataType curModName tpls def = case unTypeConName (dataTypeCon def) of
    [] -> error "IMPOSSIBLE: empty type constructor name"
    _:_:_ -> error "TODO(MH): multi-part type constructor names"
    [conName] -> case dataCons def of
        DataSynonym{} -> ((makeType ["unknown;"], makeSer ["jtv.unknownJson,"]), Set.empty)  -- TODO(NICK)
        DataVariant{} -> ((makeType ["unknown;"], makeSer ["jtv.unknownJson,"]), Set.empty)  -- TODO(MH): make variants type safe
        DataEnum{} -> ((makeType ["unknown;"], makeSer ["jtv.unknownJson,"]), Set.empty)  -- TODO(MH): make enum types type safe
        DataRecord fields ->
            let (fieldNames, fieldTypesLf) = unzip [(unFieldName x, t) | (x, t) <- fields]
                (fieldTypesTs, fieldSers) = unzip (map (genType curModName) fieldTypesLf)
                fieldRefs = map (Set.setOf typeModuleRef . snd) fields
                typeDesc =
                    ["{"] ++
                    ["  " <> x <> ": " <> t <> ";" | (x, t) <- zip fieldNames fieldTypesTs] ++
                    ["};"]
                serDesc =
                    ["() => jtv.object({"] ++
                    ["  " <> x <> ": " <> ser <> ".decoder()," | (x, ser) <- zip fieldNames fieldSers] ++
                    ["}),"]
            in
            case NM.lookup (dataTypeCon def) tpls of
                Nothing -> ((makeType typeDesc, makeSer serDesc), Set.unions fieldRefs)
                Just tpl ->
                    let (chcs, argRefs) = unzip
                            [((unChoiceName (chcName chc), t), argRefs)
                            | chc <- NM.toList (tplChoices tpl)
                            , let tLf = snd (chcArgBinder chc)
                            , let (t, _) = genType curModName tLf
                            , let argRefs = Set.setOf typeModuleRef tLf
                            ]
                        dict =
                            ["export const " <> conName <> ": daml.Template<" <> conName <> "> & {"] ++
                            ["  " <> x <> ": daml.Choice<" <> conName <> ", " <> t <> ">;" | (x, t) <- chcs] ++
                            ["} = {"
                            ] ++
                            ["  templateId: templateId('" <> conName <> "'),"
                            ] ++
                            map ("  " <>) (onHead ("decoder: " <>) serDesc) ++
                            concat
                            [ ["  " <> x <> ": {"
                              ,"    template: undefined as unknown as daml.Template<" <> conName <> ">,"
                              ,"    choiceName: '" <> x <> "',"
                              ,"    decoder: " <> t <> ".decoder,"
                              ,"  },"
                              ]
                            | (x, t) <- chcs
                            ] ++
                            ["};"]
                        knots =
                            [conName <> "." <> x <> ".template = " <> conName <> ";" | (x, _) <- chcs]
                        refs = Set.unions (fieldRefs ++ argRefs)
                    in
                    ((makeType typeDesc, dict ++ knots), refs)
      where
        paramNames = map (unTypeVarName . fst) (dataParams def)
        typeParams
          | null paramNames = ""
          | otherwise = "<" <> T.intercalate ", " paramNames <> ">"
        serParam paramName = paramName <> ": daml.Serializable<" <> paramName <> ">"
        serHeader
          | null paramNames = ": daml.Serializable<" <> conName <> "> ="
          | otherwise = " = " <> typeParams <> "(" <> T.intercalate ", " (map serParam paramNames) <> "): daml.Serializable<" <> conName <> typeParams <> "> =>"
        makeType = onHead (\x -> "export type " <> conName <> typeParams <> " = " <> x)
        makeSer serDesc =
            ["export const " <> conName <> serHeader <> " ({"] ++
            map ("  " <>) (onHead ("decoder: " <>) serDesc) ++
            ["});"]

genType :: ModuleName -> Type -> (T.Text, T.Text)
genType curModName = go
  where
    go = \case
        TVar v -> dup (unTypeVarName v)
        TUnit -> ("{}", "daml.Unit")
        TBool -> ("boolean", "daml.Bool")
        TInt64 -> dup "daml.Int"
        TDecimal -> dup "daml.Decimal"
        TNumeric _ -> dup "daml.Numeric"  -- TODO(MH): Figure out what to do with the scale.
        TText -> ("string", "daml.Text")
        TTimestamp -> dup "daml.Time"
        TParty -> dup "daml.Party"
        TDate -> dup "daml.Date"
        TList t ->
            let (t', ser) = go t
            in
            (t' <> "[]", "daml.List(" <> ser <> ")")
        TOptional (TOptional _) -> error "TODO(MH): nested optionals"
        TOptional t ->
            let (t', ser) = go t
            in
            ("(" <> t' <> " | null)", "daml.Optional(" <> ser <> ")")
        TTextMap t  ->
            let (t', ser) = go t
            in
            ("{ [key: string]: " <> t' <> " }", "daml.TextMap(" <> ser <> ")")
        TUpdate _ -> error "IMPOSSIBLE: Update not serializable"
        TScenario _ -> error "IMPOSSIBLE: Scenario not serializable"
        TContractId t ->
            let (t', ser) = go t
            in
            ("daml.ContractId<" <> t' <> ">", "daml.ContractId(" <> ser <> ")")
        TConApp con ts ->
            let (con', ser) = genTypeCon curModName con
                (ts', sers) = unzip (map go ts)
            in
            if null ts
                then (con', ser)
                else
                    ( con' <> "<" <> T.intercalate ", " ts' <> ">"
                    , ser <> "(" <> T.intercalate ", " sers <> ")"
                    )
        TCon _ -> error "IMPOSSIBLE: lonely type constructor"
        t@TApp{} -> error $ "IMPOSSIBLE: type application not serializable - " <> DA.Pretty.renderPretty t
        TBuiltin t -> error $ "IMPOSSIBLE: partially applied primitive type not serializable - " <> DA.Pretty.renderPretty t
        TForall{} -> error "IMPOSSIBLE: universally quantified type not serializable"
        TStruct{} -> error "IMPOSSIBLE: structural record not serializable"
        TNat{} -> error "IMPOSSIBLE: standalone type level natural not serializable"

genTypeCon :: ModuleName -> Qualified TypeConName -> (T.Text, T.Text)
genTypeCon curModName (Qualified pkgRef modName conParts) =
    case unTypeConName conParts of
        [] -> error "IMPOSSIBLE: empty type constructor name"
        _:_:_ -> error "TODO(MH): multi-part type constructor names"
        [conName]
          | modRef == (PRSelf, curModName) -> dup conName
          | otherwise -> dup (genModuleRef modRef <> "." <> conName)
          where
            modRef = (pkgRef, modName)

genModuleRef :: ModuleRef -> T.Text
genModuleRef (pkgRef, modName) = case pkgRef of
    PRSelf -> modNameStr
    PRImport pkgId -> "pkg" <> unPackageId pkgId <> "_" <> modNameStr
  where
    modNameStr = T.intercalate "_" (unModuleName modName)

onHead :: (a -> a) -> [a] -> [a]
onHead f = \case
    [] -> []
    x:xs -> f x:xs
