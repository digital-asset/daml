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
import qualified Data.Text.Extended as T
import qualified "zip-archive" Codec.Archive.Zip as Zip

import Control.Monad.Extra
import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Optics
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
    in
    Just $ T.unlines $
        ["// Generated from " <> T.intercalate "/" (unModuleName curModName) <> ".daml"
        ,"/* eslint-disable @typescript-eslint/camelcase */"
        ,"/* eslint-disable @typescript-eslint/no-use-before-define */"
        ,"import * as daml from '@digitalasset/daml-json-types';"
        ,"import * as jtv from '@mojotech/json-type-validation';"
        ,"import packageId from '" <> pkgRootPath <> "/packageId';"
        ] ++
        ["import * as " <> modNameStr <> " from '" <> pkgRootPath <> "/" <> pkgRefStr <> T.intercalate "/" (unModuleName modName) <> "';"
        | modRef@(pkgRef, modName) <- Set.toList (Set.unions refs)
        , let pkgRefStr = case pkgRef of
                PRSelf -> ""
                PRImport pkgId -> "../" <> unPackageId pkgId <> "/"
        , let modNameStr = genModuleRef modRef
        ] ++
        [ ""
        ,"const moduleName = '" <> T.intercalate "." (unModuleName curModName) <> "';"
        ,"const templateId = (entityName: string): daml.TemplateId => ({packageId, moduleName, entityName});"
        ] ++
        concatMap (\(def, ser) -> [""] ++ def ++ ser) defSers
  where
    serDefs = filter (getIsSerializable . dataSerializable) (NM.toList (moduleDataTypes mod))

genDefDataType :: ModuleName -> NM.NameMap Template -> DefDataType -> (([T.Text], [T.Text]), Set.Set ModuleRef)
genDefDataType curModName tpls def = case unTypeConName (dataTypeCon def) of
    [] -> error "IMPOSSIBLE: empty type constructor name"
    _:_:_ -> error "TODO(MH): multi-part type constructor names"
    [conName] -> case dataCons def of
        DataVariant{} -> ((makeType ["unknown;"], makeSer ["jtv.unknownJson,"]), Set.empty)  -- TODO(MH): make variants type safe
        DataEnum{} -> ((makeType ["unknown;"], makeSer ["jtv.unknownJson,"]), Set.empty)  -- TODO(MH): make enum types type safe
        DataRecord fields ->
            let (fieldNames, fieldTypesLf) = unzip [(unFieldName x, t) | (x, t) <- fields]
                (unzip -> (fieldTypesTs, fieldSers), fieldRefs) = unzip (map (genType curModName) fieldTypesLf)
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
                            , let ((t, _), argRefs) = genType curModName (snd (chcArgBinder chc))
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

genType :: ModuleName -> Type -> ((T.Text, T.Text), Set.Set ModuleRef)
genType curModName = go
  where
    go = \case
        TVar v -> (dup (unTypeVarName v), Set.empty)
        TUnit -> (("{}", "daml.Unit"), Set.empty)
        TBool -> (("boolean", "daml.Bool"), Set.empty)
        TInt64 -> (dup "daml.Int", Set.empty)
        TDecimal -> (dup "daml.Decimal", Set.empty)
        TNumeric _ -> (dup "daml.Numeric", Set.empty)  -- TODO(MH): Figure out what to do with the scale.
        TText -> (("string", "daml.Text"), Set.empty)
        TTimestamp -> (dup "daml.Time", Set.empty)
        TParty -> (dup "daml.Party", Set.empty)
        TDate -> (dup "daml.Date", Set.empty)
        TList t ->
            let ((t', ser), refs) = go t
            in
            ((t' <> "[]", "daml.List(" <> ser <> ")"), refs)
        TOptional (TOptional _) -> error "TODO(MH): nested optionals"
        TOptional t ->
            let ((t', ser), refs) = go t
            in
            (("(" <> t' <> " | null)", "daml.Optional(" <> ser <> ")"), refs)
        TTextMap t  ->
            let ((t', ser), refs) = go t
            in
            (("{ [key: string]: " <> t' <> " }", "daml.TextMap(" <> ser <> ")"), refs)
        TUpdate _ -> error "IMPOSSIBLE: Update not serializable"
        TScenario _ -> error "IMPOSSIBLE: Scenario not serializable"
        TContractId t ->
            let ((t', ser), refs) = go t
            in
            (("daml.ContractId<" <> t' <> ">", "daml.ContractId(" <> ser <> ")"), refs)
        TConApp con ts ->
            let ((con', ser), conRefs) = genTypeCon curModName con
                (unzip -> (ts', sers), tsRefs) = unzip (map go ts)
                refs = Set.unions (conRefs:tsRefs)
            in
            if null ts
                then ((con', ser), refs)
                else
                    ( ( con' <> "<" <> T.intercalate ", " ts' <> ">"
                      , ser <> "(" <> T.intercalate ", " sers <> ")"
                      )
                    , refs
                    )
        TCon _ -> error "IMPOSSIBLE: lonely type constructor"
        t@TApp{} -> error $ "IMPOSSIBLE: type application not serializable - " <> DA.Pretty.renderPretty t
        TBuiltin t -> error $ "IMPOSSIBLE: partially applied primitive type not serializable - " <> DA.Pretty.renderPretty t
        TForall{} -> error "IMPOSSIBLE: universally quantified type not serializable"
        TStruct{} -> error "IMPOSSIBLE: structural record not serializable"
        TNat{} -> error "IMPOSSIBLE: standalone type level natural not serializable"

genTypeCon :: ModuleName -> Qualified TypeConName -> ((T.Text, T.Text), Set.Set ModuleRef)
genTypeCon curModName (Qualified pkgRef modName conParts) =
    case unTypeConName conParts of
        [] -> error "IMPOSSIBLE: empty type constructor name"
        _:_:_ -> error "TODO(MH): multi-part type constructor names"
        [conName]
          | modRef == (PRSelf, curModName) -> (dup conName, Set.empty)
          | otherwise -> (dup (genModuleRef modRef <> "." <> conName), Set.singleton modRef)
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
