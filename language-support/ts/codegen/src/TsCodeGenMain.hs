-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module TsCodeGenMain (main) where

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
import Data.Maybe
import Options.Applicative
import System.Directory
import System.FilePath hiding ((<.>))
import qualified System.FilePath as FP

data Options = Options
    { optInputDar :: FilePath
    , optOutputDir :: FilePath
    , optMainPackageName :: Maybe String
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
    <*> optional (strOption
        (  long "main-package-name"
        <> metavar "STRING"
        <> help "Package name to use for the main DALF of the DAR"
        ))

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
    forM_ ((DAR.mainDalf dalfs, optMainPackageName) : map (, Nothing) (DAR.dalfs dalfs)) $ \(dalf, mbPkgName) -> do
        (pkgId, pkg) <- either (fail . show)  pure $ Archive.decodeArchive Archive.DecodeAsMain (BSL.toStrict dalf)
        daml2ts opts pkgId pkg mbPkgName

daml2ts :: Options -> PackageId -> Package -> Maybe String -> IO ()
daml2ts Options{..} pkgId pkg mbPkgName = do
    let outputDir = optOutputDir </> fromMaybe (T.unpack (unPackageId pkgId)) mbPkgName
    createDirectoryIfMissing True outputDir
    T.writeFileUtf8 (outputDir </> "packageId.ts") $ T.unlines
        ["export default '" <> unPackageId pkgId <> "';"]
    forM_ (packageModules pkg) $ \mod -> do
        whenJust (genModule pkgId mod) $ \modTxt -> do
            let outputFile = outputDir </> joinPath (map T.unpack (unModuleName (moduleName mod))) FP.<.> "ts"
            putStrLn $ "Generating " ++ outputFile
            createDirectoryIfMissing True (takeDirectory outputFile)
            T.writeFileUtf8 outputFile modTxt

dup :: a -> (a, a)
dup x = (x, x)

infixr 6 <.> -- This is the same fixity as '<>'.
(<.>) :: T.Text -> T.Text -> T.Text
(<.>) u v = u <> "." <> v

genModule :: PackageId -> Module -> Maybe T.Text
genModule curPkgId mod
  | null serDefs = Nothing
  | otherwise =
    let curModName = moduleName mod
        pkgRootPath
          | lenModName == 1 = "."
          | otherwise = T.intercalate "/" (replicate (lenModName - 1) "..")
          where
            lenModName = length (unModuleName curModName)
        tpls = moduleTemplates mod
        (defSers, refs) = unzip (map (genDataDef curPkgId mod tpls) serDefs)
        header =
            ["// Generated from " <> T.intercalate "/" (unModuleName curModName) <> ".daml"
            ,"/* eslint-disable @typescript-eslint/camelcase */"
            ,"/* eslint-disable @typescript-eslint/no-use-before-define */"
            ,"import * as jtv from '@mojotech/json-type-validation';"
            ,"import * as daml from '@daml/types';"
            ]
        imports =
            ["import * as " <> modNameStr <> " from '" <> pkgRootPath <> "/" <> pkgRefStr <> T.intercalate "/" (unModuleName modName) <> "';"
            | modRef@(pkgRef, modName) <- Set.toList ((PRSelf, curModName) `Set.delete` Set.unions refs)
            , let pkgRefStr = case pkgRef of
                    PRSelf -> ""
                    PRImport pkgId -> "../" <> unPackageId pkgId <> "/"
            , let modNameStr = genModuleRef modRef
            ]
        defs = map (\(def, ser) -> def ++ ser) defSers
    in
    Just $ T.unlines $ intercalate [""] $ filter (not . null) $ header : imports : defs
  where
    serDefs = defDataTypes mod

defDataTypes :: Module -> [DefDataType]
defDataTypes mod = filter (getIsSerializable . dataSerializable) (NM.toList (moduleDataTypes mod))

genDataDef :: PackageId -> Module -> NM.NameMap Template -> DefDataType -> (([T.Text], [T.Text]), Set.Set ModuleRef)
genDataDef curPkgId mod tpls def = case unTypeConName (dataTypeCon def) of
    [] -> error "IMPOSSIBLE: empty type constructor name"
    _: _: _: _ -> error "IMPOSSIBLE: multi-part type constructor of more than two names"

    [conName] -> genDefDataType curPkgId conName mod tpls def
    [c1, c2] -> ((makeNamespace $ map ("  " <>) typs, []), refs)
      where
        ((typs, _), refs) = genDefDataType curPkgId c2 mod tpls def
        makeNamespace stuff =
          [ "// eslint-disable-next-line @typescript-eslint/no-namespace"
          , "export namespace " <> c1 <> " {"] ++ stuff ++ ["} //namespace " <> c1]

genDefDataType :: PackageId -> T.Text -> Module -> NM.NameMap Template -> DefDataType -> (([T.Text], [T.Text]), Set.Set ModuleRef)
genDefDataType curPkgId conName mod tpls def =
    case dataCons def of
        DataVariant bs ->
          let
            (typs, sers) = unzip $ map genBranch bs
            typeDesc = makeType ([""] ++ typs)
            typ = conName <> typeParams -- Type of the variant.
            serDesc =
              if not $ null paramNames -- Polymorphic type.
              then -- Companion function.
                let
                  -- Any associated serializers.
                  assocSers = map (\(n, d) -> serFromDef id n d) assocDefDataTypes
                  -- The variant deserializer.
                  function = onLast (<> ";") (makeSer ( ["() => jtv.oneOf<" <> typ <> ">("] ++ sers ++ [")"]));
                  props = -- Fix the first and last line of each serializer.
                    concatMap (onHead (fromJust . T.stripPrefix (T.pack "export const ")) . onLast (<> ";")) assocSers
                  -- The complete definition of the companion function.
                  in function ++ props
                     -- To-do: Can we formulate a static implements
                     -- serializable check that works for companion
                     -- functions?
              else -- Companion object.
                let
                  assocNames = map fst assocDefDataTypes
                  -- Any associated serializers, dropping the first line
                  -- of each.
                  assocSers = map (\(n, d) -> (n, serFromDef (drop 1) n d)) assocDefDataTypes
                  -- Type of the companion object.
                  typ' = "daml.Serializable<" <> conName <> "> & {\n" <>
                    T.concat (map (\n -> "    " <> n <> ": daml.Serializable<" <> (conName <.> n) <> ">;\n") assocNames) <>
                    "  }"
                  -- Body of the companion object.
                  body = map ("  " <>) $
                    -- The variant deserializer.
                    ["decoder: () => jtv.oneOf<" <> typ <> ">("] ++  sers ++ ["),"] ++
                    -- Remember how we dropped the first line of each
                    -- associated serializer above? This replaces them.
                    concatMap (\(n, ser) -> n <> ": ({" : onLast (<> ",") ser) assocSers
                  -- The complete definition of the companion object.
                  in ["export const " <> conName <> ":\n  " <> typ' <> " = ({"] ++ body ++ ["});"] ++
                     ["daml.STATIC_IMPLEMENTS_SERIALIZABLE_CHECK<" <> conName <> ">(" <> conName <> ")"]
            in ((typeDesc, serDesc), Set.unions $ map (Set.setOf typeModuleRef . snd) bs)
        DataEnum enumCons ->
          let
            typeDesc =
                [ "export enum " <> conName <> " {"] ++
                [ "  " <> cons <> " = " <> "\'" <> cons <> "\'" <> ","
                | VariantConName cons <- enumCons] ++
                [ "}"
                , "daml.STATIC_IMPLEMENTS_SERIALIZABLE_CHECK<" <> conName <> ">(" <> conName <> ")"
                ]
            serDesc =
                ["() => jtv.oneOf<" <> conName <> ">" <> "("] ++
                ["  jtv.constant(" <> conName <> "." <> cons <> ")," | VariantConName cons <- enumCons] ++
                [");"]
          in
          ((typeDesc, makeNameSpace serDesc), Set.empty)
        DataRecord fields ->
            let (fieldNames, fieldTypesLf) = unzip [(unFieldName x, t) | (x, t) <- fields]
                (fieldTypesTs, fieldSers) = unzip (map (genType (moduleName mod)) fieldTypesLf)
                fieldRefs = map (Set.setOf typeModuleRef . snd) fields
                typeDesc =
                    ["{"] ++
                    ["  " <> x <> ": " <> t <> ";" | (x, t) <- zip fieldNames fieldTypesTs] ++
                    ["}"]
                serDesc =
                    ["() => jtv.object({"] ++
                    ["  " <> x <> ": " <> ser <> ".decoder()," | (x, ser) <- zip fieldNames fieldSers] ++
                    ["})"]
            in
            case NM.lookup (dataTypeCon def) tpls of
                Nothing -> ((makeType typeDesc, makeSer serDesc), Set.unions fieldRefs)
                Just tpl ->
                    let (chcs, argRefs) = unzip
                            [((unChoiceName (chcName chc), t, rtyp, rser), Set.union argRefs retRefs)
                            | chc <- NM.toList (tplChoices tpl)
                            , let tLf = snd (chcArgBinder chc)
                            , let rLf = chcReturnType chc
                            , let (t, _) = genType (moduleName mod) tLf
                            , let (rtyp, rser) = genType (moduleName mod) rLf
                            , let argRefs = Set.setOf typeModuleRef tLf
                            , let retRefs = Set.setOf typeModuleRef rLf
                            ]
                        (keyTypeTs, keySer) = case tplKey tpl of
                            Nothing -> ("undefined", "() => jtv.constant(undefined)")
                            Just key -> (conName <.> "Key", "() => " <> snd (genType (moduleName mod) (tplKeyType key)) <> ".decoder()")
                        templateId =unPackageId curPkgId <> ":" <> T.intercalate "." (unModuleName (moduleName mod)) <> ":" <> conName 
                        dict =
                            ["export const " <> conName <> ": daml.Template<" <> conName <> ", " <> keyTypeTs <> ", '" <> templateId <> "'> & {"] ++
                            ["  " <> x <> ": daml.Choice<" <> conName <> ", " <> t <> ", " <> rtyp <> ", " <> keyTypeTs <> ">;" | (x, t, rtyp, _) <- chcs] ++
                            ["} = {"
                            ] ++
                            ["  templateId: '" <> templateId <> "',"
                            ,"  keyDecoder: " <> keySer <> ","
                            ] ++
                            map ("  " <>) (onLast (<> ",") (onHead ("decoder: " <>) serDesc)) ++
                            concat
                            [ ["  " <> x <> ": {"
                              ,"    template: () => " <> conName <> ","
                              ,"    choiceName: '" <> x <> "',"
                              ,"    argumentDecoder: " <> t <> ".decoder,"
                              -- We'd write,
                              --   "   resultDecoder: " <> rser <> ".decoder"
                              -- here but, consider the following scenario:
                              --   export const Person: daml.Template<Person>...
                              --    = {  ...
                              --         Birthday: { resultDecoder: daml.ContractId(Person).decoder, ... }
                              --         ...
                              --      }
                              -- This gives rise to "error TS2454: Variable 'Person' is used before being assigned."
                              ,"    resultDecoder: () => " <> rser <> ".decoder()," -- Eta-conversion provides an escape hatch.
                              ,"  },"
                              ]
                            | (x, t, _rtyp, rser) <- chcs
                            ] ++
                            ["};"]
                        associatedTypes =
                          maybe [] (\key ->
                              [ "// eslint-disable-next-line @typescript-eslint/no-namespace"
                              , "export namespace " <> conName <> " {"] ++
                              ["  export type Key = " <> fst (genType (moduleName mod) (tplKeyType key)) <> ""] ++
                              ["}"]) (tplKey tpl)
                        registrations =
                            ["daml.registerTemplate(" <> conName <> ");"]
                        refs = Set.unions (fieldRefs ++ argRefs)
                    in
                    ((makeType typeDesc, dict ++ associatedTypes ++ registrations), refs)
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
            map ("  " <>) (onLast (<> ",") (onHead ("decoder: " <>) serDesc)) ++
            ["})"]
        makeNameSpace serDesc =
            [ "// eslint-disable-next-line @typescript-eslint/no-namespace"
            , "export namespace " <> conName <> " {"
            ] ++
            map ("  " <>) (onHead ("export const decoder = " <>) serDesc) ++
            ["}"]
        genBranch (VariantConName cons, t) =
          let (typ, ser) = genType (moduleName mod) t in
          ( "  |  { tag: '" <> cons <> "'; value: " <> typ <> " }"
          , "  jtv.object({tag: jtv.constant('" <> cons <> "'), value: jtv.lazy(() => " <> ser <> ".decoder())}),"
          )
        -- A type such as
        --   data Q = C { x: Int, y: Text }| G { z: Bool }
        -- has a DAML-LF representation like,
        --   record Q.C = { x: Int, y: String }
        --   record Q.G = { z: Bool }
        --   variant Q = C Q.C | G Q.G
        -- This constant is the definitions of 'Q.C' and 'Q.G' given
        -- 'Q'.
        assocDefDataTypes =
          [(sub, def) | def <- defDataTypes mod
            , [sup, sub] <- [unTypeConName (dataTypeCon def)], sup == conName]
        -- Extract the serialization code associated with a data type
        -- definition.
        serFromDef f c2 = f . snd . fst . genDefDataType curPkgId (conName <.> c2) mod tpls

genType :: ModuleName -> Type -> (T.Text, T.Text)
genType curModName = go
  where
    go = \case
        TVar v -> dup (unTypeVarName v)
        TUnit -> ("{}", "daml.Unit")
        TBool -> ("boolean", "daml.Bool")
        TInt64 -> dup "daml.Int"
        TDecimal -> dup "daml.Decimal"
        TNumeric (TNat n) -> (
            "daml.Numeric"
          , "daml.Numeric(" <> T.pack (show (fromTypeLevelNat n :: Integer)) <> ")"
          )
        TText -> ("string", "daml.Text")
        TTimestamp -> dup "daml.Time"
        TParty -> dup "daml.Party"
        TDate -> dup "daml.Date"
        TList t ->
            let (t', ser) = go t
            in
            (t' <> "[]", "daml.List(" <> ser <> ")")
        TOptional t ->
            let (t', ser) = go t
            in
            ("daml.Optional<" <> t' <> ">", "daml.Optional(" <> ser <> ")")
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
        TSynApp{} -> error "IMPOSSIBLE: type synonym not serializable"
        t@TApp{} -> error $ "IMPOSSIBLE: type application not serializable - " <> DA.Pretty.renderPretty t
        TBuiltin t -> error $ "IMPOSSIBLE: partially applied primitive type not serializable - " <> DA.Pretty.renderPretty t
        TForall{} -> error "IMPOSSIBLE: universally quantified type not serializable"
        TStruct{} -> error "IMPOSSIBLE: structural record not serializable"
        TNat{} -> error "IMPOSSIBLE: standalone type level natural not serializable"

genTypeCon :: ModuleName -> Qualified TypeConName -> (T.Text, T.Text)
genTypeCon curModName (Qualified pkgRef modName conParts) =
    case unTypeConName conParts of
        [] -> error "IMPOSSIBLE: empty type constructor name"
        _: _: _: _ -> error "TODO(MH): multi-part type constructor names"
        [c1 ,c2]
          | modRef == (PRSelf, curModName) -> dup $ c1 <.> c2
          | otherwise -> dup $ genModuleRef modRef <> c1 <.> c2
        [conName]
          | modRef == (PRSelf, curModName) -> dup conName
          | otherwise -> dup $ genModuleRef modRef <.> conName
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

onLast :: (a -> a) -> [a] -> [a]
onLast f = \case
    [] -> []
    [l] -> [f l]
    x : xs -> x : onLast f xs
