-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This module extracts docs from DAML modules. It does so by reading
-- haddock-style comments from the parsed syntax tree and correlating them
-- with definitions in the typechecked module in order to obtain accurate
-- type information.
module DA.Daml.Doc.Extract
    ( ExtractOptions (..)
    , QualifyTypes (..)
    , defaultExtractOptions
    , extractDocs
    ) where

import DA.Daml.Doc.Types as DDoc
import DA.Daml.Doc.Anchor as DDoc
import DA.Daml.Doc.Extract.Exports

import Development.IDE.Types.Options (IdeOptions(..))
import Development.IDE.Core.FileStore
import qualified Development.IDE.Core.Service     as Service
import qualified Development.IDE.Core.Rules     as Service
import qualified Development.IDE.Core.RuleTypes as Service
import qualified Development.IDE.Core.OfInterest as Service
import           Development.IDE.Types.Diagnostics
import Development.IDE.Types.Logger
import Development.IDE.Types.Location

import           "ghc-lib" GHC
import           "ghc-lib-parser" Module
import           "ghc-lib-parser" TyCoRep
import           "ghc-lib-parser" TyCon
import           "ghc-lib-parser" ConLike
import           "ghc-lib-parser" DataCon
import           "ghc-lib-parser" Class
import           "ghc-lib-parser" BasicTypes
import           "ghc-lib-parser" InstEnv
import           "ghc-lib-parser" CoreSyn
import           "ghc-lib-parser" Var
import           "ghc-lib-parser" Id
import           "ghc-lib-parser" Name
import           "ghc-lib-parser" RdrName
import qualified "ghc-lib-parser" Outputable                      as Out
import qualified "ghc-lib-parser" DynFlags                        as DF
import           "ghc-lib-parser" Bag (bagToList)

import           Control.Monad.Except             as Ex
import Control.Monad.Trans.Maybe
import           Data.Char (isSpace)
import           Data.List.Extra
import           Data.Maybe
import qualified Data.Map.Strict as MS
import qualified Data.Set as Set
import qualified Data.Text                                 as T
import           Data.Tuple.Extra                          (second)
import Data.Either

-- | Options that affect doc extraction.
data ExtractOptions = ExtractOptions
    { eo_qualifyTypes :: QualifyTypes
        -- ^ qualify non-local types
    , eo_simplifyQualifiedTypes :: Bool
        -- ^ drop common module prefix when qualifying types
    } deriving (Eq, Show, Read)

data QualifyTypes
    = QualifyTypesAlways
    | QualifyTypesInPackage
    | QualifyTypesNever
    deriving (Eq, Show, Read)

-- | Default options for doc extraction.
defaultExtractOptions :: ExtractOptions
defaultExtractOptions = ExtractOptions
    { eo_qualifyTypes = QualifyTypesNever
    , eo_simplifyQualifiedTypes = False
    }

-- | Extract documentation in a dependency graph of modules.
extractDocs ::
    ExtractOptions
    -> IdeOptions
    -> [NormalizedFilePath]
    -> Ex.ExceptT [FileDiagnostic] IO [ModuleDoc]
extractDocs extractOpts ideOpts fp = do
    modules <- haddockParse ideOpts fp
    pure $ map mkModuleDocs modules

  where
    modDoc :: TypecheckedModule -> Maybe DocText
    modDoc
        = fmap (docToText . unLoc)
        . hsmodHaddockModHeader . unLoc
        . pm_parsed_source . tm_parsed_module

    mkModuleDocs :: Service.TcModuleResult -> ModuleDoc
    mkModuleDocs tmr =
        let ctx@DocCtx{..} = buildDocCtx extractOpts (Service.tmrModule tmr)
            typeMap = MS.fromList $ mapMaybe (getTypeDocs ctx) dc_decls
            classDocs = mapMaybe (getClsDocs ctx) dc_decls

            (md_classes, templateInstanceClasses) =
                partitionEithers . flip map classDocs $ \classDoc ->
                    case stripInstanceSuffix (cl_name classDoc) of
                        Nothing -> Left classDoc
                        Just templateName -> Right (templateName, classDoc)
            templateInstanceClassMap = MS.fromList templateInstanceClasses

            md_name = dc_modname
            md_anchor = Just (moduleAnchor md_name)
            md_descr = modDoc dc_tcmod
            md_templates = getTemplateDocs ctx typeMap templateInstanceClassMap
            md_functions = mapMaybe (getFctDocs ctx) dc_decls
            md_instances = map (getInstanceDocs ctx) dc_insts

            filteredAdts -- all ADT docs without templates or choices
                = MS.elems . MS.withoutKeys typeMap . Set.unions
                $ dc_templates : MS.elems dc_choices

            (md_adts, md_templateInstances) =
                partitionEithers . flip map filteredAdts $ \adt ->
                    case getTemplateInstanceDoc adt of
                        Nothing -> Left adt
                        Just ti -> Right ti

        in ModuleDoc {..}

-- | This is equivalent to Haddock’s Haddock.Interface.Create.collectDocs
collectDocs :: [LHsDecl a] -> [(LHsDecl a, Maybe DocText)]
collectDocs = go Nothing []
  where
    go :: Maybe (LHsDecl p) -> [DocText] -> [LHsDecl p] -> [(LHsDecl p, Maybe DocText)]
    go Nothing _ [] = []
    go (Just prev) docs [] = finished prev docs []
    go prev docs (L _ (DocD _ (DocCommentNext str)) : ds)
      | Nothing <- prev = go Nothing (docToText str:docs) ds
      | Just decl <- prev = finished decl docs (go Nothing [docToText str] ds)
    go prev docs (L _ (DocD _ (DocCommentPrev str)) : ds) =
      go prev (docToText str:docs) ds
    go Nothing docs (d:ds) = go (Just d) docs ds
    go (Just prev) docs (d:ds) = finished prev docs (go (Just d) [] ds)

    finished decl docs rest = (decl, toDocText . map unDocText . reverse $ docs) : rest

-- | Context in which to extract a module's docs. This is created from
-- 'TypecheckedModule' by 'buildDocCtx'.
data DocCtx = DocCtx
    { dc_ghcMod :: GHC.Module
        -- ^ ghc name for current module
    , dc_modname :: Modulename
        -- ^ name of the current module
    , dc_tcmod :: TypecheckedModule
        -- ^ typechecked module
    , dc_decls :: [DeclData]
        -- ^ module declarations
    , dc_insts :: [ClsInst]
        -- ^ typeclass instances
    , dc_tycons :: MS.Map Typename TyCon
        -- ^ types defined in this module
    , dc_datacons :: MS.Map Typename DataCon
        -- ^ constructors defined in this module
    , dc_ids :: MS.Map Fieldname Id
        -- ^ values defined in this module
    , dc_templates :: Set.Set Typename
        -- ^ DAML templates defined in this module
    , dc_choices :: MS.Map Typename (Set.Set Typename)
        -- ^ choices per DAML template defined in this module
    , dc_extractOptions :: ExtractOptions
        -- ^ command line options that affect the doc extractor
    , dc_exports :: ExportSet
        -- ^ set of export, unless everything is exported
    }

-- | Parsed declaration with associated docs.
data DeclData = DeclData
    { _dd_decl :: LHsDecl GhcPs
    , _dd_docs :: Maybe DocText
    }

buildDocCtx :: ExtractOptions -> TypecheckedModule -> DocCtx
buildDocCtx dc_extractOptions dc_tcmod  =
    let dc_ghcMod = ms_mod . pm_mod_summary . tm_parsed_module $ dc_tcmod
        dc_modname = getModulename dc_ghcMod
        dc_decls
            = map (uncurry DeclData) . collectDocs . hsmodDecls . unLoc
            . pm_parsed_source . tm_parsed_module $ dc_tcmod
        (dc_templates, dc_choices)
            = getTemplateData . tm_parsed_module $ dc_tcmod

        tythings = modInfoTyThings . tm_checked_module_info $ dc_tcmod
        dc_insts = modInfoInstances . tm_checked_module_info $ dc_tcmod

        dc_tycons = MS.fromList
            [ (typename, tycon)
            | ATyCon tycon <- tythings
            , let typename = Typename . packName . tyConName $ tycon
            ]

        dc_datacons = MS.fromList
            [ (conname, datacon)
            | AConLike (RealDataCon datacon) <- tythings
            , let conname = Typename . packName . dataConName $ datacon
            ]

        dc_ids = MS.fromList
            [ (fieldname, id)
            | AnId id <- tythings
            , let fieldname = Fieldname . packId $ id
            ]

        dc_exports = extractExports . tm_parsed_module $ dc_tcmod

    in DocCtx {..}

-- | Parse and typecheck a module and its dependencies in Haddock mode
--   (retaining Doc declarations), and return the 'TcModuleResult's in
--   dependency order (top module last).
--
--   "Internal" modules are filtered out, they don't contribute to the docs.
--
--   Not using the cached file store, as it is expected to run stand-alone
--   invoked by a CLI tool.
haddockParse :: IdeOptions ->
                [NormalizedFilePath] ->
                Ex.ExceptT [FileDiagnostic] IO [Service.TcModuleResult]
haddockParse opts f = ExceptT $ do
  vfs <- makeVFSHandle
  service <- Service.initialise Service.mainRule (const $ pure ()) noLogging opts vfs
  Service.setFilesOfInterest service (Set.fromList f)
  parsed  <- Service.runAction service $
             runMaybeT $
             do deps <- Service.usesE Service.GetDependencies f
                Service.usesE Service.TypeCheck $ nubOrd $ f ++ concatMap Service.transitiveModuleDeps deps
                -- The DAML compiler always parses with Opt_Haddock on
  diags <- Service.getDiagnostics service
  pure (maybe (Left diags) Right parsed)

------------------------------------------------------------

toDocText :: [T.Text] -> Maybe DocText
toDocText docs =
    if null docs
        then Nothing
        else Just . DocText . T.strip . T.unlines $ docs

-- | Extracts the documentation of a function. Comments are either
--   adjacent to a type signature, or to the actual function definition. If
--   neither a comment nor a function type is in the source, we omit the
--   function.
getFctDocs :: DocCtx -> DeclData -> Maybe FunctionDoc
getFctDocs ctx@DocCtx{..} (DeclData decl docs) = do
    name <- case unLoc decl of
        SigD _ (TypeSig _ (L _ n :_) _) -> Just n
        ValD _ FunBind{..} | not (null docs) ->
            Just (unLoc fun_id)
            -- NB assuming we do _not_ have a type signature for the function in the
            -- pairs (otherwise we'll get a duplicate)
        _ ->
            Nothing

    let fct_name = Fieldname (packRdrName name)
    id <- MS.lookup fct_name dc_ids

    let ty = idType id
        fct_context = typeToContext ctx ty
        fct_type = typeToType ctx ty
        fct_anchor = Just $ functionAnchor dc_modname fct_name
        fct_descr = docs

    guard (exportsFunction dc_exports fct_name)
    Just FunctionDoc {..}

getClsDocs :: DocCtx -> DeclData -> Maybe ClassDoc
getClsDocs ctx@DocCtx{..} (DeclData (L _ (TyClD _ ClassDecl{..})) tcdocs) = do
    let cl_name = Typename . packRdrName $ unLoc tcdLName
    tycon <- MS.lookup cl_name dc_tycons
    tycls <- tyConClass_maybe tycon
    let cl_anchor = Just $ classAnchor dc_modname cl_name
        cl_descr = tcdocs
        cl_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
        opMap = MS.fromList
            [ (Fieldname $ packId id, (id, dmInfoM))
            | (id, dmInfoM) <- classOpItems tycls ]
        cl_methods = concatMap (getMethodDocs opMap cl_anchor cl_name cl_args) subDecls
        cl_super = do
            let theta = classSCTheta tycls
            guard (notNull theta)
            Just (TypeTuple $ map (typeToType ctx) theta)
        cl_instances = Nothing -- filled out later in 'distributeInstanceDocs'
    guard (exportsType dc_exports cl_name)
    Just ClassDoc {..}
  where
    -- All documentation of class members is stored in tcdDocs.
    -- To associate the docs with the correct member, we convert all members
    -- and docs to declarations, sort them by their location
    -- and then use collectDocs.
    -- This is the equivalent of Haddock’s Haddock.Interface.Create.classDecls.
    subDecls :: [(LHsDecl GhcPs, Maybe DocText)]
    subDecls = collectDocs . sortOn getLoc $ decls
    decls = docs ++ defs ++ sigs ++ ats
    docs = map (fmap (DocD noExt)) tcdDocs
    defs = map (fmap (ValD noExt)) (bagToList tcdMeths)
    sigs = map (fmap (SigD noExt)) tcdSigs
    ats = map (fmap (TyClD noExt . FamDecl noExt)) tcdATs

    -- | Extract typeclass method docs from a subDecl. Notice that
    -- we may have to generate multiple docs from a single declaration,
    -- thanks to declarations of the form
    --
    -- @
    --     (+), (-) : t -> t -> t
    -- @
    --
    -- where multiple names are present. In that case we just duplicate
    -- the associated docs for each name that is defined.
    getMethodDocs ::
        MS.Map Fieldname ClassOpItem
        -> Maybe Anchor
        -> Typename
        -> [T.Text]
        -> (LHsDecl GhcPs, Maybe DocText)
        -> [ClassMethodDoc]
    getMethodDocs opMap cl_anchor cl_name cl_args = \case
        (L _ (SigD _ (ClassOpSig _ cm_isDefault rdrNamesL _)), cm_descr) ->
            flip mapMaybe rdrNamesL $ \rdrNameL -> do
                let cm_name = Fieldname . packRdrName . unLoc $ rdrNameL
                    cm_anchor = guard (not cm_isDefault) >>
                        Just (functionAnchor dc_modname cm_name)
                (id, dmInfoM) <- MS.lookup cm_name opMap

                let ghcType
                        | cm_isDefault = -- processing default method type sig
                            case dmInfoM of
                                Nothing ->
                                    error "getMethodDocs: expected default method to have associated default method info"
                                Just (_, VanillaDM) -> idType id
                                Just (_, GenericDM ty) -> ty
                        | otherwise = -- processing original method type sig
                            idType id
                    cm_type = typeToType ctx ghcType
                    cm_globalContext = typeToContext ctx ghcType
                    cm_localContext = do
                        context <- cm_globalContext
                        dropMemberContext cl_anchor cl_args context
                guard (exportsField dc_exports cl_name cm_name)
                Just ClassMethodDoc{..}

        _ -> []

    -- | Remove the implied context from typeclass member functions.
    dropMemberContext :: Maybe Anchor -> [T.Text] -> DDoc.Type -> Maybe DDoc.Type
    dropMemberContext cl_anchor cl_args = \case
        TypeTuple xs -> do
            let xs' = filter (not . matchesMemberContext cl_anchor cl_args) xs
            guard (notNull xs')
            Just (TypeTuple xs')

        _ -> error "dropMemberContext: expected type tuple as context"
            -- TODO: Move to using a more appropriate type
            -- for contexts in damldocs, to avoid this case.

    -- | Is this the implied context for member functions? We use an anchor
    -- for comparison because it is more accurate than typenames (which are
    -- generally susceptible to qualification and shadowing).
    matchesMemberContext :: Maybe Anchor -> [T.Text] -> DDoc.Type -> Bool
    matchesMemberContext cl_anchor cl_args ty =
        cl_anchor == getTypeAppAnchor ty &&
        Just [(Just (Typename arg), Just []) | arg <- cl_args] ==
            (map (\arg -> (getTypeAppName arg, getTypeAppArgs arg))
            <$> getTypeAppArgs ty)

getClsDocs _ _ = Nothing

unknownType :: DDoc.Type
unknownType = TypeApp Nothing (Typename "_") []

getTypeDocs :: DocCtx -> DeclData -> Maybe (Typename, ADTDoc)
getTypeDocs ctx@DocCtx{..} (DeclData (L _ (TyClD _ decl)) doc)
    | XTyClDecl{} <- decl =
        Nothing
    | ClassDecl{} <- decl =
        Nothing
    | FamDecl{}   <- decl =
        Nothing

    | SynDecl{..} <- decl = do
        let ad_name = Typename . packRdrName $ unLoc tcdLName
            ad_descr = doc
            ad_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
            ad_anchor = Just $ typeAnchor dc_modname ad_name
            ad_rhs = fromMaybe unknownType $ do
                tycon <- MS.lookup ad_name dc_tycons
                rhs <- synTyConRhs_maybe tycon
                Just (typeToType ctx rhs)
            ad_instances = Nothing -- filled out later in 'distributeInstanceDocs'
        guard (exportsType dc_exports ad_name)
        Just (ad_name, TypeSynDoc {..})

    | DataDecl{..} <- decl = do
        let ad_name = Typename . packRdrName $ unLoc tcdLName
            ad_descr = doc
            ad_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
            ad_anchor = Just $ typeAnchor dc_modname ad_name
            ad_constrs = mapMaybe (constrDoc ad_name) . dd_cons $ tcdDataDefn
            ad_instances = Nothing -- filled out later in 'distributeInstanceDocs'
        guard (exportsType dc_exports ad_name)
        Just (ad_name, ADTDoc {..})
  where
    constrDoc :: Typename -> LConDecl GhcPs -> Maybe ADTConstr
    constrDoc ad_name (L _ con) = do
        let ac_name = Typename . packRdrName . unLoc $ con_name con
            ac_anchor = Just $ constrAnchor dc_modname ac_name
            ac_descr = fmap (docToText . unLoc) $ con_doc con
            ac_args =
                case MS.lookup ac_name dc_datacons of
                    Nothing ->
                        -- no type info available for args ... we can display "_" though
                        replicate
                            (length . hsConDeclArgTys . con_args $ con)
                            unknownType
                    Just datacon ->
                        map (typeToType ctx) (dataConOrigArgTys datacon)

        guard (exportsConstr dc_exports ad_name ac_name)
        Just $ case con_args con of
            PrefixCon _ -> PrefixC {..}
            InfixCon _ _ -> PrefixC {..} -- FIXME: should probably change this!
            RecCon (L _ fs) ->
              let ac_fields = mapMaybe (fieldDoc ad_name) (zip ac_args fs)
              in RecordC {..}

    fieldDoc :: Typename -> (DDoc.Type, LConDeclField GhcPs) -> Maybe FieldDoc
    fieldDoc ad_name (fd_type, L _ ConDeclField{..}) = do
        let fd_name = Fieldname . T.concat . map (toText . unLoc) $ cd_fld_names
            fd_anchor = Just $ functionAnchor dc_modname fd_name
            fd_descr = fmap (docToText . unLoc) cd_fld_doc
        guard (exportsField dc_exports ad_name fd_name)
        Just FieldDoc{..}
    fieldDoc _ (_, L _ XConDeclField{}) = Nothing

getTypeDocs _ _other = Nothing

-- | Build template docs up from ADT and class docs.
getTemplateDocs ::
    DocCtx
    -> MS.Map Typename ADTDoc -- ^ maps template names to their ADT docs
    -> MS.Map Typename ClassDoc -- ^ maps template names to their template instance class docs
    -> [TemplateDoc]
getTemplateDocs DocCtx{..} typeMap templateInstanceMap =
    map mkTemplateDoc $ Set.toList dc_templates
  where
    -- The following functions use the type map and choice map in scope, so
    -- defined internally, and not expected to fail on consistent arguments.
    mkTemplateDoc :: Typename -> TemplateDoc
    mkTemplateDoc name = TemplateDoc
      { td_anchor = ad_anchor tmplADT
      , td_name = ad_name tmplADT
      , td_args = ad_args tmplADT
      , td_super = cl_super =<< MS.lookup name templateInstanceMap
      , td_descr = ad_descr tmplADT
      , td_payload = getFields tmplADT
      -- assumes exactly one record constructor (syntactic, template syntax)
      , td_choices = map mkChoiceDoc choices
      }
      where
        tmplADT = asADT name
        choices = Set.toList . fromMaybe Set.empty $ MS.lookup name dc_choices

    mkChoiceDoc :: Typename -> ChoiceDoc
    mkChoiceDoc name = ChoiceDoc
      { cd_name = ad_name choiceADT
      , cd_descr = ad_descr choiceADT
      -- assumes exactly one constructor (syntactic in the template syntax), or
      -- uses a dummy value otherwise.
      , cd_fields = getFields choiceADT
      }
          where choiceADT = asADT name

    asADT n = fromMaybe dummyDT $
              MS.lookup n typeMap
    -- returns a dummy ADT if the choice argument is not in the local type map
    -- (possible if choice instances are defined directly outside the template).
    -- This wouldn't be necessary if we used the type-checked AST.
      where dummyDT = ADTDoc { ad_anchor = Nothing
                             , ad_name    = Typename $ "External:" <> unTypename n
                             , ad_descr   = Nothing
                             , ad_args = []
                             , ad_constrs = []
                             , ad_instances = Nothing
                             }
    -- Assuming one constructor (record or prefix), extract the fields, if any.
    -- For choices without arguments, GHC returns a prefix constructor, so we
    -- need to cater for this case specially.
    getFields adt = case ad_constrs adt of
                      [PrefixC{}] -> []
                      [RecordC{ ac_fields = fields }] -> fields
                      [] -> [] -- catching the dummy case here, see above
                      _other -> error "getFields: found multiple constructors"

-- | A template instance is desugared into a newtype with a docs marker.
-- For example,
--
-- @template instance ProposalIou = Proposal Iou@
--
-- becomes
--
-- @newtype ProposalIou = ProposalIou (Proposal Iou) -- ^ TEMPLATE_INSTANCE@
--
-- So the goal of this function is to extract the template instance doc
-- from the newtype doc if it exists.
getTemplateInstanceDoc :: ADTDoc -> Maybe TemplateInstanceDoc
getTemplateInstanceDoc adt
    | ADTDoc{..} <- adt
    , [PrefixC{..}] <- ad_constrs
    , Just (DocText "TEMPLATE_INSTANCE") <- ac_descr
    , [argType] <- ac_args
    = Just TemplateInstanceDoc
        { ti_name = ad_name
        , ti_anchor = ad_anchor
        , ti_descr = ad_descr
        , ti_rhs = argType
        }

    | otherwise
    = Nothing

-- recognising Template and Choice instances


-- | Extracts all names of templates defined in a module,
-- and a map of template names to its set of choices
getTemplateData :: ParsedModule ->
    ( Set.Set Typename
    , MS.Map Typename (Set.Set Typename) )
getTemplateData ParsedModule{..} =
  let
    instDs    = mapMaybe (isInstDecl . unLoc) . hsmodDecls . unLoc $ pm_parsed_source
    templates = mapMaybe isTemplate instDs
    choiceMap = MS.fromListWith (<>) $
                map (second Set.singleton) $
                mapMaybe isChoice instDs
  in
    (Set.fromList templates, choiceMap)
    where
      isInstDecl (InstD _ (ClsInstD _ i)) = Just i
      isInstDecl _ = Nothing


-- | If the given instance declaration is declaring a template instance, return
--   its name (IdP). Used to build the set of templates declared in a module.
isTemplate :: ClsInstDecl GhcPs -> Maybe Typename
isTemplate (XClsInstDecl _) = Nothing
isTemplate ClsInstDecl{..}
  | L _ ty <- getLHsInstDeclHead cid_poly_ty
  , HsAppTy _ (L _ t1) t2 <- ty
  , HsTyVar _ _ (L _ tmplClass) <- t1
  , Just (L _ tmplName) <- hsTyGetAppHead_maybe t2
  , toText tmplClass == "DA.Internal.Desugar.Template"
  || toText tmplClass == "Template" -- temporary for generic templates
  = Just (Typename . packRdrName $ tmplName)

  | otherwise = Nothing

-- | If the given instance declaration is declaring a template choice instance,
--   return template and choice name (IdP). Used to build the set of choices
--   per template declared in a module.
isChoice :: ClsInstDecl GhcPs -> Maybe (Typename, Typename)
isChoice (XClsInstDecl _) = Nothing
isChoice ClsInstDecl{..}
  | L _ ty <- getLHsInstDeclHead cid_poly_ty
  , HsAppTy _ (L _ cApp1) (L _ _cArgs) <- ty
  , HsAppTy _ (L _ cApp2) cName <- cApp1
  , HsAppTy _ (L _ choice) cTmpl <- cApp2
  , HsTyVar _ _ (L _ choiceClass) <- choice
  , Just (L _ choiceName) <- hsTyGetAppHead_maybe cName
  , Just (L _ tmplName) <- hsTyGetAppHead_maybe cTmpl
  , toText choiceClass == "DA.Internal.Desugar.Choice"
  || toText choiceClass == "Choice" -- temporary for generic templates
  = Just (Typename . packRdrName $ tmplName, Typename . packRdrName $ choiceName)

  | otherwise = Nothing

-- | Strip the @Instance@ suffix off of a typename, if it's there.
-- Otherwise returns 'Nothing'.
stripInstanceSuffix :: Typename -> Maybe Typename
stripInstanceSuffix (Typename t) = Typename <$> T.stripSuffix "Instance" t

-- | Get (normal) typeclass instances data. TODO: Correlate with
-- instance declarations via SrcSpan (like Haddock).
getInstanceDocs :: DocCtx -> ClsInst -> InstanceDoc
getInstanceDocs ctx ClsInst{..} =
    let ty = varType is_dfun
    in InstanceDoc
        { id_context = typeToContext ctx ty
        , id_type = typeToType ctx ty
        , id_isOrphan = isOrphan is_orphan
        }

------------------------------------------------------------
-- Generating doc.s from parsed modules


-- render type variables as text (ignore kind information)
tyVarText :: HsTyVarBndr GhcPs -> T.Text
tyVarText arg = case arg of
                  UserTyVar _ (L _ idp)         -> packRdrName idp
                  KindedTyVar _ (L _ idp) _kind -> packRdrName idp
                  XTyVarBndr _
                    -> error "unexpected X thing"


-- | Converts and trims the bytestring of a doc. decl to Text.
docToText :: HsDocString -> DocText
docToText = DocText . T.strip . T.unlines . go . T.lines . T.pack . unpackHDS
  where
    -- For a haddock comment of the form
    --
    -- -- | First line
    -- --   second line
    -- --   third line
    --
    -- we strip all whitespace from the first line and then on the following
    -- lines we strip at most as much whitespace as we find on the next
    -- non-whitespace line. In the example above, this would result
    -- in the string "First line\nsecond line\n third line".
    -- Trailing whitespace is always stripped.
    go :: [T.Text] -> [T.Text]
    go [] = []
    go (x:xs) = case span isWhitespace xs of
      (_, []) -> [T.strip x]
      (allWhitespace, ls@(firstNonWhitespace : _)) ->
        let limit = T.length (T.takeWhile isSpace firstNonWhitespace)
        in T.strip x : map (const "") allWhitespace ++ map (stripLine limit ) ls
    isWhitespace = T.all isSpace
    stripLine limit = T.stripEnd . stripLeading limit
    stripLeading limit = T.pack . map snd . dropWhile (\(i, c) -> i < limit && isSpace c) . zip [0..] . T.unpack

-- | Turn an Id into Text by taking the unqualified name it represents.
packId :: Id -> T.Text
packId = packName . idName

-- | Turn a Name into Text by taking the unqualified name it represents.
packName :: Name -> T.Text
packName = packOccName . nameOccName

-- | Turn an OccName into Text by taking the unqualified name it represents.
packOccName :: OccName -> T.Text
packOccName = T.pack . occNameString

-- | Turn a RdrName into Text by taking the unqualified name it represents.
packRdrName :: RdrName -> T.Text
packRdrName = packOccName . rdrNameOcc

-- | Turn a GHC Module into a Modulename. (Unlike the above functions,
-- we only ever want this to be a Modulename, so no reason to return
-- Text.)
getModulename :: Module -> Modulename
getModulename = Modulename . T.pack . moduleNameString . moduleName

---------------------------------------------------------------------

-- | Get package name from unit id.
modulePackage :: Module -> Maybe Packagename
modulePackage mod =
    case moduleUnitId mod of
        unitId@(DefiniteUnitId _) ->
            Just . Packagename . T.pack . unitIdString $ unitId
        _ -> Nothing

-- | Create an anchor from a TyCon.
tyConAnchor :: DocCtx -> TyCon -> Maybe Anchor
tyConAnchor DocCtx{..} tycon = do
    let ghcName = tyConName tycon
        name = Typename . packName $ ghcName
        mod = maybe dc_modname getModulename (nameModule_maybe ghcName)
        anchorFn
            | isClassTyCon tycon = classAnchor
            | otherwise = typeAnchor
    Just (anchorFn mod name)

-- | Create a (possibly external) reference from a TyCon.
tyConReference :: DocCtx -> TyCon -> Maybe Reference
tyConReference ctx@DocCtx{..} tycon = do
    referenceAnchor <- tyConAnchor ctx tycon
    let ghcName = tyConName tycon
        referencePackage = do
            guard (not (nameIsHomePackage dc_ghcMod ghcName))
            mod <- nameModule_maybe ghcName
            modulePackage mod
    Just Reference {..}

-- | Extract a potentially qualified typename from a TyCon.
tyConTypename :: DocCtx -> TyCon -> Typename
tyConTypename DocCtx{..} tycon =
    let ExtractOptions{..} = dc_extractOptions
        ghcName = tyConName tycon
        qualify =
            case eo_qualifyTypes of
                QualifyTypesAlways -> True
                QualifyTypesInPackage -> nameIsHomePackageImport dc_ghcMod ghcName
                QualifyTypesNever -> False

        moduleM = guard qualify >> nameModule_maybe ghcName
        modNameM = getModulename <$> moduleM
        simplifyModName
            | eo_simplifyQualifiedTypes = dropCommonModulePrefix dc_modname
            | otherwise = id
        prefix = maybe "" ((<> ".") . unModulename . simplifyModName) modNameM
    in Typename (prefix <> packName ghcName)

-- | Drop common module name prefix, returning the second module name
-- sans the module prefix it has in common with the first module name.
-- This will not return an empty module name however (unless given an
-- empty module name to start).
--
-- This function respects the atomicity of the module names between
-- periods. For instance @dropCommonModulePrefix "Foo.BarBaz" "Foo.BarSpam"@
-- will evaluate to @"BarSpam"@, not @"Spam"@.
dropCommonModulePrefix :: Modulename -> Modulename -> Modulename
dropCommonModulePrefix (Modulename baseMod) (Modulename targetMod) =
    Modulename . T.intercalate "." $
        aux (T.splitOn "." baseMod) (T.splitOn "." targetMod)
  where
    aux :: [T.Text] -> [T.Text] -> [T.Text]
    aux _ [x] = [x]
    aux (x:xs) (y:ys) | x == y = aux xs ys
    aux _ p = p

---------------------------------------------------------------------

-- | Extract context from GHC type. Returns Nothing if there are no constraints.
typeToContext :: DocCtx -> TyCoRep.Type -> Maybe DDoc.Type
typeToContext dc ty =
    let ctx = typeToConstraints dc ty
    in guard (notNull ctx) >> Just (TypeTuple ctx)

-- | Extract constraints from GHC type, returning list of constraints.
typeToConstraints :: DocCtx -> TyCoRep.Type -> [DDoc.Type]
typeToConstraints dc = \case
    FunTy a@(TyConApp tycon _) b | isClassTyCon tycon ->
        typeToType dc a : typeToConstraints dc b
    FunTy _ b ->
        typeToConstraints dc b
    ForAllTy _ b -> -- TODO: I think forall can introduce constraints?
        typeToConstraints dc b
    _ -> []


-- | Convert GHC Type into a damldoc type, ignoring constraints.
typeToType :: DocCtx -> TyCoRep.Type -> DDoc.Type
typeToType ctx = \case
    TyVarTy var -> TypeApp Nothing (Typename $ packId var) []

    TyConApp tycon bs | isTupleTyCon tycon ->
        TypeTuple (map (typeToType ctx) bs)

    TyConApp tycon [b] | "[]" == packName (tyConName tycon) ->
        TypeList (typeToType ctx b)

    -- Special case for unsaturated (->) to remove the levity arguments.
    TyConApp tycon (_:_:bs) | isFunTyCon tycon ->
        TypeApp
            Nothing
            (Typename "->")
            (map (typeToType ctx) bs)

    TyConApp tycon bs ->
        TypeApp
            (tyConReference ctx tycon)
            (tyConTypename ctx tycon)
            (map (typeToType ctx) bs)

    AppTy a b ->
        case typeToType ctx a of
            TypeApp m f bs -> TypeApp m f (bs <> [typeToType ctx b]) -- flatten app chains
            TypeFun _ -> unexpected "function type in a type app"
            TypeList _ -> unexpected "list type in a type app"
            TypeTuple _ -> unexpected "tuple type in a type app"
            TypeLit _ -> unexpected "type-level literal in a type app"

    -- ignore context
    ForAllTy _ b -> typeToType ctx b
    FunTy (TyConApp tycon _) b | isClassTyCon tycon ->
        typeToType ctx b

    FunTy a b ->
        case typeToType ctx b of
            TypeFun bs -> TypeFun (typeToType ctx a : bs) -- flatten function types
            b' -> TypeFun [typeToType ctx a, b']

    CastTy a _ -> typeToType ctx a
    LitTy x -> TypeLit (toText x)
    CoercionTy _ -> unexpected "coercion" -- TODO?

  where
    -- | Unhandled case.
    unexpected x = error $ "typeToType: found an unexpected " <> x


---- HACK ZONE --------------------------------------------------------

-- Generic ppr for various things we need as text.
-- FIXME Replace by specialised functions for the particular things.
toText :: Out.Outputable a => a -> T.Text
toText = T.pack . Out.showSDocOneLine DF.unsafeGlobalDynFlags . Out.ppr
