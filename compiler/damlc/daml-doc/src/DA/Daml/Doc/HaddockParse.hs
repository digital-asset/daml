-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Doc.HaddockParse(mkDocs) where

import DA.Daml.Doc.Types as DDoc
import DA.Daml.Doc.Anchor as DDoc
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
import           "ghc-lib-parser" TyCoRep
import           "ghc-lib-parser" TyCon
import           "ghc-lib-parser" ConLike
import           "ghc-lib-parser" DataCon
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

-- | Parse, and process documentation in, a dependency graph of modules.
mkDocs
    :: IdeOptions
    -> [NormalizedFilePath]
    -> Ex.ExceptT [FileDiagnostic] IO [ModuleDoc]
mkDocs opts fp = do
    modules <- haddockParse opts fp
    pure $ map mkModuleDocs modules

  where
    modDoc :: TypecheckedModule -> Maybe DocText
    modDoc
        = fmap (docToText . unLoc)
        . hsmodHaddockModHeader . unLoc
        . pm_parsed_source . tm_parsed_module

    mkModuleDocs :: Service.TcModuleResult -> ModuleDoc
    mkModuleDocs tmr =
        let ctx@DocCtx{..} = buildDocCtx (Service.tmrModule tmr)
            typeMap = MS.fromList $ mapMaybe (getTypeDocs ctx) dc_decls
            classDocs = mapMaybe (getClsDocs ctx) dc_decls

            (md_classes, templateInstanceClasses) =
                partitionEithers . flip map classDocs $ \classDoc ->
                    case stripInstanceSuffix (cl_name classDoc) of
                        Nothing -> Left classDoc
                        Just templateName -> Right (templateName, classDoc)
            templateInstanceMap = MS.fromList templateInstanceClasses

            md_name = dc_modname
            md_anchor = Just (moduleAnchor md_name)
            md_descr = modDoc dc_tcmod
            md_templates = getTemplateDocs ctx typeMap templateInstanceMap
            md_templateInstances = []
            md_functions = mapMaybe (getFctDocs ctx) dc_decls
            md_adts
                = MS.elems . MS.withoutKeys typeMap . Set.unions
                $ dc_templates : MS.keysSet dc_templateInstances : MS.elems dc_choices

        in ModuleDoc {..}

-- | Return the names for a given signature.
-- Equivalent of Haddock’s Haddock.GhcUtils.sigNameNoLoc.
sigNameNoLoc :: Sig name -> [IdP name]
sigNameNoLoc (TypeSig    _   ns _)         = map unLoc ns
sigNameNoLoc (ClassOpSig _ _ ns _)         = map unLoc ns
sigNameNoLoc (PatSynSig  _   ns _)         = map unLoc ns
sigNameNoLoc (SpecSig    _   n _ _)        = [unLoc n]
sigNameNoLoc (InlineSig  _   n _)          = [unLoc n]
sigNameNoLoc (FixSig _ (FixitySig _ ns _)) = map unLoc ns
sigNameNoLoc _                             = []

-- | Given a class, return a map from member names to associated haddocks.
memberDocs :: TyClDecl GhcPs -> MS.Map RdrName DocText
memberDocs cls = MS.fromList . catMaybes $
    [ (name,) <$> doc | (d, doc) <- subDecls, name <- declNames (unLoc d) ]
  where
    declNames :: HsDecl GhcPs -> [IdP GhcPs]
    declNames (SigD _ d) = sigNameNoLoc d
    declNames _ = []
    -- All documentation of class members is stored in tcdDocs.
    -- To associate the docs with the correct member, we convert all members
    -- and docs to declarations, sort them by their location
    -- and then use collectDocs.
    -- This is the equivalent of Haddock’s Haddock.Interface.Create.classDecls.
    subDecls :: [(LHsDecl GhcPs, Maybe DocText)]
    subDecls = collectDocs . sortOn getLoc $ decls
    decls = docs ++ defs ++ sigs ++ ats
    docs = map (fmap (DocD noExt)) (tcdDocs cls)
    defs = map (fmap (ValD noExt)) (bagToList (tcdMeths cls))
    sigs = map (fmap (SigD noExt)) (tcdSigs cls)
    ats = map (fmap (TyClD noExt . FamDecl noExt)) (tcdATs cls)

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
    { dc_modname :: Modulename
    , dc_tcmod :: TypecheckedModule
    , dc_decls :: [DeclData]

    , dc_tycons :: MS.Map Typename TyCon
    , dc_datacons :: MS.Map Typename DataCon
    , dc_ids :: MS.Map Fieldname Id

    , dc_templates :: Set.Set Typename
    , dc_choices :: MS.Map Typename (Set.Set Typename)
        -- ^ choices per template
    , dc_templateInstances :: MS.Map Typename Typename
        -- ^ maps template instance name to generic template name
        -- (note that the generic template may be in a different module,
        -- so one should not rely on it being in dc_templates)
    }

-- | Parsed declaration with associated docs.
data DeclData = DeclData
    { _dd_decl :: LHsDecl GhcPs
    , _dd_docs :: Maybe DocText
    }

buildDocCtx :: TypecheckedModule -> DocCtx
buildDocCtx dc_tcmod  =
  let dc_modname = getModulename . ms_mod . pm_mod_summary . tm_parsed_module $ dc_tcmod
      dc_decls
          = map (uncurry DeclData) . collectDocs . hsmodDecls . unLoc
          . pm_parsed_source . tm_parsed_module $ dc_tcmod
      (dc_templates, dc_choices, dc_templateInstances)
          = getTemplateData . tm_parsed_module $ dc_tcmod

      tythings = modInfoTyThings . tm_checked_module_info $ dc_tcmod

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
  (name, keepContext) <- case unLoc decl of
    SigD _ (TypeSig _ (L _ n :_) _) ->
      Just (n, True)
    SigD _ (ClassOpSig _ _ (L _ n :_) _) ->
      Just (n, False)
    ValD _ FunBind{..} | not (null docs) ->
      Just (unLoc fun_id, True)
      -- NB assuming we do _not_ have a type signature for the function in the
      -- pairs (otherwise we'll get a duplicate)
    _ ->
      Nothing

  let fct_name = Fieldname (packRdrName name)
      mbId = MS.lookup fct_name dc_ids
      mbType = idType <$> mbId
      fct_context = guard keepContext >> mbType >>= typeToContext ctx
      fct_type = typeToType ctx <$> mbType
      fct_anchor = Just $ functionAnchor dc_modname fct_name
      fct_descr = docs
  Just FunctionDoc {..}

getClsDocs :: DocCtx -> DeclData -> Maybe ClassDoc
getClsDocs ctx@DocCtx{..} (DeclData (L _ (TyClD _ c@ClassDecl{..})) docs) = do
    let cl_name = Typename . packRdrName $ unLoc tcdLName
        tyconMb = MS.lookup cl_name dc_tycons
        cl_anchor = tyConAnchor ctx =<< tyconMb
        cl_descr = docs
        cl_functions = concatMap f tcdSigs
        cl_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
        cl_super = do
            tycon <- tyconMb
            cls <- tyConClass_maybe tycon
            let theta = classSCTheta cls
            guard (notNull theta)
            Just (TypeTuple $ map (typeToType ctx) theta)
    Just ClassDoc {..}
  where
    f :: LSig GhcPs -> [FunctionDoc]
    f (L dloc (ClassOpSig p b names n)) = catMaybes
      [ getFctDocs ctx (DeclData (L dloc (SigD noExt (ClassOpSig p b [L loc name] n))) (MS.lookup name subdocs))
      | L loc name <- names
      ]
    f _ = []
    subdocs = memberDocs c
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

      Just (ad_name, TypeSynDoc {..})

  | DataDecl{..} <- decl = do
      let ad_name = Typename . packRdrName $ unLoc tcdLName
          ad_descr = doc
          ad_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
          ad_anchor = Just $ typeAnchor dc_modname ad_name
          ad_constrs = mapMaybe constrDoc . dd_cons $ tcdDataDefn
      Just (ad_name, ADTDoc {..})
  where
    constrDoc :: LConDecl GhcPs -> Maybe ADTConstr
    constrDoc (L _ con) = do
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

        Just $ case con_args con of
            PrefixCon _ -> PrefixC {..}
            InfixCon _ _ -> PrefixC {..} -- FIXME: should probably change this!
            RecCon (L _ fs) ->
              let ac_fields = mapMaybe fieldDoc (zip ac_args fs)
              in RecordC {..}

    fieldDoc :: (DDoc.Type, LConDeclField GhcPs) -> Maybe FieldDoc
    fieldDoc (fd_type, L _ ConDeclField{..}) = do
        let fd_name = Fieldname . T.concat . map (toText . unLoc) $ cd_fld_names
            fd_anchor = Just $ functionAnchor dc_modname fd_name
            fd_descr = fmap (docToText . unLoc) cd_fld_doc
        Just FieldDoc{..}
    fieldDoc (_, L _ XConDeclField{}) = Nothing
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
                             }
    -- Assuming one constructor (record or prefix), extract the fields, if any.
    -- For choices without arguments, GHC returns a prefix constructor, so we
    -- need to cater for this case specially.
    getFields adt = case ad_constrs adt of
                      [PrefixC{}] -> []
                      [RecordC{ ac_fields = fields }] -> fields
                      [] -> [] -- catching the dummy case here, see above
                      _other -> error "getFields: found multiple constructors"



-- recognising Template and Choice instances


-- | Extracts all names of templates defined in a module, a map of template names
-- to its set of choices, and a map of template instance names to generic template
-- names for all template instances defined in the module (note, the generic template
-- need not be defined in the same module).
getTemplateData :: ParsedModule ->
    ( Set.Set Typename
    , MS.Map Typename (Set.Set Typename)
    , MS.Map Typename Typename )
getTemplateData ParsedModule{..} =
  let
    instDs    = mapMaybe (isInstDecl . unLoc) . hsmodDecls . unLoc $ pm_parsed_source
    templates = mapMaybe isTemplate instDs
    choiceMap = MS.fromListWith (<>) $
                map (second Set.singleton) $
                mapMaybe isChoice instDs
    instanceMap = MS.fromList $ mapMaybe isTemplateInstance instDs
  in
    (Set.fromList templates, choiceMap, instanceMap)
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

-- | Is this a "template instance" instance declaration? If so,
-- we want to hold on to the typename of the template instance,
-- so return that, and the corresponding template name.
--
-- Returns @(templateInstanceName, genericTemplateName)@ on success.
isTemplateInstance :: ClsInstDecl GhcPs -> Maybe (Typename, Typename)
isTemplateInstance (XClsInstDecl _) = Nothing
isTemplateInstance ClsInstDecl{..}
  | L _ ty <- getLHsInstDeclHead cid_poly_ty
  , HsAppTy _ (L _ t1) t2 <- ty
  , HsTyVar _ _ (L _ className) <- t1
  , Just (L _ instanceName) <- hsTyGetAppHead_maybe t2
  , Just genericName <- stripInstanceSuffix (Typename . packRdrName $ className)
  = Just (Typename . packRdrName $ instanceName, genericName)

  | otherwise = Nothing

-- | Strip the @Instance@ suffix off of a typename, if it's there.
-- Otherwise returns 'Nothing'.
stripInstanceSuffix :: Typename -> Maybe Typename
stripInstanceSuffix (Typename t) = Typename <$> T.stripSuffix "Instance" t

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

-- | Create an anchor from a TyCon. Don't make anchors for wired in names.
tyConAnchor :: DocCtx -> TyCon -> Maybe Anchor
tyConAnchor DocCtx{..} tycon = do
    let ghcName = tyConName tycon
        name = Typename . packName $ ghcName
        mod = maybe dc_modname getModulename (nameModule_maybe ghcName)
        anchorFn
            | isClassTyCon tycon = classAnchor
            | otherwise = typeAnchor
    Just (anchorFn mod name)

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

    TyConApp tycon bs ->
        TypeApp
            (tyConAnchor ctx tycon)
            (Typename . packName . tyConName $ tycon)
            (map (typeToType ctx) bs)

    AppTy a b ->
        case typeToType ctx a of
            TypeApp m f bs -> TypeApp m f (bs <> [typeToType ctx b]) -- flatten app chains
            TypeFun _ -> unexpected "function type in a type app"
            TypeList _ -> unexpected "list type in a type app"
            TypeTuple _ -> unexpected "tuple type in a type app"

    -- ignore context
    ForAllTy _ b -> typeToType ctx b
    FunTy (TyConApp tycon _) b | isClassTyCon tycon ->
        typeToType ctx b

    FunTy a b ->
        case typeToType ctx b of
            TypeFun bs -> TypeFun (typeToType ctx a : bs) -- flatten function types
            b' -> TypeFun [typeToType ctx a, b']

    CastTy a _ -> typeToType ctx a
    LitTy x -> TypeApp Nothing (Typename $ toText x) []
    CoercionTy _ -> unexpected "coercion" -- TODO?

  where
    -- | Unhandled case.
    unexpected x = error $ "typeToType: found an unexpected " <> x


---- HACK ZONE --------------------------------------------------------

-- Generic ppr for various things we need as text.
-- FIXME Replace by specialised functions for the particular things.
toText :: Out.Outputable a => a -> T.Text
toText = T.pack . Out.showSDocOneLine DF.unsafeGlobalDynFlags . Out.ppr
