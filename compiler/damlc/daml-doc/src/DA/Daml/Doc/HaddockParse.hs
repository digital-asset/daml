-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Daml.Doc.HaddockParse(mkDocs) where

import           DA.Daml.Doc.Types                 as DDoc
import Development.IDE.Types.Options (IdeOptions(..))
import Development.IDE.Core.FileStore
import qualified Development.IDE.Core.Service     as Service
import qualified Development.IDE.Core.Rules     as Service
import qualified Development.IDE.Core.RuleTypes as Service
import qualified Development.IDE.Core.OfInterest as Service
import           Development.IDE.Types.Diagnostics
import Development.IDE.Types.Logger
import Development.IDE.Types.Location
import Development.IDE.Core.Compile


import           "ghc-lib" GHC
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

    mkModuleDocs :: TcModuleResult -> ModuleDoc
    mkModuleDocs tmr =
        let ctx@DocCtx{..} = buildDocCtx (tmrModule tmr)
            typeMap = MS.fromList $ mapMaybe (getTypeDocs ctx) dc_decls
            fctDocs = mapMaybe (getFctDocs ctx) dc_decls
            clsDocs = mapMaybe (getClsDocs ctx) dc_decls
            tmplDocs = getTemplateDocs ctx typeMap
            adtDocs
                = MS.elems . MS.withoutKeys typeMap . Set.unions
                $ dc_templates : MS.elems dc_choices
        in ModuleDoc
            { md_name = dc_mod
            , md_descr = modDoc dc_tcmod
            , md_adts = adtDocs
            , md_templates = tmplDocs
            , md_functions = fctDocs
            , md_classes = clsDocs
            }

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
    { dc_mod :: Modulename
    , dc_tcmod :: TypecheckedModule
    , dc_decls :: [DeclData]
    , dc_templates :: Set.Set RdrName
    , dc_choices :: MS.Map RdrName (Set.Set RdrName)
    }

-- | Parsed declaration with associated docs.
data DeclData = DeclData
    { _dd_decl :: LHsDecl GhcPs
    , _dd_docs :: Maybe DocText
    }

buildDocCtx :: TypecheckedModule -> DocCtx
buildDocCtx dc_tcmod  =
  let dc_mod
          = Modulename . T.pack . moduleNameString . moduleName
          . ms_mod . pm_mod_summary . tm_parsed_module $ dc_tcmod
      dc_decls
          = map (uncurry DeclData) . collectDocs . hsmodDecls . unLoc
          . pm_parsed_source . tm_parsed_module $ dc_tcmod
      (dc_templates, dc_choices)
          = getTemplateData . tm_parsed_module $ dc_tcmod

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
                Ex.ExceptT [FileDiagnostic] IO [TcModuleResult]
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
getFctDocs _ (DeclData decl docs) = do
  (name, mbType) <- case unLoc decl of
    SigD _ (TypeSig _ (L _ n :_) t) ->
      Just (n, Just . hsib_body . hswc_body $ t)
    SigD _ (ClassOpSig _ _ (L _ n :_) t) ->
      Just (n, Just . hsib_body $ t)
    ValD _ FunBind{..} | not (null docs) ->
      Just (unLoc fun_id, Nothing)
      -- NB assuming we do _not_ have a type signature for the function in the
      -- pairs (otherwise we'll get a duplicate)
    _ ->
      Nothing
  Just $ FunctionDoc
    { fct_name = Fieldname (idpToText name)
    , fct_context = hsTypeToContext =<< mbType
    , fct_type = fmap hsTypeToType mbType
    , fct_descr = docs
    }

getClsDocs :: DocCtx -> DeclData -> Maybe ClassDoc
getClsDocs ctx (DeclData (L _ (TyClD _ c@ClassDecl{..})) docs) = Just ClassDoc
      {cl_name = Typename . idpToText $ unLoc tcdLName
      ,cl_descr = docs
      ,cl_super = case unLoc tcdCtxt of
        [] -> Nothing
        xs -> Just $ TypeTuple $ map hsTypeToType xs
      ,cl_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
      ,cl_functions = concatMap f tcdSigs
      }
  where
    f :: LSig GhcPs -> [FunctionDoc]
    f (L dloc (ClassOpSig p b names n)) = catMaybes
      [ getFctDocs ctx (DeclData (L dloc (SigD noExt (ClassOpSig p b [L loc name] n))) (MS.lookup name subdocs))
      | L loc name <- names
      ]
    f _ = []
    subdocs = memberDocs c
getClsDocs _ _ = Nothing

getTypeDocs :: DocCtx -> DeclData -> Maybe (RdrName, ADTDoc)
getTypeDocs _ (DeclData (L _ (TyClD _ decl)) doc)
  | XTyClDecl{} <- decl =
      Nothing
  | ClassDecl{} <- decl =
      Nothing
  | FamDecl{}   <- decl =
      Nothing

  | SynDecl{..} <- decl =
      let name = idpToText $ unLoc tcdLName
      in Just . (unLoc tcdLName,) $ TypeSynDoc
         { ad_name = Typename name
         , ad_descr = doc
         , ad_args = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
         , ad_rhs  = hsTypeToType tcdRhs
         }

  | DataDecl{..} <- decl =
      let name = idpToText $ unLoc tcdLName
      in Just . (unLoc tcdLName,) $ ADTDoc
         { ad_name =  Typename name
         , ad_args   = map (tyVarText . unLoc) $ hsq_explicit tcdTyVars
         , ad_descr  = doc
         , ad_constrs = map constrDoc . dd_cons $ tcdDataDefn
         }
  where
    constrDoc :: LConDecl GhcPs -> ADTConstr
    constrDoc (L _ con) =
          case con_args con of
            PrefixCon args ->
              PrefixC { ac_name = Typename . idpToText . unLoc $ con_name con
                      , ac_descr = fmap (docToText . unLoc) $ con_doc con
                      , ac_args = map hsTypeToType args
                      }
            InfixCon l r ->
              PrefixC { ac_name = Typename . idpToText . unLoc $ con_name con
                      , ac_descr = fmap (docToText . unLoc) $ con_doc con
                      , ac_args = map hsTypeToType [l, r]
                      }
            RecCon (L _ fs) ->
              RecordC { ac_name  = Typename. idpToText . unLoc $ con_name con
                      , ac_descr = fmap (docToText . unLoc) $ con_doc con
                      , ac_fields = mapMaybe (fieldDoc . unLoc) fs
                      }

    fieldDoc :: ConDeclField GhcPs -> Maybe FieldDoc
    fieldDoc ConDeclField{..} =
      Just $ FieldDoc
      { fd_name = Fieldname . T.concat . map (toText . unLoc) $ cd_fld_names -- FIXME why more than one?
      , fd_type = hsTypeToType cd_fld_type
      , fd_descr = fmap (docToText . unLoc) cd_fld_doc
      }
    fieldDoc XConDeclField{}  = Nothing
getTypeDocs _ _other = Nothing

getTemplateDocs :: DocCtx -> MS.Map RdrName ADTDoc -> [TemplateDoc]
getTemplateDocs DocCtx{..} typeMap = map mkTemplateDoc $ Set.toList dc_templates
  where
    -- The following functions use the type map and choice map in scope, so
    -- defined internally, and not expected to fail on consistent arguments.
    mkTemplateDoc :: IdP GhcPs -> TemplateDoc
    mkTemplateDoc name = TemplateDoc
      { td_name  = ad_name tmplADT
      , td_descr = ad_descr tmplADT
      , td_payload = getFields tmplADT
      -- assumes exactly one record constructor (syntactic, template syntax)
      , td_choices = map mkChoiceDoc choices
      }
      where
        tmplADT = asADT name
        choices = Set.toList . fromMaybe Set.empty $ MS.lookup name dc_choices

    mkChoiceDoc :: IdP GhcPs -> ChoiceDoc
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
      where dummyDT = ADTDoc { ad_name    = Typename $ "External:" <> idpToText n
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


-- | Extracts all names of Template instances defined in a module and a map of
-- template to set of its choices (instances of Choice with a particular
-- template).
getTemplateData :: ParsedModule -> (Set.Set RdrName, MS.Map RdrName (Set.Set RdrName))
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
isTemplate :: ClsInstDecl GhcPs -> Maybe RdrName
isTemplate (XClsInstDecl _) = Nothing
isTemplate ClsInstDecl{..}
  | HsIB _ (L _ ty) <-  cid_poly_ty
  , HsAppTy _ (L _ t1) (L _ t2) <- ty
  , HsTyVar _ _ (L _ tmplClass) <- t1
  , HsTyVar _ _ (L _ tmplName) <- t2
  , toText tmplClass == "DA.Internal.Desugar.Template"
  = Just tmplName

  | otherwise = Nothing

-- | If the given instance declaration is declaring a template choice instance,
--   return template and choice name (IdP). Used to build the set of choices
--   per template declared in a module.
isChoice :: ClsInstDecl GhcPs -> Maybe (IdP GhcPs, IdP GhcPs)
isChoice (XClsInstDecl _) = Nothing
isChoice ClsInstDecl{..}
  | HsIB _ (L _ ty) <-  cid_poly_ty
  , HsAppTy _ (L _ cApp1) (L _ _cArgs) <- ty
  , HsAppTy _ (L _ cApp2) (L _ cName) <- cApp1
  , HsAppTy _ (L _ choice) (L _ cTmpl) <- cApp2
  , HsTyVar _ _ (L _ choiceClass) <- choice
  , HsTyVar _ _ (L _ choiceName) <- cName
  , HsTyVar _ _ (L _ tmplName) <- cTmpl
  , toText choiceClass == "DA.Internal.Desugar.Choice"
  = Just (tmplName, choiceName)

  | otherwise = Nothing


------------------------------------------------------------
-- Generating doc.s from parsed modules

-- render type variables as text (ignore kind information)
tyVarText :: HsTyVarBndr GhcPs -> T.Text
tyVarText arg = case arg of
                  UserTyVar _ (L _ idp)         -> idpToText idp
                  KindedTyVar _ (L _ idp) _kind -> idpToText idp
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

-- | show a parsed ID (IdP GhcPs == RdrName) as a string
idpToText :: IdP GhcPs -> T.Text
idpToText = T.pack . Out.showSDocUnsafe . Out.ppr


---------------------------------------------------------------------

hsTypeToContext :: LHsType GhcPs -> Maybe DDoc.Type
hsTypeToContext (L _ HsQualTy{..}) = case unLoc hst_ctxt of
    [] -> Nothing
    xs -> Just $ TypeTuple $ map hsTypeToType xs
hsTypeToContext _ = Nothing


hsTypeToType :: LHsType GhcPs -> DDoc.Type
hsTypeToType (L _ t) = hsTypeToType_ t

hsTypeToType_ :: HsType GhcPs -> DDoc.Type
hsTypeToType_ t = case t of

  -- drop context things
  HsForAllTy{..} -> hsTypeToType hst_body
  HsQualTy {..} -> hsTypeToType hst_body
  HsBangTy _ _b ty -> hsTypeToType ty

  -- drop comments (we might want to re-add those at some point)
  HsDocTy _ ty _doc -> hsTypeToType ty

  -- special tuple syntax
  HsTupleTy _ _con tys -> TypeTuple $ map hsTypeToType tys

  -- GHC specials. FIXME deal with them specially
  HsRecTy _ _flds -> TypeApp Nothing (Typename $ toText t) [] -- FIXME pprConDeclFields flds

  HsSumTy _ _tys -> undefined -- FIXME tupleParens UnboxedTuple (pprWithBars ppr tys)


  -- straightforward base case
  HsTyVar _ _ (L _ name) -> TypeApp Nothing (Typename $ idpToText name) []

  HsFunTy _ ty1 ty2 -> case hsTypeToType ty2 of
    TypeFun as -> TypeFun $ hsTypeToType ty1 : as
    ty22 -> TypeFun [hsTypeToType ty1, ty22]

  HsKindSig _ ty _kind -> hsTypeToType ty

  HsListTy _ ty -> TypeList (hsTypeToType ty)

  HsIParamTy _ _n ty -> hsTypeToType ty
  -- currently bailing out when we meet promoted structures
  HsSpliceTy _ _s     -> unexpected "splice"
  HsExplicitListTy _ _ _tys ->  unexpected "explicit list"
  HsExplicitTupleTy _ _tys  -> unexpected "explicit tuple"

  HsTyLit _ ty      -> TypeApp Nothing (Typename $ toText ty) []
  -- kind things. Can be printed, not sure why we would
  HsWildCardTy {}   -> TypeApp Nothing (Typename "_") []
  HsStarTy _ _      -> TypeApp Nothing (Typename "*") []

  HsAppTy _ fun_ty arg_ty ->
    case hsTypeToType fun_ty of
      TypeApp m f as -> TypeApp m f $ as <> [hsTypeToType arg_ty]  -- flattens app chains
      TypeFun _ -> unexpected "function type in a type app"
      TypeList _   -> unexpected "list type in a type app"
      TypeTuple _   -> unexpected "tuple type in a type app"

  HsAppKindTy _ _ _ -> unexpected "kind application"

  HsOpTy _ ty1 (L _ op) ty2 ->
    TypeApp Nothing (Typename $ toText op) [ hsTypeToType ty1, hsTypeToType ty2 ]

  HsParTy _ ty  -> hsTypeToType ty

  XHsType _t -> unexpected "XHsType"

  where unexpected x = error $ "hsTypeToType: found an unexpected " <> x

---- HACK ZONE --------------------------------------------------------

-- Generic ppr for various things we need as text.
-- FIXME Replace by specialised functions for the particular things.
toText :: Out.Outputable a => a -> T.Text
toText = T.pack . Out.showSDocOneLine DF.unsafeGlobalDynFlags . Out.ppr
