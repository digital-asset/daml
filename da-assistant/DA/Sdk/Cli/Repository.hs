-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}

module DA.Sdk.Cli.Repository
    ( module Types
    , makeHandleWithCredentials
    , makeManagementHandle
    , makeBintrayTemplateRepoHandle
    , Bintray.makeBintrayHandle

    , daExperimentalRepository
    , daRepository
    ) where

import DA.Sdk.Prelude
import DA.Sdk.Cli.Repository.Bintray as Bintray
import DA.Sdk.Cli.Conf.Types         (RepositoryURLs(..))
import DA.Sdk.Cli.Repository.Types as Types
import Servant.Client (ClientEnv(..))
import qualified Network.HTTP.Client.TLS as Client

makeHandleWithCredentials :: MonadIO m => RepositoryURLs -> Credentials -> m (Types.Handle m)
makeHandleWithCredentials repoUrls credentials = do
    mgr <- Client.newTlsManager
    h <- makeBintrayRequestHandle downloadBaseUrl (ClientEnv mgr apiBaseUrl Nothing)
    Bintray.makeBintrayHandle h repoUrls credentials
  where
    apiBaseUrl      = repositoryAPIURL repoUrls
    downloadBaseUrl = repositoryDownloadURL repoUrls

makeManagementHandle ::
       MonadIO m
    => RepositoryURLs
    -> Types.Credentials
    -> m Types.ManagementHandle
makeManagementHandle repoUrls credentials = do
    mgr <- Client.newTlsManager
    handle <- makeBintrayRequestHandle downloadBaseUrl (ClientEnv mgr apiBaseUrl Nothing)
    makeBintrayManagementHandle handle apiBaseUrl credentials
  where
    apiBaseUrl      = repositoryAPIURL repoUrls
    downloadBaseUrl = repositoryDownloadURL repoUrls

makeBintrayTemplateRepoHandle ::
       MonadIO m
    => RepositoryURLs
    -> Types.Credentials
    -> m Types.TemplateRepoHandle
makeBintrayTemplateRepoHandle repoUrls credentials = do
    mgr <- Client.newTlsManager
    h <- makeBintrayRequestHandle downloadBaseUrl (ClientEnv mgr apiBaseUrl Nothing)
    makeBintrayTemplateRepoHandleWithRequestLayer credentials Bintray.daSubject h
  where
    apiBaseUrl      = repositoryAPIURL repoUrls
    downloadBaseUrl = repositoryDownloadURL repoUrls
