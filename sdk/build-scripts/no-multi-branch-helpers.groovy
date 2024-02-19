// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env groovy

/**
 * get the tag name
 * This is not the best way to do it, Jenkins should provide this info, but it isn't
 */
String getTag() {
  def revtag = sh(script: 'git describe --tags',  returnStdout: true).trim()
  return revtag
}

/**
 * get the repo name
 * This is not the best way to do it, Jenkins should provide this info, but it isn't
 */
String getRepoUrl() {
  def repourl = sh(script: 'git config --get remote.origin.url', returnStdout: true).trim()
  return repourl
}

/**
 * Sends notification to the team that the release started.
 */
def notifyReleaseStart(channel) {
  slackSend (color: 'good',
             message: "Release job for ${getTag()} (${env.BUILD_URL}) started.",
             channel: channel)
}

/**
 * Sends notification to component releases.
 * Includes a link to the changelog
 */
def notifyReleaseSuccessfull(teamChannel, anchor="") {
  def component = getTag().split("/")[1]
  def relver = getTag().split("/")[2]
  def changelog = getRepoUrl() + "/blob/${getTag()}/${component}/CHANGELOG.md${anchor}"
  slackSend (color: 'good',
             message: "New release of $component ($relver): $changelog",
             channel: '#alert-releases')
  // also notify the team, not everyone follows alert-releases
  slackSend (color: 'good',
             message: "New release of $component ($relver): $changelog",
             channel: teamChannel)
}

/**
 * Sends notification to the team that the release failed.
 */
def notifyReleaseFailure(channel) {
  slackSend (color: 'danger',
             message: "FAILED: Release job for ${getTag()} (${env.BUILD_URL})",
             channel: channel)
}

return this
