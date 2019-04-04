#!/usr/bin/env groovy

@Library('da-jenkins-build-helpers@v0.76') _

import groovy.transform.Field

// Disable Jenkins linter for now until its version is updated (see DEL-6655)
//SKIP_UNIT_TESTS SKIP_LINTER

@Field final String context_linux = 'daml/bazel/linux'
@Field final String context_macos = 'daml/bazel/macos'
@Field final String context_windows = 'daml/bazel/windows'
@Field final String notificationChannel = '#team-daml'

@Field final String releaseDir = '--release-dir /tmp/sdk-release'
@Field final String slackReleaseMessageFile = 'slack-release-message'

@Field boolean shouldRunTests = true

def status = ['windows': false, 'linux': false, 'macos': false];

def bazelRemoteCacheConfig = 'cat "$GOOGLE_CREDENTIALS" > google-credentials.json && echo "build --remote_http_cache=https://storage.googleapis.com/daml-bazel-cache --remote_upload_local_results=true --google_credentials=google-credentials.json" >> .bazelrc.local'

pipeline {
    agent none
    parameters {
        string(
            defaultValue: '',
            description: 'Set to a git ref if you want to run a release CI on the given ref',
            name: 'RELEASE'
        )
    }
    options {
        timestamps()
        timeout(time: 120, unit: 'MINUTES')
        buildDiscarder logRotator(numToKeepStr: onMasterBranch() ? '30' : '4')
        skipDefaultCheckout false
    }
    stages {
        stage("Build") {
            parallel {
                stage("Linux") {
                    agent { label 'centos-7-2018-11-23.1-052b0c3a0c78e6b46' }
                    stages {
                        stage('Load Environment') {
                            steps {
                                gitHubNotifyStatus context: context_linux, starting: true
                                script {
                                    if (!params.RELEASE.equals('')) {
                                        nixBash "git checkout ${params.RELEASE}"
                                    }
                                }
                                sh 'git clean -xdf'
                            }
                        }
                        stage("Build @Linux") {
                            environment {
                                MANPATH = ""
                                MVN_SETTINGS = credentials('mvn-settings-for-bazel')
                                JFROG_CREDENTIALS = credentials('artifactory-da-jenkins')
                                ARTIFACTORY_CI = credentials('artifactory-da-jenkins')
                                // Needed by bazelRemoteCacheConfig
                                GOOGLE_CREDENTIALS = credentials('google-credentials')
                            }
                            steps {
                                withNetRcCredentials {
                                    withJFrogCredentials {
                                      script {
                                        // Configure the remote cache
                                        nixBash bazelRemoteCacheConfig

                                        // Temporarily added to get an execution log comparable to a local build
                                        nixBash 'bazel clean --expunge'

                                        nixBash './build.sh "_linux"'

                                        status["linux"] = true
                                      }
                                    }
                                }
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts artifacts: 'build_execution_linux.log', fingerprint: true, allowEmptyArchive: true
                            archiveArtifacts artifacts: 'test_execution_linux.log', fingerprint: true, allowEmptyArchive: true
                            archiveArtifacts artifacts: 'build_execution_linux.log', fingerprint: true, allowEmptyArchive: true
                            githubNotify context: context_linux,
                                         description: status['linux'] ? "build succeeded" : "build failed",
                                         status: status['linux'] ? 'SUCCESS' : 'FAILURE'
                        }
                        cleanup {
                            nixBash "bazel clean --expunge"
                            cleanWs()
                        }
                    }
                }
                stage("macOS") {
                    agent { label "macos-mojave" }
                    stages {
                        stage('Load Environment') {
                            steps {
                                gitHubNotifyStatus context: context_macos, starting: true
                                script {
                                    if (!params.RELEASE.equals('')) {
                                        nixBash "git checkout ${params.RELEASE}"
                                    }
                                }
                                sh 'git clean -xdf'
                            }
                        }
                        stage("Build @macOS") {
                            environment {
                                MANPATH = ""
                                MVN_SETTINGS = credentials('mvn-settings-for-bazel')
                                JFROG_CREDENTIALS = credentials('artifactory-da-jenkins')
                                ARTIFACTORY_CI = credentials('artifactory-da-jenkins')
                                // Needed by bazelRemoteCacheConfig
                                GOOGLE_CREDENTIALS = credentials('google-credentials')
                            }
                            steps {
                                withNetRcCredentials {
                                    withJFrogCredentials {
                                      script {
                                        // Configure the remote cache
                                        nixBash bazelRemoteCacheConfig

                                        nixBash 'bazel clean --expunge'
                                        nixBash 'bazel build //... --experimental_execution_log_file build_execution_macos.log'

                                        // Make sure that Bazel query works.
                                        nixBash 'bazel query "deps(//...)"'

                                        status['macos'] = true
                                      }
                                    }
                                }
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts artifacts: 'build_execution_macos.log', fingerprint: true, allowEmptyArchive: true
                            githubNotify context: context_macos,
                                         description: status['macos'] ? "build succeeded" : "build failed",
                                         status: status['macos'] ? 'SUCCESS' : 'FAILURE'
                        }
                        cleanup {
                            nixBash "bazel clean --expunge"
                            cleanWs()
                        }
                    }
                }
                stage("Windows") {
                    // Unfortunately we cannot use docker builds since we
                    // migrated to msys2, see
                    // https://github.com/docker/for-win/issues/262
                    agent { label 'windows-vm' }

                    environment {
                        MANPATH = ""
                        MVN_SETTINGS = credentials('mvn-settings-for-bazel')
                        JFROG_CREDENTIALS = credentials('artifactory-da-jenkins')
                        ARTIFACTORY_CI = credentials('artifactory-da-jenkins')
                        // Needed by bazelRemoteCacheConfig
                        GOOGLE_CREDENTIALS = credentials('google-credentials')
                    }

                    stages {
                        stage("Build @Windows") {
                            steps {
                                gitHubNotifyStatus context: context_windows, starting: true
                                script {
                                    if (!params.RELEASE.equals('')) {
                                        nixBash "git checkout ${params.RELEASE}"
                                    }
                                }
                                withNetRcCredentials {
                                    withJFrogCredentials {
                                        timeout(time: 45, unit: 'MINUTES') {
                                            script {
                                                nixBash "sed -i '1,/# this_breaks_windows_start/!{ /# this_breaks_windows_stop/,/# this_breaks_windows_start/!s/^/#/; }' WORKSPACE"

                                                nixBash 'echo "build --config windows-ci" >> .bazelrc.local'
                                                // Configure the remote cache
                                                nixBash bazelRemoteCacheConfig

                                                powershellWithDevEnv "./build.ps1"

                                                status['windows'] = true
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    post {
                        always {
                            githubNotify context: context_windows,
                                         description: status['windows'] ? "build succeeded" : "build failed",
                                         status: status['windows'] ? 'SUCCESS' : 'FAILURE'
                        }
                        cleanup {
                            powershellWithDevEnv 'bazel clean --expunge'
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            slackNotifyStatus '#team-daml'
        }
    }
}

// vim: expandtab tabstop=4 shiftwidth=4
