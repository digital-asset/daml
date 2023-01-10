# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

variable "name" {
  description = "Name prefix for all the resources"
}

variable "labels" {
  description = "Labels to apply on all the resources"
  type        = map(any)
  default     = {}
}

variable "project" {
  description = "GCP project name"
}

variable "region" {
  description = "GCP region in which to create the resources"
}

variable "ssl_certificate" {
  description = "A reference to the SSL certificate, google managed or not"
}

variable "ssl_policy" {
  description = "The url of an SSL policy to use."
}

variable "cache_retention_days" {
  description = "The number of days to keep the objects around"
}
