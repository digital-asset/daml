resource "google_compute_global_address" "default" {
  project    = "da-dev-gcp-daml-language"
  name       = "daml-bazel-cache-address"
  ip_version = "IPV4"
}

output external_ip {
  description = "The external IP assigned to the global fowarding rule."
  value       = "${google_compute_global_address.default.address}"
}
