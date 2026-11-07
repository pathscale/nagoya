# Cargo publishing

Set the package version in `Cargo.toml`; `publish = ["crates-io"]` is the
explicit registry allowlist. Publishing is a manual workflow dispatch on
`master`, which runs `cargo publish --registry crates-io` directly.

Before dispatch, run the applicable local checks, including
`cargo publish --dry-run --registry crates-io`. The workflow publishes the
root package version from the manifest; it does not run remote tests or infer
a version from a tag.
