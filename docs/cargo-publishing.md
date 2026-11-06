# Cargo publishing

Edit the package version in `Cargo.toml`; its `publish` allowlist selects the
permitted registry. The `Publish crate` workflow runs manually on `master` and
requires the exact reviewed commit plus confirmation of completed local checks.

Before publishing, run the affected local checks, including:

- `cargo test --release`
- `cargo check --no-default-features` (preserves the no_std release requirement)
- `cargo publish --dry-run --registry crates-io`

The workflow does not run the remote test suite. Its helper rejects a dirty
checkout, a mismatched commit, or a manifest without explicit crates.io
eligibility. Existing versions are skipped; registry outages fail rather than
being interpreted as an unpublished version. Publishing to another permitted
registry is a separate operation with its own credentials and verification.
