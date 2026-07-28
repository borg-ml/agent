# Contributing

Thank you for improving Borg CLI.

## Contributor Licence Agreement

Borg requires an executed Contributor Licence Agreement (CLA) before accepting
a contribution. The CLA must:

- preserve availability of the contributed code under AGPL-3.0-only; and
- grant Borg sufficient rights to sublicense and relicense the contribution,
  including as part of separately licensed commercial distributions.

This checked-in document is policy, **not a signing mechanism**, and opening a
pull request does not by itself execute a CLA. A maintainer must provide and
confirm Borg's approved signing process before a contribution can be merged.
The final CLA text and signing workflow require Borg's software-licensing
counsel.

Contributors must have the right to submit their work. Do not submit customer
data, credentials, generated model transcripts, or third-party code whose
licence is incompatible with the repository.

## Development

```sh
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
```

Keep public protocol and provider contracts in their owning crate. Private Borg
platform code depends on this repository and must not be copied into it.
