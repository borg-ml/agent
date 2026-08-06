# Contributing

Thank you for improving Borg CLI.

## Contributor Licence Agreement

Borg requires an executed Contributor Licence Agreement (CLA) before accepting
a contribution. The CLA must:

- preserve availability of the contributed code under the MIT licence; and
- grant Borg the rights needed to distribute the contribution as part of Borg.

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

Keep protocol and provider contracts in their owning crate, and keep changes
small enough to review and validate independently.
