# Third-party and generated-file exceptions

Rust dependencies are not Borg-authored. Their licence and copyright metadata
is supplied by their respective packages and source repositories; Cargo's
lockfile is an inventory, not a relicensing statement.

`LICENSE` is the unmodified GNU Affero General Public License version 3 text
published by the Free Software Foundation. Its notice remains intact.

Generated TypeScript bindings under `bindings/` are build/test outputs and are
excluded from source distributions. Provider transcripts, prompts, session
journals, credentials, product assets, and customer data do not belong in this
repository. If generated source is intentionally distributed later, it must
identify its generator and source licence in the file or an adjacent notice.
Third-party files must retain their upstream notices and must not carry a Borg
SPDX copyright claim unless Borg actually owns that file.
