# Contributing

Contributions are welcome to the Total Perspective Vortex library. To help with
code consistency across the Galaxy project ecosystem, TPV adheres to the same
contribution guidelines as the Galaxy application.

For details, please look at [Galaxy Contributing
document](https://github.com/galaxyproject/galaxy/blob/dev/CONTRIBUTING.md).

## Governance and maintainership

TPV and the shared tool resource registry are maintained by Galaxy community
maintainers, with a designated set of registry maintainers responsible for
curation of registry entries.

All substantive changes are proposed through pull requests and reviewed before
merge by maintainers; changes that affect deployment behavior should include
input from Galaxy deployment operators.

Galaxy deployment operators are encouraged to participate by opening issues,
submitting pull requests, and reviewing/testing proposals against real-world
infrastructure constraints.

Registry recommendations are accepted via pull request after maintainer review,
with preference for recommendations supported by production evidence,
benchmarking data, or operator validation.

## Releasing

TPV is published to PyPI by the [deploy workflow](.github/workflows/deploy.yaml)
using PyPI [trusted publishing](https://docs.pypi.org/trusted-publishers/), so
there is no API token and no manual `twine upload`. Publishing a GitHub Release
publishes to PyPI. Pushing a tag only builds and checks the distributions; there
is no TestPyPI stage.

1. Make sure the `tests` workflow passes on `main`.

2. Set the new version in `pyproject.toml`, following
   [semver](https://semver.org/). Dependencies are not pinned for a release:
   they are general-purpose libraries and are kept as broad as possible.

3. Add a section to the top of `CHANGELOG.rst`, in the same format as the
   existing ones:

   ```rst
   X.Y.Z - Mon D, YYYY. (sha <full sha of the main commit being released>)
   --------------------------------------------------------------------
   * What changed, described for a Galaxy admin by @author (PR #123)
   ```

   The merged pull requests are listed by
   `git log --first-parent --oneline vPREVIOUS..origin/main`.

4. Build the distributions and check them. The file names must show the new
   version: 3.2.1 had to be re-tagged because its version bump was missed.

   ```bash
   rm -rf dist && uv build && uvx twine check dist/*
   ```

5. Commit and push to `main`. This needs a maintainer who can bypass the
   pull-request rule on `main`. Then wait for the `tests` workflow to pass on
   the new commit.

   ```bash
   git commit -am "Update changelog and increment version to X.Y.Z"
   git push origin main
   ```

6. Create and publish a GitHub Release. Publishing it creates the `vX.Y.Z` tag
   on the target commit and triggers the deploy workflow, which reads
   `deploy.yaml` from that commit and publishes to PyPI.

   ```bash
   gh release create vX.Y.Z --target <release commit sha> --title vX.Y.Z \
     --notes-file highlights.md --generate-notes --notes-start-tag vPREVIOUS
   ```

   `--generate-notes` adds GitHub's "What's Changed" list after the contents of
   `highlights.md`. Use that file for a short "Release Highlights" section on
   anything a Galaxy admin should know about, or leave out `--notes-file` when
   there is nothing to highlight.

7. Confirm the upload at https://pypi.org/project/total-perspective-vortex/.
