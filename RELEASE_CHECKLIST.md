# Release Checklist (Slicer Extension)

## Metadata

- [ ] `CMakeLists.txt` homepage points to this repository.
- [ ] `EXTENSION_ICONURL` set to public raw URL.
- [ ] `EXTENSION_SCREENSHOTURLS` set to real screenshots.
- [ ] Extension description/category/contributors verified.

## Module

- [ ] Module loads in Slicer developer mode.
- [ ] `Install / Update timelapsed-hrpqct` works on clean Slicer install.
- [ ] Full pipeline run works on representative dataset.
- [ ] Analyze rerun works with changed threshold/cluster.
- [ ] Raw/transformed/remodelling loading works.

## Testing

- [ ] `TimelapsedHRpQCTTest` passes.
- [ ] Manual smoke test done on at least one real dataset.

## Docs

- [ ] README reflects current UI labels and workflow.
- [ ] Changelog updated for release version.
- [ ] Troubleshooting notes include config/dependency tips.

## Publish

- [ ] Tag release (e.g. `v0.1.0`).
- [ ] Submit/update Slicer Extensions Index entry.
