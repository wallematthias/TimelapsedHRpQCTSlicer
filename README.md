# TimelapsedHRpQCT Slicer Extension

![TimelapsedHRpQCTSlicer logo](resources/TimelapsedHRpQCTSlicer.png)

3D Slicer scripted extension for running and reviewing the `timelapsed-hrpqct` pipeline.

## Features

- Dataset parse with session table and clear error guidance.
- One-click pipeline actions:
  - `Run Full`
  - `Run Masks`
  - `Run Timelapse`
  - `Run + Multistack`
  - `Run Analysis` (analysis rerun with updated parameters)
- Smart reuse of existing outputs (import, masks, registration, analysis) through pipeline skip logic.
- Processed data loading for:
  - `raw`
  - `transformed`
  - `remodelling image`
- Segmentation-aware loading and remodelling 3D preview controls.
- In-module dependency install/update button for `timelapsed-hrpqct`.

## Exposed Settings

### Mask generation

- Method: `adaptive` or `global`
- Lower threshold
- Higher threshold

### Registration

- Metric: `mattes` or `correlation`
- Sampling percentage (timelapse + multistack correction)
- Number of resolutions (timelapse + multistack correction)
- Number of iterations (timelapse + multistack correction)

### Analysis

- Threshold
- Cluster size

## Installation (Developer Mode)

1. Open Slicer.
2. Go to `Edit -> Application Settings -> Modules`.
3. Add module path:
   - `<repo>/TimelapsedHRpQCTSlicer/TimelapsedHRpQCT`
4. Restart Slicer.
5. Open module `TimelapsedHRpQCT`.

## Runtime Dependency

The module installs/updates `timelapsed-hrpqct` inside Slicer Python using the built-in button.

## Official Extension Readiness

This repository now includes:

- Extension metadata in `CMakeLists.txt`
- Icon wiring for module resources
- Scripted smoke tests (`TimelapsedHRpQCTTest`)
- Release notes scaffold (`CHANGELOG.md`)

Before first public extension submission, complete:

1. Replace placeholder screenshot URLs with real UI screenshots.
2. Keep this README in sync with any UI changes.
3. Tag a release in this repository.
4. Submit to the Slicer Extensions Index.

## License

AGPL-3.0-only
