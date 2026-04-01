# Changelog

All notable changes to this extension are documented in this file.

## [0.1.0] - 2026-03-31

### Added

- First release-ready Slicer module scaffold for TimelapsedHRpQCT workflows.
- Pipeline controls for full run, masks, timelapse, multistack, and analysis rerun.
- Remodelling segmentation loading and 3D preview tools.
- Runtime install/update flow for `timelapsed-hrpqct`.
- Module icon wiring and extension metadata setup.
- Scripted smoke tests (`TimelapsedHRpQCTTest`).

### Changed

- UI compacted for better 2D/3D viewer space.
- Improved process logging/cancellation behavior and stage status handling.
- Robust config resolution with fallback when packaged defaults are missing.
- Improved artifact lookup fallback for remodelling load paths.

### Fixed

- QProcess output decode handling across Qt/Python bindings.
- Process-finished callback signature compatibility.
- Analysis stage status completion behavior in full pipeline runs.
- Forced reinstall behavior for dependency update button.
