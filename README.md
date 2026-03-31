# TimelapsedHRpQCT 3D Slicer Add-on (Prototype)

This repository provides a 3D Slicer scripted module GUI wrapper around `timelapsed-hrpqct`.

## Current Scope

- Drag/drop or browse dataset root
- Parse input and show clear filename guidance on parse failures
- Run buttons:
  - `Run mask generation`
  - `Run timelapse` (regular branch)
  - `Run multistack`
  - `Analyze` (re-runs analysis)
- Processed patient picker + data type loader (`raw`, `transformed`, `remodelling image`)
- Exposed settings:
  - Mask method (`adaptive`/`global`) + lower/higher threshold
  - Registration metric (`mattes`/`correlation`) + sampling percentage
  - Timelapse resolutions + iterations
  - Multistack resolutions + iterations
  - Analysis threshold + cluster size
  - Optional mineralisation label toggle for visualization labels

## Prerequisites

Inside Slicer Python:

```python
slicer.util.pip_install('timelapsed-hrpqct')
```

## Load In Slicer (Developer Mode)

1. In Slicer: `Edit -> Application Settings -> Modules`
2. Add module path: `<repo>/TimelapsedHRpQCTSlicer/TimelapsedHRpQCT`
3. Restart Slicer
4. Open module `TimelapsedHRpQCT`

## Notes

- This is a first functional scaffold, not yet packaged in the Slicer Extensions Index.
- Next step is to split logic/UI further and add Slicer test coverage before publishing as a formal extension.
