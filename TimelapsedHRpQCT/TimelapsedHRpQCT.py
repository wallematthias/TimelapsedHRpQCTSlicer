import os
import re
import shutil
import signal
import subprocess
import tempfile
from pathlib import Path

import numpy as np
import qt
import ctk
import slicer
import vtk
import SimpleITK as sitk
from slicer.ScriptedLoadableModule import (
    ScriptedLoadableModule,
    ScriptedLoadableModuleWidget,
    ScriptedLoadableModuleLogic,
    ScriptedLoadableModuleTest,
)

MODULE_VERSION = "0.1.0"


class TimelapsedHRpQCT(ScriptedLoadableModule):
    def __init__(self, parent):
        super().__init__(parent)
        parent.title = "TimelapsedHRpQCT"
        parent.categories = ["Bone"]
        parent.dependencies = []
        parent.contributors = ["Matthias Walle"]
        parent.helpText = (
            "GUI wrapper for timelapsed-hrpqct pipeline.\n"
            f"Module version: {MODULE_VERSION}"
        )
        parent.acknowledgementText = """Built for streamlined longitudinal HR-pQCT workflows."""


class TimelapsedHRpQCTLogic(ScriptedLoadableModuleLogic):
    def __init__(self):
        super().__init__()
        self._proc = None
        self._temp_config_path = None
        self._fallback_default_config_path = None

    def is_pipeline_available(self):
        try:
            import timelapsedhrpqct  # noqa: F401
            return True
        except Exception:
            return False

    def install_or_update_pipeline(self):
        # Force-refresh from PyPI so "Install / Update" always pulls latest.
        slicer.util.pip_install(
            "--upgrade --force-reinstall --no-cache-dir timelapsed-hrpqct"
        )

    def default_config_path(self):
        import timelapsedhrpqct
        import yaml
        from dataclasses import asdict

        package_default = Path(timelapsedhrpqct.__file__).resolve().parent / "configs" / "defaults.yml"
        if package_default.exists():
            return package_default

        # Fallback for environments where package data files were not included.
        if self._fallback_default_config_path and Path(self._fallback_default_config_path).exists():
            return Path(self._fallback_default_config_path)

        from timelapsedhrpqct.config.models import AppConfig

        default_cfg = asdict(AppConfig())
        fd, path = tempfile.mkstemp(prefix="timelapsed_default_", suffix=".yml")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(default_cfg, f, sort_keys=False)
        self._fallback_default_config_path = path
        return Path(path)

    def create_override_config(self, settings_dict):
        import yaml

        with open(self.default_config_path(), "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

        def deep_update(dst, src):
            for k, v in src.items():
                if isinstance(v, dict) and isinstance(dst.get(k), dict):
                    deep_update(dst[k], v)
                else:
                    dst[k] = v

        deep_update(cfg, settings_dict)

        fd, path = tempfile.mkstemp(prefix="timelapsed_slicer_", suffix=".yml")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f, sort_keys=False)

        self._temp_config_path = path
        return path

    def parse_input(self, root_path):
        try:
            from timelapsedhrpqct.config.loader import load_config
            from timelapsedhrpqct.dataset.discovery import discover_raw_sessions

            config = load_config(self.default_config_path())
            sessions = discover_raw_sessions(root_path, config.discovery)
            return sessions, None
        except Exception as exc:
            return [], str(exc)

    def run_cli(self, args, on_output=None, on_finished=None):
        if self._proc is not None:
            raise RuntimeError("A pipeline process is already running")
        stale = self.list_external_run_pids()
        if stale:
            raise RuntimeError(
                "Detected existing timelapsed run process(es): "
                + ", ".join(str(p) for p in stale)
                + ". Cancel stale runs first."
            )

        proc = qt.QProcess()
        proc.setProcessChannelMode(qt.QProcess.MergedChannels)
        env = qt.QProcessEnvironment.systemEnvironment()
        env.insert("PYTHONUNBUFFERED", "1")
        # Avoid loading Slicer-specific ITK ImageIO plugins (e.g., MRMLIDImageIO)
        # in the pipeline subprocess, which can cause noisy factory warnings when
        # mixed with pip-installed ITK/ITKElastix components.
        if env.contains("ITK_AUTOLOAD_PATH"):
            env.remove("ITK_AUTOLOAD_PATH")
        proc.setProcessEnvironment(env)

        def _read_output():
            raw = proc.readAll()
            # PythonQt/PySide can expose QByteArray differently across versions.
            if isinstance(raw, (bytes, bytearray)):
                data = bytes(raw)
            else:
                try:
                    data = raw.data()
                    if isinstance(data, str):
                        data = data.encode("utf-8", errors="replace")
                    else:
                        data = bytes(data)
                except Exception:
                    try:
                        data = bytes(raw)
                    except Exception:
                        data = str(raw).encode("utf-8", errors="replace")
            text = data.decode("utf-8", errors="replace")
            if on_output and text:
                on_output(text)

        def _finished(*signal_args):
            self._proc = None
            # Handle different finished signal signatures across bindings.
            if len(signal_args) >= 2:
                exit_code = int(signal_args[0])
                exit_status = signal_args[1]
            elif len(signal_args) == 1:
                exit_code = int(signal_args[0])
                exit_status = 0
            else:
                try:
                    exit_code = int(proc.exitCode())
                except Exception:
                    exit_code = 0
                try:
                    exit_status = proc.exitStatus()
                except Exception:
                    exit_status = 0
            if on_finished:
                on_finished(exit_code, exit_status)

        proc.readyRead.connect(_read_output)
        proc.finished.connect(_finished)

        python_exe = shutil.which("PythonSlicer") or shutil.which("python") or shutil.which("python3")
        if python_exe is None:
            raise RuntimeError("Could not find Python executable in Slicer environment")

        full_args = ["-m", "timelapsedhrpqct.cli"] + args
        if on_output:
            on_output(f"[process] launching: {python_exe} {' '.join(full_args)}\n")
        proc.start(python_exe, full_args)

        if not proc.waitForStarted(3000):
            raise RuntimeError("Failed to start timelapsed-hrpqct process")
        if on_output:
            try:
                on_output(f"[process] started (pid={int(proc.processId())})\n")
            except Exception:
                on_output("[process] started\n")

        self._proc = proc

    def is_running(self):
        return self._proc is not None

    def list_external_run_pids(self):
        try:
            out = subprocess.check_output(
                ["pgrep", "-f", "timelapsedhrpqct.cli run"],
                text=True,
            ).strip()
        except Exception:
            return []
        if not out:
            return []
        pids = []
        current = os.getpid()
        for line in out.splitlines():
            try:
                pid = int(line.strip())
            except Exception:
                continue
            if pid == current:
                continue
            if self._proc is not None:
                try:
                    if pid == int(self._proc.processId()):
                        continue
                except Exception:
                    pass
            pids.append(pid)
        return sorted(set(pids))

    def kill_external_runs(self):
        pids = self.list_external_run_pids()
        killed = []
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
                killed.append(pid)
            except Exception:
                pass
        return killed

    def cancel_run(self):
        if self._proc is None:
            return False
        proc = self._proc
        try:
            proc.terminate()
            if not proc.waitForFinished(1500):
                proc.kill()
                proc.waitForFinished(1500)
            return True
        except Exception:
            try:
                proc.kill()
                proc.waitForFinished(1500)
                return True
            except Exception:
                return False


class TimelapsedHRpQCTWidget(ScriptedLoadableModuleWidget):
    def setup(self):
        super().setup()
        self.logic = TimelapsedHRpQCTLogic()
        self._queued_commands = []
        self._queued_stages = []
        self._active_stage = None
        self._stage_states = {}
        self._last_parsed_sessions = []
        self._parsed_baseline_rows = []
        self._patient_keys = []
        self._sh_tree_hooks_installed = False
        self._is_full_pipeline_run = False
        self._run_includes_analysis = False
        self._updating_parse_table = False
        self._temp_input_root = None
        self._mask_method_defaults = {
            "adaptive": (100.0, 300.0),
            "global": (100.0, 300.0),
        }

        self._build_ui()
        self._load_defaults_from_pipeline_config()
        self._refresh_patient_list()
        self._set_3d_background_black()

    def _build_ui(self):
        def _cap_width(widget, width=320):
            try:
                widget.setMaximumWidth(width)
            except Exception:
                pass

        depBox = ctk.ctkCollapsibleButton()
        depBox.text = "Dependency"
        depForm = qt.QFormLayout(depBox)
        depForm.setLabelAlignment(qt.Qt.AlignRight | qt.Qt.AlignVCenter)
        self.pipelineStatusLabel = qt.QLabel()
        self.installBtn = qt.QPushButton("Install / Update timelapsed-hrpqct")
        self.checkBtn = qt.QPushButton("Check")
        self.installBtn.clicked.connect(self._on_install_pipeline)
        self.checkBtn.clicked.connect(self._on_check_pipeline)
        rowWidget = qt.QWidget()
        row = qt.QHBoxLayout(rowWidget)
        row.setContentsMargins(0, 0, 0, 0)
        row.addWidget(self.installBtn)
        row.addWidget(self.checkBtn)
        depForm.addRow("Status", self.pipelineStatusLabel)
        depForm.addRow(rowWidget)
        self.layout.addWidget(depBox)

        form = qt.QFormLayout()
        form.setLabelAlignment(qt.Qt.AlignRight | qt.Qt.AlignVCenter)

        self.inputPath = ctk.ctkPathLineEdit()
        self.inputPath.filters = ctk.ctkPathLineEdit.Dirs
        self.inputPath.setCurrentPath("")
        _cap_width(self.inputPath, 360)
        form.addRow("Dataset root", self.inputPath)

        parseBtn = qt.QPushButton("Parse input")
        parseBtn.clicked.connect(self._on_parse)
        form.addRow(parseBtn)

        self.parseSummaryLabel = qt.QLabel("Parse summary: not run")
        self.parseSummaryLabel.wordWrap = True
        form.addRow(self.parseSummaryLabel)
        self.userMessageLabel = qt.QLabel("")
        self.userMessageLabel.wordWrap = True
        self.userMessageLabel.setStyleSheet(
            "QLabel { background:#fff6db; border:1px solid #f0c36d; padding:8px; border-radius:4px; }"
        )
        self.userMessageLabel.hide()
        form.addRow(self.userMessageLabel)

        self.parseTable = qt.QTableWidget()
        self.parseTable.setColumnCount(7)
        self.parseTable.setHorizontalHeaderLabels(
            ["Subject", "Site", "Session", "Stack", "Image", "Masks", "Seg"]
        )
        self.parseTable.horizontalHeader().setStretchLastSection(False)
        self.parseTable.horizontalHeader().setSectionResizeMode(qt.QHeaderView.ResizeToContents)
        self.parseTable.setMinimumHeight(160)
        self.parseTable.setEditTriggers(
            qt.QAbstractItemView.DoubleClicked
            | qt.QAbstractItemView.EditKeyPressed
            | qt.QAbstractItemView.SelectedClicked
        )
        self.parseTable.itemChanged.connect(self._on_parse_table_item_changed)

        parseBox = ctk.ctkCollapsibleButton()
        parseBox.text = "Parse Details"
        parseBox.collapsed = True
        parseLayout = qt.QVBoxLayout(parseBox)
        parseLayout.addWidget(self.parseTable)

        quickBox = ctk.ctkCollapsibleButton()
        quickBox.text = "Quick Presets"
        quickForm = qt.QFormLayout(quickBox)
        self.presetCombo = qt.QComboBox()
        self.presetCombo.addItems(["Default", "Fast preview", "High quality"])
        _cap_width(self.presetCombo, 240)
        self.applyPresetBtn = qt.QPushButton("Apply preset")
        self.applyPresetBtn.clicked.connect(self._on_apply_preset)
        quickForm.addRow("Preset", self.presetCombo)
        quickForm.addRow(self.applyPresetBtn)

        settingsBox = ctk.ctkCollapsibleButton()
        settingsBox.text = "Advanced Settings"
        settingsBox.collapsed = True
        self.advancedSettingsBox = settingsBox
        settingsLayout = qt.QVBoxLayout(settingsBox)

        maskBox = qt.QGroupBox("Mask generation")
        maskForm = qt.QFormLayout(maskBox)

        self.maskMethod = qt.QComboBox()
        self.maskMethod.addItems(["adaptive", "global"])
        self.maskMethod.currentTextChanged.connect(self._on_mask_method_changed)
        _cap_width(self.maskMethod, 220)
        self.maskLow = ctk.ctkDoubleSpinBox()
        self.maskLow.minimum = -10000.0
        self.maskLow.maximum = 10000.0
        self.maskLow.decimals = 1
        self.maskLow.singleStep = 5.0
        self.maskLow.value = 100.0
        self.maskHigh = ctk.ctkDoubleSpinBox()
        self.maskHigh.minimum = -10000.0
        self.maskHigh.maximum = 10000.0
        self.maskHigh.decimals = 1
        self.maskHigh.singleStep = 5.0
        self.maskHigh.value = 300.0
        _cap_width(self.maskLow, 220)
        _cap_width(self.maskHigh, 220)
        maskForm.addRow("Mask method", self.maskMethod)
        maskForm.addRow("Mask lower threshold", self.maskLow)
        maskForm.addRow("Mask higher threshold", self.maskHigh)

        self.resultsRootPath = ctk.ctkPathLineEdit()
        self.resultsRootPath.filters = ctk.ctkPathLineEdit.Dirs
        self.resultsRootPath.setCurrentPath("")
        _cap_width(self.resultsRootPath, 360)
        maskForm.addRow("Results folder (optional)", self.resultsRootPath)

        registrationBox = qt.QGroupBox("Registration")
        registrationForm = qt.QFormLayout(registrationBox)

        self.regMetric = qt.QComboBox(); self.regMetric.addItems(["mattes", "correlation"])
        _cap_width(self.regMetric, 220)
        registrationForm.addRow("Registration metric", self.regMetric)
        self.tlSampling = ctk.ctkDoubleSpinBox()
        self.tlSampling.minimum = 0.00001
        self.tlSampling.maximum = 1.0
        self.tlSampling.decimals = 5
        self.tlSampling.singleStep = 0.0001
        self.tlSampling.value = 0.001
        _cap_width(self.tlSampling, 220)
        registrationForm.addRow("Timelapse sampling", self.tlSampling)

        self.tlRes = qt.QSpinBox(); self.tlRes.minimum = 1; self.tlRes.maximum = 10; self.tlRes.value = 4
        self.tlIter = qt.QSpinBox(); self.tlIter.minimum = 1; self.tlIter.maximum = 5000; self.tlIter.value = 250
        _cap_width(self.tlRes, 220)
        _cap_width(self.tlIter, 220)
        registrationForm.addRow("Timelapse resolutions", self.tlRes)
        registrationForm.addRow("Timelapse iterations", self.tlIter)

        self.msRes = qt.QSpinBox(); self.msRes.minimum = 1; self.msRes.maximum = 10; self.msRes.value = 4
        self.msIter = qt.QSpinBox(); self.msIter.minimum = 1; self.msIter.maximum = 5000; self.msIter.value = 250
        self.msSampling = ctk.ctkDoubleSpinBox()
        self.msSampling.minimum = 0.00001
        self.msSampling.maximum = 1.0
        self.msSampling.decimals = 5
        self.msSampling.singleStep = 0.0001
        self.msSampling.value = 0.005
        _cap_width(self.msSampling, 220)
        _cap_width(self.msRes, 220)
        _cap_width(self.msIter, 220)
        registrationForm.addRow("Multistack correction sampling", self.msSampling)
        registrationForm.addRow("Multistack correction resolutions", self.msRes)
        registrationForm.addRow("Multistack correction iterations", self.msIter)

        analysisBox = qt.QGroupBox("Analysis")
        analysisForm = qt.QFormLayout(analysisBox)

        self.analysisThreshold = ctk.ctkDoubleSpinBox()
        self.analysisThreshold.minimum = -10000.0
        self.analysisThreshold.maximum = 10000.0
        self.analysisThreshold.decimals = 1
        self.analysisThreshold.singleStep = 5.0
        self.analysisThreshold.value = 225.0
        self.analysisCluster = qt.QSpinBox(); self.analysisCluster.minimum = 1; self.analysisCluster.maximum = 1000; self.analysisCluster.value = 12
        _cap_width(self.analysisThreshold, 220)
        _cap_width(self.analysisCluster, 220)
        analysisForm.addRow("Threshold", self.analysisThreshold)
        analysisForm.addRow("Cluster size", self.analysisCluster)

        settingsLayout.addWidget(maskBox)
        settingsLayout.addWidget(registrationBox)
        settingsLayout.addWidget(analysisBox)

        actionLayout = qt.QGridLayout()
        self.runMasksBtn = qt.QPushButton("Generate Masks")
        self.runMasksBtn.toolTip = "Generate/recompute masks from imported stacks."
        self.runTimelapseBtn = qt.QPushButton("Timelapse Pipeline")
        self.runTimelapseBtn.toolTip = (
            "Run import + timelapse pipeline in regular mode, "
            "while skipping automatic mask generation."
        )
        self.runMultistackBtn = qt.QPushButton("Timelapse + Multistack Pipeline")
        self.runMultistackBtn.toolTip = (
            "Run import + timelapse pipeline in multistack mode, "
            "while skipping automatic mask generation."
        )
        self.runAnalysisBtn = qt.QPushButton("Run Analysis")
        self.runAnalysisBtn.toolTip = "Re-run analysis only."
        self.cancelRunBtn = qt.QPushButton("Cancel current run")
        self.cancelRunBtn.clicked.connect(self._on_cancel_run)
        self.cancelRunBtn.enabled = False
        self.cancelRunBtn.toolTip = "Cancel the currently running pipeline step."
        buttons = [
            self.runMasksBtn,
            self.runTimelapseBtn,
            self.runMultistackBtn,
            self.runAnalysisBtn,
            self.cancelRunBtn,
        ]
        for b in buttons:
            _cap_width(b, 180)
        actionLayout.addWidget(self.runMasksBtn, 0, 0)
        actionLayout.addWidget(self.runTimelapseBtn, 0, 1)
        actionLayout.addWidget(self.runMultistackBtn, 1, 0)
        actionLayout.addWidget(self.runAnalysisBtn, 1, 1)
        actionLayout.addWidget(self.cancelRunBtn, 2, 0)

        self.runMasksBtn.clicked.connect(self._on_run_masks)
        self.runTimelapseBtn.clicked.connect(self._on_run_timelapse)
        self.runMultistackBtn.clicked.connect(self._on_run_multistack)
        self.runAnalysisBtn.clicked.connect(self._on_run_analysis)

        statusBox = ctk.ctkCollapsibleButton()
        statusBox.text = "Pipeline Status"
        statusForm = qt.QFormLayout(statusBox)
        self.progressBar = qt.QProgressBar()
        self.progressBar.minimum = 0
        self.progressBar.maximum = 5
        self.progressBar.value = 0
        self.currentStepLabel = qt.QLabel("Current step: idle")
        statusForm.addRow("Progress", self.progressBar)
        statusForm.addRow("Current", self.currentStepLabel)
        self.stageLabels = {}
        for key, title in [
            ("dataset", "Dataset"),
            ("parse", "Parse"),
            ("masks", "Mask generation"),
            ("registration", "Registration"),
            ("analysis", "Analysis"),
        ]:
            lbl = qt.QLabel("")
            lbl.wordWrap = True
            self.stageLabels[key] = lbl
            statusForm.addRow(title, lbl)

        loadBox = ctk.ctkCollapsibleButton()
        loadBox.text = "Load Processed Data"
        loadForm = qt.QFormLayout(loadBox)
        self.patientCombo = qt.QComboBox()
        self.loadTypeCombo = qt.QComboBox()
        self.loadTypeCombo.addItems(
            ["raw", "transformed", "remodelling image"]
        )
        self.loadDataBtn = qt.QPushButton("Load selected")
        _cap_width(self.patientCombo, 260)
        _cap_width(self.loadTypeCombo, 260)
        _cap_width(self.loadDataBtn, 180)
        self.loadDataBtn.clicked.connect(self._on_load_selected)
        loadForm.addRow("Patient", self.patientCombo)
        loadForm.addRow("Data type", self.loadTypeCombo)
        loadForm.addRow(self.loadDataBtn)

        previewBox = ctk.ctkCollapsibleButton()
        previewBox.text = "Remodelling 3D Preview"
        previewForm = qt.QFormLayout(previewBox)
        self.remodellingFullSegCombo = qt.QComboBox()
        self.remodellingRefreshBtn = qt.QPushButton("Refresh list")
        _cap_width(self.remodellingFullSegCombo, 260)
        _cap_width(self.remodellingRefreshBtn, 120)
        self.remodellingRefreshBtn.clicked.connect(self._refresh_remodelling_full_selector)
        segRow = qt.QWidget()
        segRowLayout = qt.QHBoxLayout(segRow)
        segRowLayout.setContentsMargins(0, 0, 0, 0)
        segRowLayout.addWidget(self.remodellingFullSegCombo, 1)
        segRowLayout.addWidget(self.remodellingRefreshBtn)
        self.remodellingAxisCombo = qt.QComboBox()
        self.remodellingAxisCombo.addItems(["x", "y", "z"])
        self.remodellingAxisCombo.currentIndex = 0
        self.remodellingThicknessSpin = qt.QSpinBox()
        self.remodellingThicknessSpin.minimum = 1
        self.remodellingThicknessSpin.maximum = 512
        self.remodellingThicknessSpin.value = 15
        self.remodellingDetailSlider = ctk.ctkSliderWidget()
        self.remodellingDetailSlider.minimum = 0
        self.remodellingDetailSlider.maximum = 100
        self.remodellingDetailSlider.singleStep = 1
        self.remodellingDetailSlider.decimals = 0
        self.remodellingDetailSlider.value = 50
        self.remodellingApplyPreviewBtn = qt.QPushButton("Show remodelling in 3D")
        _cap_width(self.remodellingAxisCombo, 180)
        _cap_width(self.remodellingThicknessSpin, 180)
        _cap_width(self.remodellingDetailSlider, 260)
        _cap_width(self.remodellingApplyPreviewBtn, 220)
        self.remodellingApplyPreviewBtn.clicked.connect(self._on_update_remodelling_preview)
        previewForm.addRow("Full segmentation", segRow)
        previewForm.addRow("Axis", self.remodellingAxisCombo)
        previewForm.addRow("Thickness (vox)", self.remodellingThicknessSpin)
        previewForm.addRow("3D detail (0-100)", self.remodellingDetailSlider)
        previewForm.addRow(self.remodellingApplyPreviewBtn)

        self.logText = qt.QPlainTextEdit()
        self.logText.readOnly = True
        self.logText.setMinimumHeight(200)
        self.logText.setMaximumHeight(260)

        self.layout.addLayout(form)
        self.layout.addWidget(parseBox)
        self.layout.addWidget(quickBox)
        self.layout.addWidget(settingsBox)
        self.layout.addWidget(statusBox)
        self.layout.addLayout(actionLayout)
        self.layout.addWidget(loadBox)
        self.layout.addWidget(previewBox)
        self.layout.addWidget(self.logText)
        self.layout.addStretch(1)
        self._update_dependency_ui()
        self._set_stage_status("dataset", "pending")
        self._set_stage_status("parse", "pending")
        self._set_stage_status("masks", "pending")
        self._set_stage_status("registration", "pending")
        self._set_stage_status("analysis", "pending")
        self._update_progress_ui()

    def _dataset_root(self):
        p = self.inputPath.currentPath.strip()
        root = Path(p) if p else None
        self._set_stage_status("dataset", "done" if root is not None else "pending")
        return root

    def _imported_dataset_root(self):
        root = self._dataset_root()
        if root is None:
            return None
        # Explicit user override wins.
        override = self.resultsRootPath.currentPath.strip() if hasattr(self, "resultsRootPath") else ""
        if override:
            return Path(override)
        # If the selected folder is already a dataset root, keep it.
        if root.name == "TimelapsedHRpQCT_results":
            return root
        return root / "TimelapsedHRpQCT_results"

    def _derivatives_root(self):
        imported = self._imported_dataset_root()
        if imported is None:
            return None
        return imported / "derivatives" / "TimelapsedHRpQCT"

    def _show(self, text):
        message = text.rstrip()
        if hasattr(self, "logText") and self.logText is not None:
            self.logText.appendPlainText(message)
        else:
            print(message)

    def _set_user_message(self, level, title, body):
        palette = {
            "info": ("#eaf2ff", "#7ea6f7"),
            "warn": ("#fff6db", "#f0c36d"),
            "error": ("#ffeceb", "#e68a87"),
            "success": ("#eaf8ea", "#8aca8a"),
        }
        bg, border = palette.get(level, palette["info"])
        self.userMessageLabel.setStyleSheet(
            f"QLabel {{ background:{bg}; border:1px solid {border}; padding:8px; border-radius:4px; }}"
        )
        self.userMessageLabel.setText(f"<b>{title}</b><br>{body}")
        self.userMessageLabel.show()

    def _clear_user_message(self):
        self.userMessageLabel.hide()
        self.userMessageLabel.setText("")

    def _set_stage_status(self, stage_key, status):
        if stage_key not in self.stageLabels:
            return
        self._stage_states[stage_key] = status
        style = {
            "pending": ("●", "#888888", "Pending"),
            "running": ("●", "#2f7ed8", "Running"),
            "done": ("●", "#2d9a4b", "Done"),
            "error": ("●", "#c73a3a", "Needs attention"),
        }
        dot, color, label = style.get(status, style["pending"])
        self.stageLabels[stage_key].setText(f"<span style='color:{color}; font-weight:700'>{dot}</span> {label}")
        self._update_progress_ui()

    def _update_progress_ui(self):
        order = ["dataset", "parse", "masks", "registration", "analysis"]
        done = sum(1 for k in order if self._stage_states.get(k) == "done")
        if hasattr(self, "progressBar") and self.progressBar is not None:
            self.progressBar.value = int(done)

        running = [k for k in order if self._stage_states.get(k) == "running"]
        errors = [k for k in order if self._stage_states.get(k) == "error"]
        label_map = {
            "dataset": "dataset selection",
            "parse": "parse",
            "masks": "mask generation",
            "registration": "registration",
            "analysis": "analysis",
        }
        if errors:
            text = f"Current step: blocked at {label_map.get(errors[0], errors[0])}"
        elif running:
            text = f"Current step: {label_map.get(running[0], running[0])}"
        elif done == len(order):
            text = "Current step: complete"
        else:
            text = "Current step: idle"
        if hasattr(self, "currentStepLabel") and self.currentStepLabel is not None:
            self.currentStepLabel.text = text

    def _on_apply_preset(self):
        preset = str(self.presetCombo.currentText or "Default").strip().lower()
        if hasattr(self, "advancedSettingsBox") and self.advancedSettingsBox is not None:
            self.advancedSettingsBox.collapsed = False

        def _summary():
            return (
                f"Timelapse: sampling={self.tlSampling.value:.5f}, res={int(self.tlRes.value)}, iter={int(self.tlIter.value)}"
                f"<br>Multistack: sampling={self.msSampling.value:.5f}, res={int(self.msRes.value)}, iter={int(self.msIter.value)}"
                f"<br>Analysis: cluster={int(self.analysisCluster.value)}"
            )

        if preset == "fast preview":
            # faster/safer defaults for quick checks
            self.tlRes.value = 3
            self.tlIter.value = 100
            self.tlSampling.value = 0.001
            self.msRes.value = 3
            self.msIter.value = 100
            self.msSampling.value = 0.002
            self.analysisCluster.value = 20
            self._set_user_message("info", "Preset applied: Fast preview", _summary())
            return
        if preset == "high quality":
            self.tlRes.value = 6
            self.tlIter.value = 400
            self.tlSampling.value = 0.008
            self.msRes.value = 6
            self.msIter.value = 400
            self.msSampling.value = 0.008
            self.analysisCluster.value = 8
            self._set_user_message("info", "Preset applied: High quality", _summary())
            return
        # Default
        self.tlRes.value = 4
        self.tlIter.value = 250
        self.tlSampling.value = 0.001
        self.msRes.value = 4
        self.msIter.value = 250
        self.msSampling.value = 0.005
        self.analysisCluster.value = 12
        self._set_user_message("info", "Preset applied: Default", _summary())

    def _load_defaults_from_pipeline_config(self):
        if not self.logic.is_pipeline_available():
            return
        try:
            import yaml

            with open(self.logic.default_config_path(), "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}

            seg_cfg = ((cfg.get("masks") or {}).get("segmentation") or {})
            adaptive_low = float(seg_cfg.get("adaptive_low_threshold", 100.0))
            adaptive_high = float(seg_cfg.get("adaptive_high_threshold", 300.0))
            global_low = float(seg_cfg.get("trab_threshold", 100.0))
            global_high = float(seg_cfg.get("cort_threshold", 300.0))
            self._mask_method_defaults = {
                "adaptive": (adaptive_low, adaptive_high),
                "global": (global_low, global_high),
            }
            self._on_mask_method_changed(self.maskMethod.currentText)
        except Exception as exc:
            self._show(f"[settings] could not load defaults from pipeline config: {exc}")

    def _on_mask_method_changed(self, method_name):
        method = str(method_name).strip().lower()
        if method not in self._mask_method_defaults:
            return
        low, high = self._mask_method_defaults[method]
        self.maskLow.value = float(low)
        self.maskHigh.value = float(high)

    def _settings_override(self):
        label_map = {
            "resorption": 1,
            "quiescent": 2,
            "formation": 3,
        }

        if self.tlSampling.value > 0.01:
            self._show("[warning] Timelapse sampling > 0.01 can be slow or unstable on some datasets.")
        if self.msSampling.value > 0.01:
            self._show("[warning] Multistack sampling > 0.01 can be slow or unstable on some datasets.")

        return {
            "import": {
                # Do not fail when z-slices are not perfectly divisible by stack depth.
                # Keep the last partial stack to preserve data coverage.
                "on_incomplete_stack": "keep_last",
            },
            "masks": {
                "segmentation": {
                    "method": self.maskMethod.currentText,
                    "trab_threshold": float(self.maskLow.value),
                    "cort_threshold": float(self.maskHigh.value),
                    "adaptive_low_threshold": float(self.maskLow.value),
                    "adaptive_high_threshold": float(self.maskHigh.value),
                }
            },
            "timelapsed_registration": {
                "metric": self.regMetric.currentText,
                "sampling_percentage": float(self.tlSampling.value),
                "number_of_resolutions": int(self.tlRes.value),
                "number_of_iterations": int(self.tlIter.value),
            },
            "multistack_correction": {
                "metric": self.regMetric.currentText,
                "sampling_percentage": float(self.msSampling.value),
                "number_of_resolutions": int(self.msRes.value),
                "number_of_iterations": int(self.msIter.value),
            },
            "analysis": {
                "thresholds": [float(self.analysisThreshold.value)],
                "cluster_sizes": [int(self.analysisCluster.value)],
                "use_filled_images": False,
            },
            "fusion": {
                "enable_filling": False,
            },
            "visualization": {
                "threshold": float(self.analysisThreshold.value),
                "cluster_size": int(self.analysisCluster.value),
                "label_map": label_map,
            },
        }

    def _run(self, args):
        try:
            stage = None
            if self._active_stage is not None:
                stage = self._active_stage
                self._set_stage_status(stage, "running")
            self._set_running_ui(True)
            self.logic.run_cli(args, on_output=self._show, on_finished=self._on_finished)
            self._show("[timelapsed-slicer] started: " + " ".join(args))
        except Exception as exc:
            self._set_running_ui(False)
            if stage is not None:
                self._set_stage_status(stage, "error")
            self._set_user_message(
                "error",
                "Could not start command",
                "Please verify the selected dataset and installed dependencies."
                f"<br><small>{str(exc)}</small>",
            )
            slicer.util.errorDisplay(str(exc))

    def _run_sequence(self, commands, stages=None):
        if not commands:
            return
        self._queued_commands = [list(cmd) for cmd in commands[1:]]
        self._queued_stages = list(stages[1:]) if stages else []
        self._active_stage = stages[0] if stages else None
        self._run(commands[0])

    def _set_running_ui(self, is_running):
        running = bool(is_running)
        for btn in [self.runMasksBtn, self.runTimelapseBtn, self.runMultistackBtn, self.runAnalysisBtn]:
            btn.enabled = not running
        self.cancelRunBtn.enabled = running

    def _on_cancel_run(self):
        cancelled = self.logic.cancel_run()
        killed_external = self.logic.kill_external_runs()
        self._queued_commands = []
        self._queued_stages = []
        self._active_stage = None
        self._run_includes_analysis = False
        self._set_running_ui(False)
        if cancelled or killed_external:
            extra = ""
            if killed_external:
                extra = "<br>Also terminated stale process IDs: " + ", ".join(str(p) for p in killed_external)
            self._set_user_message("warn", "Run cancelled", "Current run was cancelled by user." + extra)
            self._show("[timelapsed-slicer] cancellation requested.")
        else:
            self._set_user_message(
                "info",
                "No active run",
                "There was no active process to cancel.",
            )

    def _on_parse(self):
        if not self.logic.is_pipeline_available():
            slicer.util.errorDisplay("Please install timelapsed-hrpqct first.")
            return
        root = self._dataset_root()
        if root is None:
            self._set_user_message(
                "warn",
                "Select dataset folder",
                "Choose a dataset root directory before parsing.",
            )
            slicer.util.errorDisplay("Select a dataset folder first.")
            return

        self._clear_user_message()
        self._set_stage_status("parse", "running")
        sessions, err = self.logic.parse_input(root)
        if err:
            self.parseTable.setRowCount(0)
            self.parseSummaryLabel.text = "Parse summary: failed"
            self.parseSummaryLabel.styleSheet = "color: #cc5500;"
            self._last_parsed_sessions = []
            self._parsed_baseline_rows = []
            self._set_stage_status("parse", "error")
            msg = (
                "Could not parse file naming. Check that filenames include subject/session and use expected tokens."
                "<br><br><b>Examples</b>:"
                "<br><code>SUBJ001_DT_T1.AIM</code>"
                "<br><code>SUBJ001_DT_STACK01_T1.AIM</code>"
                "<br><code>SUBJ001_DT_T1_TRAB_MASK.AIM</code>"
                "<br><code>SUBJ001_DT_T1_CORT_MASK.AIM</code>"
                "<br><code>SUBJ001_DT_T1_REGMASK.AIM</code>"
                "<br><code>SUBJ001_DT_T1_ROI1.AIM</code>"
                f"<br><br><small>{err}</small>"
            )
            self._set_user_message("error", "Parse failed", msg)
            slicer.util.warningDisplay(msg)
            return

        self._show(f"[parse] discovered {len(sessions)} sessions under {root}")
        self._last_parsed_sessions = list(sessions)
        self._parsed_baseline_rows = [
            (
                str(getattr(s, "subject_id", "")).strip(),
                str(getattr(s, "site", "")).strip().lower(),
                str(getattr(s, "session_id", "")).strip(),
                "-" if getattr(s, "stack_index", None) is None else str(int(getattr(s, "stack_index"))),
            )
            for s in sessions
        ]
        self._populate_parse_table(sessions)
        self._refresh_patient_list()
        self._set_stage_status("parse", "done")
        self._set_user_message("success", "Parse successful", f"Discovered {len(sessions)} session(s).")

    def _site_options(self):
        return ["radius", "tibia", "knee"]

    def _session_options(self, sessions):
        default = ["T1", "T2", "T3", "T4", "T5", "BL", "FL", "C1", "C2", "C3"]
        seen = set(default)
        out = list(default)
        for session in sessions:
            sid = str(getattr(session, "session_id", "")).strip()
            if sid and sid not in seen:
                out.append(sid)
                seen.add(sid)
        return out

    def _on_parse_table_item_changed(self, item):
        if self._updating_parse_table or item is None:
            return
        row = int(item.row())
        col = int(item.column())
        if row < 0 or row >= len(self._last_parsed_sessions):
            return
        text = str(item.text() or "").strip()
        if col == 0:
            if text:
                self._last_parsed_sessions[row].subject_id = text
        elif col == 3:
            if text == "-" or text == "":
                self._last_parsed_sessions[row].stack_index = None
            else:
                try:
                    stack_value = int(text)
                    self._last_parsed_sessions[row].stack_index = stack_value if stack_value > 0 else None
                except Exception:
                    pass
        self._refresh_patient_list()

    def _on_parse_site_changed(self, row, text):
        if self._updating_parse_table:
            return
        if row < 0 or row >= len(self._last_parsed_sessions):
            return
        site = str(text or "").strip().lower()
        if not site:
            return
        self._last_parsed_sessions[row].site = site
        self._refresh_patient_list()

    def _on_parse_session_changed(self, row, text):
        if self._updating_parse_table:
            return
        if row < 0 or row >= len(self._last_parsed_sessions):
            return
        session_id = str(text or "").strip()
        if not session_id:
            return
        self._last_parsed_sessions[row].session_id = session_id
        self._refresh_patient_list()

    def _sanitize_name_token(self, text):
        token = re.sub(r"[^A-Za-z0-9]+", "_", str(text or "").strip())
        token = token.strip("_")
        return token or "UNKNOWN"

    def _site_to_token(self, site):
        site_norm = str(site or "").strip().lower()
        mapping = {
            "radius": "DR",
            "tibia": "DT",
            "knee": "KN",
        }
        return mapping.get(site_norm, self._sanitize_name_token(site).upper())

    def _mask_role_suffix(self, role):
        role_norm = str(role or "").strip().lower()
        if role_norm == "trab":
            return "_TRAB_MASK"
        if role_norm == "cort":
            return "_CORT_MASK"
        if role_norm == "full":
            return "_FULL_MASK"
        if role_norm == "regmask":
            return "_REGMASK"
        return "_" + self._sanitize_name_token(role).upper()

    def _reset_temp_input_root(self):
        if not self._temp_input_root:
            return
        try:
            shutil.rmtree(self._temp_input_root, ignore_errors=True)
        except Exception:
            pass
        self._temp_input_root = None

    def _sync_sessions_from_parse_table(self):
        if not self._last_parsed_sessions:
            return
        rows = min(int(self.parseTable.rowCount), len(self._last_parsed_sessions))
        for row in range(rows):
            session = self._last_parsed_sessions[row]
            subj_item = self.parseTable.item(row, 0)
            stack_item = self.parseTable.item(row, 3)
            site_widget = self.parseTable.cellWidget(row, 1)
            session_widget = self.parseTable.cellWidget(row, 2)

            subject = str(subj_item.text() if subj_item else "").strip()
            site = str(site_widget.currentText if site_widget else "").strip().lower()
            session_id = str(session_widget.currentText if session_widget else "").strip()
            stack_text = str(stack_item.text() if stack_item else "").strip()

            if subject:
                session.subject_id = subject
            if site:
                session.site = site
            if session_id:
                session.session_id = session_id
            if stack_text in {"", "-"}:
                session.stack_index = None
            else:
                try:
                    stack_value = int(stack_text)
                    session.stack_index = stack_value if stack_value > 0 else None
                except Exception:
                    pass

    def _has_parse_overrides(self):
        if not self._last_parsed_sessions or not self._parsed_baseline_rows:
            return False
        table_rows = int(self.parseTable.rowCount)
        if table_rows != len(self._parsed_baseline_rows):
            return False
        for row in range(table_rows):
            subj_item = self.parseTable.item(row, 0)
            stack_item = self.parseTable.item(row, 3)
            site_widget = self.parseTable.cellWidget(row, 1)
            session_widget = self.parseTable.cellWidget(row, 2)
            subj_ui = str(subj_item.text() if subj_item else "").strip()
            site_ui = str(site_widget.currentText if site_widget else "").strip().lower()
            ses_ui = str(session_widget.currentText if session_widget else "").strip()
            stack_ui = str(stack_item.text() if stack_item else "").strip()
            subj0, site0, ses0, stack0 = self._parsed_baseline_rows[row]
            if (subj_ui, site_ui, ses_ui, stack_ui) != (subj0, site0, ses0, stack0):
                return True
        return False

    def _make_run_input_root(self, dataset_root: Path):
        self._sync_sessions_from_parse_table()
        if not self._last_parsed_sessions or not self._has_parse_overrides():
            return dataset_root

        self._reset_temp_input_root()
        tmp_root = Path(tempfile.mkdtemp(prefix="timelapsed_slicer_input_"))
        created = 0

        def _link_or_copy(src: Path, dst: Path):
            dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.symlink(str(src), str(dst))
            except Exception:
                shutil.copy2(src, dst)

        for session in self._last_parsed_sessions:
            subject_id = self._sanitize_name_token(getattr(session, "subject_id", ""))
            site_token = self._site_to_token(getattr(session, "site", "radius"))
            session_id = self._sanitize_name_token(getattr(session, "session_id", ""))
            stack_index = getattr(session, "stack_index", None)
            stack_chunk = ""
            if stack_index is not None:
                try:
                    stack_chunk = f"_STACK{int(stack_index):02d}"
                except Exception:
                    stack_chunk = f"_STACK{self._sanitize_name_token(stack_index)}"

            base = f"{subject_id}_{site_token}{stack_chunk}_{session_id}"
            image_src = Path(getattr(session, "raw_image_path"))
            image_dst = tmp_root / f"{base}.AIM"
            _link_or_copy(image_src, image_dst)
            created += 1

            raw_masks = getattr(session, "raw_mask_paths", {}) or {}
            for role, mask_path in raw_masks.items():
                mask_src = Path(mask_path)
                suffix = self._mask_role_suffix(role)
                mask_dst = tmp_root / f"{base}{suffix}.AIM"
                _link_or_copy(mask_src, mask_dst)
                created += 1

            seg_path = getattr(session, "raw_seg_path", None)
            if seg_path:
                seg_src = Path(seg_path)
                seg_dst = tmp_root / f"{base}_SEG.AIM"
                _link_or_copy(seg_src, seg_dst)
                created += 1

        self._temp_input_root = str(tmp_root)
        self._show(
            f"[parse] using corrected parse labels via virtual input root: {tmp_root} ({created} file links)"
        )
        return tmp_root

    def _populate_parse_table(self, sessions):
        self._updating_parse_table = True
        try:
            self.parseTable.setRowCount(len(sessions))
            self.parseSummaryLabel.text = (
                f"Parse summary: {len(sessions)} session(s) discovered "
                "(Subject is editable. Site/Session are dropdown-correctable.)"
            )
            self.parseSummaryLabel.styleSheet = "color: #228b22;"
            site_options = self._site_options()
            session_options = self._session_options(sessions)

            for row, session in enumerate(sessions):
                subject = str(getattr(session, "subject_id", ""))
                site = str(getattr(session, "site", ""))
                session_id = str(getattr(session, "session_id", ""))
                stack_index = getattr(session, "stack_index", None)
                stack_text = "-" if stack_index is None else str(stack_index)

                raw_image = getattr(session, "raw_image_path", None)
                image_name = Path(raw_image).name if raw_image else "-"

                raw_masks = getattr(session, "raw_mask_paths", {}) or {}
                mask_roles = ", ".join(sorted(str(k) for k in raw_masks.keys())) if raw_masks else "-"

                seg_path = getattr(session, "raw_seg_path", None)
                seg_text = "yes" if seg_path else "no"

                # Subject (editable text)
                subject_item = qt.QTableWidgetItem(subject)
                subject_item.setFlags(subject_item.flags() | qt.Qt.ItemIsEditable)
                self.parseTable.setItem(row, 0, subject_item)

                # Site (dropdown)
                site_combo = qt.QComboBox()
                site_combo.addItems(site_options)
                site_current = site if site in site_options else "radius"
                site_combo.setCurrentText(site_current)
                site_combo.currentTextChanged.connect(
                    lambda text, r=row: self._on_parse_site_changed(r, text)
                )
                self.parseTable.setCellWidget(row, 1, site_combo)

                # Session (dropdown + editable)
                ses_combo = qt.QComboBox()
                ses_combo.setEditable(True)
                ses_combo.addItems(session_options)
                ses_combo.setCurrentText(session_id)
                ses_combo.currentTextChanged.connect(
                    lambda text, r=row: self._on_parse_session_changed(r, text)
                )
                self.parseTable.setCellWidget(row, 2, ses_combo)

                # Stack
                stack_item = qt.QTableWidgetItem(stack_text)
                stack_item.setFlags(stack_item.flags() | qt.Qt.ItemIsEditable)
                self.parseTable.setItem(row, 3, stack_item)

                # Read-only informative columns
                for col, value in [(4, image_name), (5, mask_roles), (6, seg_text)]:
                    item = qt.QTableWidgetItem(value)
                    item.setFlags(item.flags() & ~qt.Qt.ItemIsEditable)
                    self.parseTable.setItem(row, col, item)

            self.parseTable.resizeColumnsToContents()
        finally:
            self._updating_parse_table = False

    def _require_pipeline_installed(self) -> bool:
        if self.logic.is_pipeline_available():
            return True
        slicer.util.errorDisplay("Please install timelapsed-hrpqct first.")
        return False

    def _require_dataset_root(self) -> Path | None:
        root = self._dataset_root()
        if root is not None:
            return root
        slicer.util.errorDisplay("Select a dataset root first.")
        return None

    def _require_results_root(self, message: str = "Could not resolve results dataset path.") -> Path | None:
        imported = self._imported_dataset_root()
        if imported is not None:
            return imported
        slicer.util.errorDisplay(message)
        return None

    def _on_run_masks(self):
        if not self._require_pipeline_installed():
            return
        source_root = self._require_dataset_root()
        if source_root is None:
            return
        run_root = self._make_run_input_root(source_root)

        cfg = self.logic.create_override_config(self._settings_override())
        imported = self._require_results_root("Could not resolve imported dataset path.")
        if imported is None:
            return
        self._set_stage_status("masks", "pending")
        self._is_full_pipeline_run = False
        self._run_includes_analysis = False
        self._run_sequence(
            [
                ["import", str(run_root), "--output-root", str(imported), "--config", cfg],
                ["generate-masks", str(imported), "--config", cfg],
            ],
            stages=["masks", "masks"],
        )

    def _on_run_timelapse(self):
        if not self._require_pipeline_installed():
            return
        source_root = self._require_dataset_root()
        if source_root is None:
            return
        run_root = self._make_run_input_root(source_root)

        imported = self._require_results_root()
        if imported is None:
            return
        self._set_stage_status("registration", "pending")
        self._set_stage_status("analysis", "pending")
        self._active_stage = "registration"
        self._is_full_pipeline_run = False
        self._run_includes_analysis = True
        cfg = self.logic.create_override_config(self._settings_override())
        self._run(
            [
                "run",
                str(run_root),
                "--output-root",
                str(imported),
                "--mode",
                "regular",
                "--skip-mask-generation",
                "--config",
                cfg,
            ]
        )

    def _on_run_multistack(self):
        if not self._require_pipeline_installed():
            return
        source_root = self._require_dataset_root()
        if source_root is None:
            return
        run_root = self._make_run_input_root(source_root)

        imported = self._require_results_root()
        if imported is None:
            return
        self._set_stage_status("registration", "pending")
        self._set_stage_status("analysis", "pending")
        self._active_stage = "registration"
        self._is_full_pipeline_run = False
        self._run_includes_analysis = True
        cfg = self.logic.create_override_config(self._settings_override())
        self._run(
            [
                "run",
                str(run_root),
                "--output-root",
                str(imported),
                "--mode",
                "multistack",
                "--skip-mask-generation",
                "--config",
                cfg,
            ]
        )

    def _on_run_analysis(self):
        if not self._require_pipeline_installed():
            return
        root = self._require_dataset_root()
        if root is None:
            return

        imported = self._require_results_root("Could not resolve imported dataset path.")
        if imported is None:
            return
        self._set_stage_status("analysis", "pending")
        self._active_stage = "analysis"
        self._is_full_pipeline_run = False
        self._run_includes_analysis = False
        cfg = self.logic.create_override_config(self._settings_override())
        self._run([
            "analyse",
            str(imported),
            "--thr",
            str(float(self.analysisThreshold.value)),
            "--clusters",
            str(int(self.analysisCluster.value)),
            "--config",
            cfg,
        ])

    def _auto_mode_from_sessions(self):
        has_multistack = any(
            getattr(s, "stack_index", None) is not None and int(getattr(s, "stack_index", 0)) > 1
            for s in (self._last_parsed_sessions or [])
        )
        return "multistack" if has_multistack else "regular"

    def _on_run_full_pipeline(self):
        if not self._require_pipeline_installed():
            return
        source_root = self._require_dataset_root()
        if source_root is None:
            self._set_user_message("warn", "Select dataset folder", "Choose a dataset root first.")
            return
        run_root = self._make_run_input_root(source_root)
        imported = self._require_results_root("Could not resolve imported dataset path.")
        if imported is None:
            return
        mode = self._auto_mode_from_sessions()
        cfg = self.logic.create_override_config(self._settings_override())
        self._set_user_message(
            "info",
            "Running full pipeline",
            (
                f"Mode: <b>{mode}</b>. Running unified pipeline command with smart skip detection "
                "(existing import/masks/registration/analysis outputs are reused when available)."
            ),
        )
        for s in ("masks", "registration", "analysis"):
            self._set_stage_status(s, "pending")
        self._active_stage = "registration"
        self._is_full_pipeline_run = True
        self._run_includes_analysis = True
        self._run(
            [
                "run",
                str(run_root),
                "--output-root",
                str(imported),
                "--mode",
                mode,
                "--config",
                cfg,
            ]
        )

    def _on_finished(self, exit_code, exit_status):
        self._show(f"[timelapsed-slicer] finished with exit code {exit_code}")
        self._set_running_ui(False)
        if self._active_stage is not None:
            self._set_stage_status(self._active_stage, "done" if int(exit_code) == 0 else "error")
        if int(exit_code) != 0:
            self._set_user_message(
                "error",
                "Pipeline step failed",
                "Check the log below for the failing command and verify filenames/config."
                " You can rerun individual steps after fixing the issue.",
            )
            self._queued_commands = []
            self._queued_stages = []
            self._active_stage = None
            self._is_full_pipeline_run = False
            self._run_includes_analysis = False
            self._refresh_patient_list()
            return
        if self._queued_commands and exit_code == 0:
            next_cmd = self._queued_commands.pop(0)
            self._active_stage = self._queued_stages.pop(0) if self._queued_stages else None
            self._run(next_cmd)
            return
        self._queued_commands = []
        self._queued_stages = []
        if self._is_full_pipeline_run and int(exit_code) == 0:
            for s in ("masks", "registration", "analysis"):
                self._set_stage_status(s, "done")
        elif self._run_includes_analysis and int(exit_code) == 0:
            # Timelapse pipeline commands internally perform discovery/import
            # and analysis, so mark all stages complete for clear 100% progress.
            for s in ("dataset", "parse", "masks", "registration", "analysis"):
                self._set_stage_status(s, "done")
        self._active_stage = None
        self._is_full_pipeline_run = False
        self._run_includes_analysis = False
        self._refresh_patient_list()
        self._set_user_message("success", "Completed", "Requested step(s) finished successfully.")

    def _on_install_pipeline(self):
        self._show("[dependency] Installing/updating timelapsed-hrpqct ...")
        try:
            slicer.app.setOverrideCursor(qt.Qt.WaitCursor)
            self.logic.install_or_update_pipeline()
            self._show("[dependency] Installation finished.")
            slicer.util.infoDisplay(
                "timelapsed-hrpqct installation finished.\\n"
                "If import problems persist, restart Slicer."
            )
        except Exception as exc:
            slicer.util.errorDisplay(f"Install failed: {exc}")
        finally:
            slicer.app.restoreOverrideCursor()
            self._update_dependency_ui()

    def _on_check_pipeline(self):
        self._update_dependency_ui()

    def _update_dependency_ui(self):
        available = self.logic.is_pipeline_available()
        if available:
            self.pipelineStatusLabel.text = "Installed"
            self.pipelineStatusLabel.styleSheet = "color: #228b22;"
        else:
            self.pipelineStatusLabel.text = "Not installed"
            self.pipelineStatusLabel.styleSheet = "color: #cc5500;"

    def _refresh_patient_list(self):
        self.patientCombo.clear()
        self._patient_keys = []

        imported = self._imported_dataset_root()
        keys = set()
        if imported is not None and imported.exists():
            try:
                from timelapsedhrpqct.dataset.artifacts import (
                    iter_filled_session_records,
                    iter_fused_session_records,
                    iter_imported_stack_records,
                )

                for rec in iter_imported_stack_records(imported):
                    keys.add((str(rec.subject_id), str(rec.site)))
                for rec in iter_fused_session_records(imported):
                    keys.add((str(rec.subject_id), str(rec.site)))
                for rec in iter_filled_session_records(imported):
                    keys.add((str(rec.subject_id), str(rec.site)))
            except Exception as exc:
                self._show(f"[patients] artifact lookup failed: {exc}")

        if not keys and self._last_parsed_sessions:
            for s in self._last_parsed_sessions:
                subject = str(getattr(s, "subject_id", "")).strip()
                site = str(getattr(s, "site", "")).strip() or "radius"
                if subject:
                    keys.add((subject, site))

        self._patient_keys = sorted(keys)
        for subject, site in self._patient_keys:
            self.patientCombo.addItem(f"sub-{subject} | site-{site}")

    def _current_patient_key(self):
        idx = int(self.patientCombo.currentIndex)
        if idx < 0 or idx >= len(self._patient_keys):
            return None
        return self._patient_keys[idx]

    def _subject_hierarchy(self):
        return slicer.vtkMRMLSubjectHierarchyNode.GetSubjectHierarchyNode(slicer.mrmlScene)

    def _install_subject_hierarchy_selection_hook(self):
        if self._sh_tree_hooks_installed:
            return
        main = slicer.util.mainWindow()
        if main is None:
            return

        try:
            trees = slicer.util.findChildren(main, className="qMRMLSubjectHierarchyTreeView")
        except Exception:
            trees = []

        hooked = False
        for tree in trees:
            if tree.property("timelapsedHooked"):
                continue
            connected = False
            try:
                tree.currentItemChanged.connect(self._on_subject_hierarchy_item_selected)
                connected = True
            except Exception:
                pass
            if not connected:
                try:
                    tree.connect("currentItemChanged(vtkIdType)", self._on_subject_hierarchy_item_selected)
                    connected = True
                except Exception:
                    pass
            if connected:
                tree.setProperty("timelapsedHooked", True)
                hooked = True

        if hooked:
            self._sh_tree_hooks_installed = True
            self._show("[load] Subject hierarchy stack-selection hook enabled.")
        else:
            qt.QTimer.singleShot(1500, self._install_subject_hierarchy_selection_hook)

    def _first_scalar_volume_under_item(self, item_id):
        sh = self._subject_hierarchy()
        if sh is None:
            return None

        node = sh.GetItemDataNode(item_id)
        if node is not None and node.IsA("vtkMRMLScalarVolumeNode"):
            return node

        child_ids = vtk.vtkIdList()
        try:
            sh.GetItemChildren(item_id, child_ids, True)
        except TypeError:
            try:
                sh.GetItemChildren(item_id, child_ids)
            except Exception:
                return None
        except Exception:
            return None

        for i in range(child_ids.GetNumberOfIds()):
            child_id = child_ids.GetId(i)
            child_node = sh.GetItemDataNode(child_id)
            if child_node is not None and child_node.IsA("vtkMRMLScalarVolumeNode"):
                return child_node
        return None

    def _on_subject_hierarchy_item_selected(self, item_id):
        # Disabled for now: keep subject hierarchy interaction passive/predictable.
        return

    def _ensure_folder_item(self, parent_item_id, name):
        sh = self._subject_hierarchy()
        child_id = sh.GetItemChildWithName(parent_item_id, name)
        if child_id:
            return child_id
        return sh.CreateFolderItem(parent_item_id, name)

    def _ensure_load_folder(self, subject_id, site, session_id=None, stack_index=None):
        sh = self._subject_hierarchy()
        scene_id = sh.GetSceneItemID()
        root_id = self._ensure_folder_item(scene_id, "TimelapsedHRpQCT Loaded")
        subj_id = self._ensure_folder_item(root_id, f"sub-{subject_id}")
        site_id = self._ensure_folder_item(subj_id, f"site-{site}")
        if session_id is None:
            return site_id
        ses_id = self._ensure_folder_item(site_id, f"ses-{session_id}")
        if stack_index is None:
            return ses_id
        return self._ensure_folder_item(ses_id, f"stack-{int(stack_index):02d}")

    def _set_item_visibility_safe(self, item_id, visible):
        sh = self._subject_hierarchy()
        if sh is None:
            return
        try:
            sh.SetItemDisplayVisibility(int(item_id), 1 if bool(visible) else 0)
        except Exception:
            pass

    def _collect_stack_items(self, subject_id=None, site=None):
        sh = self._subject_hierarchy()
        if sh is None:
            return []
        scene_id = sh.GetSceneItemID()
        root_id = sh.GetItemChildWithName(scene_id, "TimelapsedHRpQCT Loaded")
        if not root_id:
            return []

        base_id = root_id
        if subject_id is not None:
            subj_id = sh.GetItemChildWithName(base_id, f"sub-{subject_id}")
            if not subj_id:
                return []
            base_id = subj_id
        if site is not None:
            site_id = sh.GetItemChildWithName(base_id, f"site-{site}")
            if not site_id:
                return []
            base_id = site_id

        child_ids = vtk.vtkIdList()
        try:
            sh.GetItemChildren(base_id, child_ids, True)
        except TypeError:
            sh.GetItemChildren(base_id, child_ids)
        except Exception:
            return []

        stack_items = []
        for i in range(child_ids.GetNumberOfIds()):
            cid = child_ids.GetId(i)
            name = str(sh.GetItemName(cid) or "").lower()
            if name.startswith("stack-"):
                stack_items.append(cid)
        return stack_items

    def _stack_sort_key(self, item_id):
        sh = self._subject_hierarchy()
        if sh is None:
            return (9999, 9999)
        name = str(sh.GetItemName(item_id) or "")
        stack_match = re.search(r"stack-(\d+)", name, flags=re.IGNORECASE)
        stack_num = int(stack_match.group(1)) if stack_match else 9999
        parent_id = sh.GetItemParent(item_id)
        parent_name = str(sh.GetItemName(parent_id) or "")
        ses_match = re.search(r"ses-([A-Za-z]*)(\d+)", parent_name)
        ses_num = int(ses_match.group(2)) if ses_match else 9999
        return (ses_num, stack_num)

    def _set_exclusive_stack_visibility(self, active_stack_item_id, subject_id=None, site=None):
        stack_items = self._collect_stack_items(subject_id=subject_id, site=site)
        if not stack_items:
            return
        for sid in stack_items:
            self._set_item_visibility_safe(sid, int(sid) == int(active_stack_item_id))

    def _apply_default_stack_visibility(self, subject_id, site):
        stack_items = self._collect_stack_items(subject_id=subject_id, site=site)
        if not stack_items:
            return
        first_stack = sorted(stack_items, key=self._stack_sort_key)[0]
        self._set_exclusive_stack_visibility(first_stack, subject_id=subject_id, site=site)
        volume_node = self._first_scalar_volume_under_item(first_stack)
        if volume_node is not None:
            slicer.util.setSliceViewerLayers(background=volume_node, fit=False)

    def _place_node_in_folder(self, node, folder_item_id):
        if node is None:
            return
        sh = self._subject_hierarchy()
        item_id = sh.GetItemByDataNode(node)
        if item_id:
            sh.SetItemParent(item_id, folder_item_id)

    def _session_base_color(self, session_id):
        token = str(session_id).upper()
        if token in {"T1", "BL", "BASELINE"}:
            return (0.95, 0.35, 0.35)
        if token in {"T2", "FL", "FU", "FOLLOWUP", "FOLLOWUP1"}:
            return (0.20, 0.65, 0.95)
        if token in {"T3", "FU2", "FOLLOWUP2"}:
            return (0.25, 0.75, 0.40)

        m = re.search(r"(\d+)$", token)
        if m:
            idx = int(m.group(1))
            palette = [
                (0.95, 0.35, 0.35),
                (0.20, 0.65, 0.95),
                (0.25, 0.75, 0.40),
                (0.95, 0.75, 0.25),
                (0.65, 0.45, 0.90),
            ]
            return palette[(max(idx, 1) - 1) % len(palette)]
        return (0.85, 0.85, 0.30)

    def _create_scalar_node_from_array(self, name, array_zyx, spacing_xyz, origin_xyz):
        node = slicer.mrmlScene.AddNewNodeByClass("vtkMRMLScalarVolumeNode", name)
        slicer.util.updateVolumeFromArray(node, array_zyx)
        node.SetSpacing(float(spacing_xyz[0]), float(spacing_xyz[1]), float(spacing_xyz[2]))
        node.SetOrigin(float(origin_xyz[0]), float(origin_xyz[1]), float(origin_xyz[2]))
        return node

    def _configure_segmentation_display(self, seg_node):
        if seg_node is None:
            return
        display = seg_node.GetDisplayNode()
        if display is None:
            seg_node.CreateDefaultDisplayNodes()
            display = seg_node.GetDisplayNode()
        if display is None:
            return
        display.SetVisibility(True)
        display.SetVisibility2D(True)
        display.SetVisibility3D(False)
        display.SetOpacity2DFill(0.35)
        display.SetOpacity2DOutline(1.0)
        display.SetSliceIntersectionThickness(2)
        # Ensure segmentations are shown in all slice views (not bound to a specific view).
        if hasattr(display, "RemoveAllViewNodeIDs"):
            display.RemoveAllViewNodeIDs()

        segmentation = seg_node.GetSegmentation()
        if segmentation is not None:
            segment_ids = vtk.vtkStringArray()
            segmentation.GetSegmentIDs(segment_ids)
            # Some Slicer versions support all-at-once visibility toggles.
            if hasattr(display, "SetAllSegmentsVisibility"):
                display.SetAllSegmentsVisibility(True)
            # Also force each segment visible for compatibility.
            if hasattr(display, "SetSegmentVisibility"):
                for i in range(segment_ids.GetNumberOfValues()):
                    seg_id = segment_ids.GetValue(i)
                    display.SetSegmentVisibility(seg_id, True)

    def _create_segmentation_from_label_array(
        self,
        segmentation_name,
        label_arr_zyx,
        spacing_xyz,
        origin_xyz,
        folder_item_id=None,
    ):
        label_node = slicer.mrmlScene.AddNewNodeByClass(
            "vtkMRMLLabelMapVolumeNode",
            f"{segmentation_name}_tmp",
        )
        slicer.util.updateVolumeFromArray(label_node, label_arr_zyx)
        label_node.SetSpacing(float(spacing_xyz[0]), float(spacing_xyz[1]), float(spacing_xyz[2]))
        label_node.SetOrigin(float(origin_xyz[0]), float(origin_xyz[1]), float(origin_xyz[2]))

        seg_node = slicer.mrmlScene.AddNewNodeByClass("vtkMRMLSegmentationNode", segmentation_name)
        seg_node.CreateDefaultDisplayNodes()
        seg_node.SetReferenceImageGeometryParameterFromVolumeNode(label_node)
        seg_logic = slicer.modules.segmentations.logic()
        seg_logic.ImportLabelmapToSegmentationNode(label_node, seg_node)
        slicer.mrmlScene.RemoveNode(label_node)
        if folder_item_id is not None:
            self._place_node_in_folder(seg_node, folder_item_id)
        return seg_node

    def _detail_to_surface_params(self, detail):
        # detail=50 matches previous default behavior (0.5 / 0.5).
        d = max(0.0, min(100.0, float(detail))) / 100.0
        simplify = 1.0 - d
        return simplify, simplify

    def _apply_preview_surface_detail(self, seg_node, detail):
        segmentation = seg_node.GetSegmentation()
        if segmentation is None:
            return
        smoothing, decimation = self._detail_to_surface_params(detail)
        try:
            segmentation.SetConversionParameter("Smoothing factor", f"{float(smoothing):.4f}")
            segmentation.SetConversionParameter("Decimation factor", f"{float(decimation):.4f}")
        except Exception:
            return
        try:
            seg_node.RemoveClosedSurfaceRepresentation()
        except Exception:
            pass
        try:
            seg_node.CreateClosedSurfaceRepresentation()
        except Exception:
            pass

    def _style_remodelling_segmentation(self, seg_node, show2d=True, show3d=True):
        display = seg_node.GetDisplayNode()
        if display is None:
            seg_node.CreateDefaultDisplayNodes()
            display = seg_node.GetDisplayNode()
        if display is None:
            return
        display.SetVisibility(True)
        display.SetVisibility2D(bool(show2d))
        display.SetVisibility3D(bool(show3d))
        display.SetOpacity2DFill(0.35)
        display.SetOpacity2DOutline(1.0)
        display.SetOpacity3D(1.0)
        if hasattr(display, "RemoveAllViewNodeIDs"):
            display.RemoveAllViewNodeIDs()

        label_style = {
            1: ("resorption", (0.98, 0.15, 0.68), 1.0),  # bright pink
            2: ("quiescent", (0.25, 0.25, 0.25), 1.0),   # dark gray
            3: ("formation", (1.00, 0.45, 0.00), 1.0),   # bright orange
        }
        seg = seg_node.GetSegmentation()
        ids = vtk.vtkStringArray()
        seg.GetSegmentIDs(ids)
        for i in range(ids.GetNumberOfValues()):
            seg_id = ids.GetValue(i)
            segment = seg.GetSegment(seg_id)
            if segment is None:
                continue
            name = str(segment.GetName() or "")
            m = re.search(r"(\d+)", name)
            label_val = int(m.group(1)) if m else None
            if label_val in label_style:
                disp_name, color, opacity3d = label_style[label_val]
                segment.SetName(disp_name)
                segment.SetColor(float(color[0]), float(color[1]), float(color[2]))
                if hasattr(display, "SetSegmentOpacity3D"):
                    display.SetSegmentOpacity3D(seg_id, float(opacity3d))
                if hasattr(display, "SetSegmentVisibility"):
                    display.SetSegmentVisibility(seg_id, True)

    def _create_midplane_preview(self, arr_zyx, axis, thickness_vox):
        axis_token = str(axis).strip().lower()
        t = max(1, int(thickness_vox))
        half = max(0, t // 2)
        out = np.zeros_like(arr_zyx)
        zdim, ydim, xdim = (int(arr_zyx.shape[0]), int(arr_zyx.shape[1]), int(arr_zyx.shape[2]))
        if axis_token == "z":
            c = zdim // 2
            a0, a1 = max(0, c - half), min(zdim, c + half + 1)
            out[a0:a1, :, :] = arr_zyx[a0:a1, :, :]
        elif axis_token == "y":
            c = ydim // 2
            a0, a1 = max(0, c - half), min(ydim, c + half + 1)
            out[:, a0:a1, :] = arr_zyx[:, a0:a1, :]
        else:
            c = xdim // 2
            a0, a1 = max(0, c - half), min(xdim, c + half + 1)
            out[:, :, a0:a1] = arr_zyx[:, :, a0:a1]
        return out

    def _refresh_remodelling_full_selector(self):
        self.remodellingFullSegCombo.clear()
        scene = slicer.mrmlScene
        for i in range(scene.GetNumberOfNodesByClass("vtkMRMLSegmentationNode")):
            node = scene.GetNthNodeByClass(i, "vtkMRMLSegmentationNode")
            if node is None:
                continue
            if not str(node.GetAttribute("TimelapsedHRpQCT.RemodellingFull") or "") == "1":
                continue
            self.remodellingFullSegCombo.addItem(node.GetName(), node.GetID())

    def _remove_existing_preview_for_full(self, full_seg_node):
        base = str(full_seg_node.GetName() or "")
        if base.endswith("_full"):
            prefix = base[:-5] + "_midslice_"
        else:
            prefix = base + "_midslice_"
        to_remove = []
        scene = slicer.mrmlScene
        for i in range(scene.GetNumberOfNodesByClass("vtkMRMLSegmentationNode")):
            node = scene.GetNthNodeByClass(i, "vtkMRMLSegmentationNode")
            if node is None:
                continue
            if str(node.GetName() or "").startswith(prefix):
                to_remove.append(node)
        for node in to_remove:
            slicer.mrmlScene.RemoveNode(node)

    def _on_update_remodelling_preview(self):
        node_id = self.remodellingFullSegCombo.currentData
        if node_id is None:
            slicer.util.warningDisplay("No remodelling full segmentation selected.")
            return
        full_seg = slicer.mrmlScene.GetNodeByID(str(node_id))
        if full_seg is None:
            slicer.util.warningDisplay("Selected remodelling segmentation no longer exists.")
            return
        source_path = str(full_seg.GetAttribute("TimelapsedHRpQCT.RemodellingSourcePath") or "")
        if not source_path:
            slicer.util.warningDisplay("Selected segmentation is missing source path metadata.")
            return
        source = Path(source_path)
        if not source.exists():
            slicer.util.warningDisplay(f"Source file not found:\n{source}")
            return
        sh = self._subject_hierarchy()
        folder_id = None
        if sh is not None:
            item_id = sh.GetItemByDataNode(full_seg)
            if item_id:
                folder_id = sh.GetItemParent(item_id)
        base_name = str(full_seg.GetName() or "")
        if base_name.endswith("_full"):
            base_name = base_name[:-5]
        self._remove_existing_preview_for_full(full_seg)
        ok = self._load_remodelling_as_segmentation(
            segmentation_name=base_name,
            labelmap_path=source,
            folder_item_id=folder_id,
            preview_axis=self.remodellingAxisCombo.currentText,
            preview_thickness_vox=int(self.remodellingThicknessSpin.value),
            detail=int(self.remodellingDetailSlider.value),
            create_full=False,
            create_preview=True,
        )
        if ok:
            self._show("[load] remodelling midslice preview updated.")

    def _load_remodelling_as_segmentation(
        self,
        segmentation_name,
        labelmap_path,
        folder_item_id=None,
        preview_axis="x",
        preview_thickness_vox=15,
        detail=50,
        create_full=True,
        create_preview=True,
    ):
        try:
            remodelling_img = sitk.ReadImage(str(labelmap_path))
            remodelling_arr = sitk.GetArrayFromImage(remodelling_img)  # z, y, x
        except Exception as exc:
            self._show(f"[load] failed to read remodelling labelmap from {labelmap_path}: {exc}")
            return False

        spacing = remodelling_img.GetSpacing()
        origin = remodelling_img.GetOrigin()
        spacing_xyz = (float(spacing[0]), float(spacing[1]), float(spacing[2]))
        origin_xyz = (float(origin[0]), float(origin[1]), float(origin[2]))

        full_seg = None
        if create_full:
            full_seg = self._create_segmentation_from_label_array(
                segmentation_name=f"{segmentation_name}_full",
                label_arr_zyx=remodelling_arr,
                spacing_xyz=spacing_xyz,
                origin_xyz=origin_xyz,
                folder_item_id=folder_item_id,
            )
            self._style_remodelling_segmentation(full_seg, show2d=True, show3d=False)
            full_seg.SetAttribute("TimelapsedHRpQCT.RemodellingFull", "1")
            full_seg.SetAttribute("TimelapsedHRpQCT.RemodellingSourcePath", str(Path(labelmap_path).resolve()))
            self._center_slices_on_segmentation(full_seg)

        if create_preview:
            preview_arr = self._create_midplane_preview(
                remodelling_arr,
                axis=preview_axis,
                thickness_vox=preview_thickness_vox,
            )
            preview_seg = self._create_segmentation_from_label_array(
                segmentation_name=f"{segmentation_name}_midslice_{str(preview_axis).lower()}_{int(preview_thickness_vox)}",
                label_arr_zyx=preview_arr,
                spacing_xyz=spacing_xyz,
                origin_xyz=origin_xyz,
                folder_item_id=folder_item_id,
            )
            self._style_remodelling_segmentation(preview_seg, show2d=False, show3d=True)
            preview_seg.SetAttribute("TimelapsedHRpQCT.RemodellingPreview", "1")
            preview_seg.SetAttribute("TimelapsedHRpQCT.RemodellingSourcePath", str(Path(labelmap_path).resolve()))
            preview_seg.SetAttribute("TimelapsedHRpQCT.RemodellingSourceBase", f"{segmentation_name}_full")

            self._apply_preview_surface_detail(preview_seg, detail=detail)

        self._set_3d_background_black()
        self._refresh_remodelling_full_selector()
        return True

    def _center_slices_on_segmentation(self, seg_node):
        if seg_node is None:
            return
        try:
            bounds = [0.0] * 6
            seg_node.GetBounds(bounds)
            if not all(np.isfinite(bounds)):
                return
            cx = 0.5 * (bounds[0] + bounds[1])
            cy = 0.5 * (bounds[2] + bounds[3])
            cz = 0.5 * (bounds[4] + bounds[5])
            lm = slicer.app.layoutManager()
            if lm is None:
                return
            for name in ("Red", "Yellow", "Green"):
                widget = lm.sliceWidget(name)
                if widget is None:
                    continue
                node = widget.mrmlSliceNode()
                if node is not None:
                    node.JumpSliceByCentering(cx, cy, cz)
        except Exception:
            pass

    def _set_3d_background_black(self):
        try:
            lm = slicer.app.layoutManager()
            if lm is None:
                return
            for i in range(int(lm.threeDViewCount)):
                view = lm.threeDWidget(i).threeDView()
                view_node = view.mrmlViewNode() if view is not None else None
                if view_node is not None:
                    view_node.SetBackgroundColor(0.0, 0.0, 0.0)
                    view_node.SetBackgroundColor2(0.0, 0.0, 0.0)
        except Exception:
            pass

    def _create_segmentation_node_from_role_arrays(
        self,
        segmentation_name,
        role_to_array,
        spacing_xyz,
        origin_xyz,
        session_id=None,
        folder_item_id=None,
    ):
        seg_node = slicer.mrmlScene.AddNewNodeByClass("vtkMRMLSegmentationNode", segmentation_name)
        seg_node.CreateDefaultDisplayNodes()
        seg_logic = slicer.modules.segmentations.logic()
        loaded_any = False

        base_color = self._session_base_color(session_id or "")
        role_tint = {
            "full": 1.00,
            "trab": 0.85,
            "cort": 0.70,
            "regmask": 0.55,
        }

        for role in sorted(role_to_array.keys()):
            arr = (np.asarray(role_to_array[role]) > 0).astype(np.uint8)
            if int(arr.max()) == 0:
                continue

            label_node = slicer.mrmlScene.AddNewNodeByClass(
                "vtkMRMLLabelMapVolumeNode",
                f"{segmentation_name}_{role}_tmp",
            )
            slicer.util.updateVolumeFromArray(label_node, arr)
            label_node.SetSpacing(float(spacing_xyz[0]), float(spacing_xyz[1]), float(spacing_xyz[2]))
            label_node.SetOrigin(float(origin_xyz[0]), float(origin_xyz[1]), float(origin_xyz[2]))

            existing_ids = vtk.vtkStringArray()
            seg_node.GetSegmentation().GetSegmentIDs(existing_ids)
            before = {existing_ids.GetValue(i) for i in range(existing_ids.GetNumberOfValues())}

            seg_logic.ImportLabelmapToSegmentationNode(label_node, seg_node)
            slicer.mrmlScene.RemoveNode(label_node)

            updated_ids = vtk.vtkStringArray()
            seg_node.GetSegmentation().GetSegmentIDs(updated_ids)
            after = {updated_ids.GetValue(i) for i in range(updated_ids.GetNumberOfValues())}
            new_ids = list(after - before)
            for seg_id in new_ids:
                segment = seg_node.GetSegmentation().GetSegment(seg_id)
                if segment:
                    segment.SetName(str(role))
                    tint = role_tint.get(str(role).lower(), 0.9)
                    segment.SetColor(
                        float(min(max(base_color[0] * tint, 0.0), 1.0)),
                        float(min(max(base_color[1] * tint, 0.0), 1.0)),
                        float(min(max(base_color[2] * tint, 0.0), 1.0)),
                    )
            loaded_any = loaded_any or bool(new_ids)

        if not loaded_any:
            slicer.mrmlScene.RemoveNode(seg_node)
            return False
        self._configure_segmentation_display(seg_node)
        if folder_item_id is not None:
            self._place_node_in_folder(seg_node, folder_item_id)
        return True

    def _merge_raw_session_records(self, records):
        if not records:
            return None

        recs = list(records)

        # First pass: read geometry so we can compute robust z placement.
        geom = []
        for rec in recs:
            img = sitk.ReadImage(str(rec.image_path))
            sx, sy, sz = img.GetSpacing()
            ox, oy, oz = img.GetOrigin()
            slice_range = getattr(rec, "slice_range", None)
            z_start_meta = getattr(slice_range, "z_start", None) if slice_range is not None else None
            geom.append(
                {
                    "rec": rec,
                    "img": img,
                    "spacing": (float(sx), float(sy), float(sz)),
                    "origin": (float(ox), float(oy), float(oz)),
                    "z_start_meta": int(z_start_meta) if z_start_meta is not None else None,
                }
            )

        # Prefer metadata z_start when available and non-degenerate; otherwise use origin-derived z.
        meta_starts = [g["z_start_meta"] for g in geom if g["z_start_meta"] is not None]
        use_meta_z = len(set(meta_starts)) > 1

        images = []
        spacing_xyz = None
        origin_xyz = None
        xmax = 0
        ymax = 0
        zmax = 0
        all_roles = set()
        min_oz = min(g["origin"][2] for g in geom) if geom else 0.0

        for g in geom:
            rec = g["rec"]
            img = g["img"]
            arr = sitk.GetArrayFromImage(img)  # z,y,x
            sx, sy, sz = g["spacing"]
            ox, oy, oz = g["origin"]
            if use_meta_z:
                z_start = int(g["z_start_meta"] or 0)
            else:
                z_start = int(round((float(oz) - float(min_oz)) / float(sz))) if sz > 0 else 0
            z_stop = z_start + int(arr.shape[0])

            if spacing_xyz is None:
                spacing_xyz = (sx, sy, sz)
            if origin_xyz is None:
                origin_xyz = (ox, oy, min_oz)

            ymax = max(ymax, int(arr.shape[1]))
            xmax = max(xmax, int(arr.shape[2]))
            zmax = max(zmax, z_stop)

            role_arrays = {}
            for role, mask_path in (getattr(rec, "mask_paths", {}) or {}).items():
                if mask_path and Path(mask_path).exists():
                    m_arr = sitk.GetArrayFromImage(sitk.ReadImage(str(mask_path)))
                    role_arrays[str(role)] = (m_arr > 0).astype(np.uint8)
                    all_roles.add(str(role))

            images.append((z_start, arr, role_arrays))

        images.sort(key=lambda item: int(item[0]))

        merged_img = np.zeros((zmax, ymax, xmax), dtype=images[0][1].dtype)
        merged_roles = {role: np.zeros((zmax, ymax, xmax), dtype=np.uint8) for role in sorted(all_roles)}

        for z_start, arr, role_arrays in images:
            z0 = int(z_start)
            z1 = z0 + int(arr.shape[0])
            y1 = int(arr.shape[1])
            x1 = int(arr.shape[2])
            merged_img[z0:z1, :y1, :x1] = arr

            for role, mask_arr in role_arrays.items():
                my1 = int(mask_arr.shape[1])
                mx1 = int(mask_arr.shape[2])
                mz1 = z0 + int(mask_arr.shape[0])
                merged_roles[role][z0:mz1, :my1, :mx1] = np.maximum(
                    merged_roles[role][z0:mz1, :my1, :mx1],
                    mask_arr,
                )

        return merged_img, merged_roles, spacing_xyz, origin_xyz

    def _load_volume_node(self, path):
        """Load scalar volume with backward-compatible return handling."""
        loaded = slicer.util.loadVolume(str(path))
        if isinstance(loaded, tuple):
            ok, node = loaded
            return bool(ok), node
        if isinstance(loaded, bool):
            return loaded, None
        return loaded is not None, loaded

    def _load_labelmap_node(self, path):
        """Load labelmap volume with backward-compatible return handling."""
        loaded = slicer.util.loadLabelVolume(str(path))
        if isinstance(loaded, tuple):
            ok, node = loaded
            return bool(ok), node
        if isinstance(loaded, bool):
            return loaded, None
        return loaded is not None, loaded

    def _maybe_apply_raw_stack_offset(self, node, record):
        """If imported raw stacks have zero origin, offset by metadata z_start."""
        slice_range = getattr(record, "slice_range", None)
        if node is None or slice_range is None:
            return
        try:
            z_start = int(getattr(slice_range, "z_start"))
            spacing = tuple(float(x) for x in node.GetSpacing())
            origin = list(float(x) for x in node.GetOrigin())
            target_z = float(z_start) * float(spacing[2])
            if abs(origin[2] - target_z) > 1e-6:
                origin[2] = target_z
                node.SetOrigin(origin)
                self._show(
                    f"[load] applied raw stack z-offset: z_start={z_start}, "
                    f"spacing_z={spacing[2]:.6f}, origin_z={target_z:.6f}"
                )
        except Exception as exc:
            self._show(f"[load] could not apply stack offset: {exc}")

    def _on_load_selected(self):
        root = self._dataset_root()
        if root is None:
            slicer.util.errorDisplay("Select a dataset root first.")
            return

        patient_key = self._current_patient_key()
        if patient_key is None:
            slicer.util.errorDisplay("No processed patient available to load.")
            return
        subject_id, site = patient_key

        imported = self._imported_dataset_root()
        if imported is None:
            slicer.util.errorDisplay("Could not resolve derivatives path.")
            return

        data_type = self.loadTypeCombo.currentText
        load_masks_with_images = data_type in {"raw", "transformed"}

        candidates = []
        image_records = []
        try:
            from timelapsedhrpqct.dataset.artifacts import (
                iter_fused_session_records,
                iter_imported_stack_records,
            )

            if data_type == "raw":
                for rec in iter_imported_stack_records(imported):
                    if rec.subject_id == subject_id and rec.site == site and rec.image_path.exists():
                        candidates.append(rec.image_path)
                        image_records.append(rec)
            elif data_type == "transformed":
                for rec in iter_fused_session_records(imported):
                    if rec.subject_id == subject_id and rec.site == site and rec.image_path.exists():
                        candidates.append(rec.image_path)
                        image_records.append(rec)
        except Exception as exc:
            self._show(f"[load] artifact-based lookup failed: {exc}")

        if data_type == "remodelling image":
            viz_dir = None
            try:
                from timelapsedhrpqct.dataset.derivative_paths import analysis_visualize_dir

                viz_dir = analysis_visualize_dir(imported, subject_id, site)
            except Exception as exc:
                self._show(f"[load] derivative path helper unavailable, using filesystem fallback: {exc}")
                viz_dir = (
                    imported
                    / "derivatives"
                    / "TimelapsedHRpQCT"
                    / f"sub-{subject_id}"
                    / f"site-{site}"
                    / "analysis"
                    / "visualize"
                )
            if viz_dir is not None and viz_dir.exists():
                candidates.extend(sorted(viz_dir.glob("*_remodelling.mha")))
                if not candidates:
                    candidates.extend(sorted(viz_dir.glob("*.mha")))

        candidates = sorted(set(candidates))

        if not candidates:
            slicer.util.warningDisplay(
                f"No files found for '{data_type}' in sub-{subject_id} site-{site}."
            )
            return

        loaded = 0

        if image_records:
            for rec in sorted(
                image_records,
                key=lambda r: (
                    str(getattr(r, "session_id", "")),
                    int(getattr(r, "stack_index", 0)),
                    str(getattr(r, "image_path", "")),
                ),
            ):
                p = rec.image_path
                ok, node = self._load_volume_node(p)
                if ok and node is not None:
                    loaded += 1
                    session_id = str(getattr(rec, "session_id", ""))
                    stack_index = getattr(rec, "stack_index", None)
                    folder_id = self._ensure_load_folder(subject_id, site, session_id, stack_index)
                    self._place_node_in_folder(node, folder_id)
                    try:
                        origin = tuple(float(x) for x in node.GetOrigin())
                        spacing = tuple(float(x) for x in node.GetSpacing())
                        self._show(
                            f"[load] {Path(p).name} origin={origin} spacing={spacing}"
                        )
                    except Exception:
                        pass
                    if load_masks_with_images:
                        role_to_path = {}
                        for role, mask_path in (getattr(rec, "mask_paths", {}) or {}).items():
                            if mask_path and Path(mask_path).exists():
                                role_to_path[str(role)] = Path(mask_path)
                        if role_to_path:
                            seg_name = (
                                f"sub-{subject_id}_site-{site}_ses-{session_id}_"
                                f"stack-{int(stack_index):02d}_{data_type}_masks"
                                if stack_index is not None
                                else f"sub-{subject_id}_site-{site}_ses-{session_id}_{data_type}_masks"
                            )
                            self._load_masks_as_segmentation(
                                seg_name,
                                role_to_path,
                                session_id=session_id,
                                folder_item_id=folder_id,
                                reference_volume_node=node,
                            )
                        seg_path = getattr(rec, "seg_path", None)
                        if seg_path and Path(seg_path).exists():
                            seg_name = (
                                f"sub-{subject_id}_site-{site}_ses-{session_id}_"
                                f"stack-{int(stack_index):02d}_{data_type}_seg"
                                if stack_index is not None
                                else f"sub-{subject_id}_site-{site}_ses-{session_id}_{data_type}_seg"
                            )
                            self._load_masks_as_segmentation(
                                seg_name,
                                {"seg": Path(seg_path)},
                                session_id=session_id,
                                folder_item_id=folder_id,
                                reference_volume_node=node,
                            )
        else:
            folder_id = self._ensure_load_folder(subject_id, site)
            for p in candidates:
                if data_type == "remodelling image":
                    seg_name = f"{Path(p).stem}_segmentation"
                    ok = self._load_remodelling_as_segmentation(
                        seg_name,
                        Path(p),
                        folder_item_id=folder_id,
                        preview_axis=self.remodellingAxisCombo.currentText,
                        preview_thickness_vox=int(self.remodellingThicknessSpin.value),
                        detail=int(self.remodellingDetailSlider.value),
                        create_full=True,
                        create_preview=False,
                    )
                    if ok:
                        loaded += 1
                        self._show(f"[load] {Path(p).name} loaded as full remodelling segmentation (2D).")
                else:
                    ok, node = self._load_volume_node(p)
                    if ok and node is not None:
                        loaded += 1
                        self._place_node_in_folder(node, folder_id)
                        try:
                            origin = tuple(float(x) for x in node.GetOrigin())
                            spacing = tuple(float(x) for x in node.GetSpacing())
                            self._show(
                                f"[load] {Path(p).name} origin={origin} spacing={spacing}"
                            )
                        except Exception:
                            pass

        self._show(
            f"[load] loaded {loaded}/{len(candidates)} files for "
            f"sub-{subject_id} site-{site} ({data_type})"
        )

    def _load_masks_as_segmentation(
        self,
        segmentation_name,
        role_to_path,
        session_id=None,
        folder_item_id=None,
        reference_volume_node=None,
    ):
        seg_node = slicer.mrmlScene.AddNewNodeByClass("vtkMRMLSegmentationNode", segmentation_name)
        seg_node.CreateDefaultDisplayNodes()
        if reference_volume_node is not None:
            seg_node.SetReferenceImageGeometryParameterFromVolumeNode(reference_volume_node)
        seg_logic = slicer.modules.segmentations.logic()
        loaded_any = False
        base_color = self._session_base_color(session_id or "")
        role_tint = {
            "full": 1.00,
            "trab": 0.85,
            "cort": 0.70,
            "regmask": 0.55,
        }

        for role in sorted(role_to_path.keys()):
            path = role_to_path[role]
            ok, label_node = self._load_labelmap_node(path)
            if not ok or label_node is None:
                self._show(f"[load] failed to load mask role '{role}' from {path}")
                continue

            existing_ids = vtk.vtkStringArray()
            seg_node.GetSegmentation().GetSegmentIDs(existing_ids)
            before = {existing_ids.GetValue(i) for i in range(existing_ids.GetNumberOfValues())}

            seg_logic.ImportLabelmapToSegmentationNode(label_node, seg_node)
            slicer.mrmlScene.RemoveNode(label_node)

            updated_ids = vtk.vtkStringArray()
            seg_node.GetSegmentation().GetSegmentIDs(updated_ids)
            after = {updated_ids.GetValue(i) for i in range(updated_ids.GetNumberOfValues())}
            new_ids = list(after - before)
            for seg_id in new_ids:
                segment = seg_node.GetSegmentation().GetSegment(seg_id)
                if segment:
                    segment.SetName(str(role))
                    tint = role_tint.get(str(role).lower(), 0.9)
                    segment.SetColor(
                        float(min(max(base_color[0] * tint, 0.0), 1.0)),
                        float(min(max(base_color[1] * tint, 0.0), 1.0)),
                        float(min(max(base_color[2] * tint, 0.0), 1.0)),
                    )
            loaded_any = loaded_any or bool(new_ids)

        if not loaded_any:
            slicer.mrmlScene.RemoveNode(seg_node)
            return False
        self._configure_segmentation_display(seg_node)
        if folder_item_id is not None:
            self._place_node_in_folder(seg_node, folder_item_id)
        return True


class TimelapsedHRpQCTTest(ScriptedLoadableModuleTest):
    """Minimal smoke tests for release readiness."""

    def setUp(self):
        slicer.mrmlScene.Clear()

    def runTest(self):
        self.setUp()
        self.test_logic_config_resolution()
        self.test_override_config_write()

    def test_logic_config_resolution(self):
        logic = TimelapsedHRpQCTLogic()
        config_path = logic.default_config_path()
        self.assertTrue(Path(config_path).exists())
        self.assertTrue(str(config_path).endswith(".yml"))

    def test_override_config_write(self):
        logic = TimelapsedHRpQCTLogic()
        path = logic.create_override_config(
            {
                "analysis": {"thresholds": [225.0], "cluster_sizes": [12]},
                "fusion": {"enable_filling": False},
            }
        )
        p = Path(path)
        self.assertTrue(p.exists())
        text = p.read_text(encoding="utf-8")
        self.assertIn("analysis:", text)
        self.assertIn("fusion:", text)
