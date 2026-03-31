import os
import shutil
import tempfile
from pathlib import Path

import qt
import ctk
import slicer
from slicer.ScriptedLoadableModule import (
    ScriptedLoadableModule,
    ScriptedLoadableModuleWidget,
    ScriptedLoadableModuleLogic,
)


class _DropArea(qt.QListWidget):
    """Simple drag-and-drop path collector."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setSelectionMode(qt.QAbstractItemView.ExtendedSelection)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            super().dragEnterEvent(event)

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            super().dragMoveEvent(event)

    def dropEvent(self, event):
        if not event.mimeData().hasUrls():
            super().dropEvent(event)
            return

        for url in event.mimeData().urls():
            local = url.toLocalFile()
            if local:
                self._add_path(local)
        event.acceptProposedAction()

    def _add_path(self, path):
        existing = {self.item(i).text() for i in range(self.count)}
        if path not in existing:
            self.addItem(path)

    def paths(self):
        return [self.item(i).text() for i in range(self.count)]


class TimelapsedHRpQCT(ScriptedLoadableModule):
    def __init__(self, parent):
        super().__init__(parent)
        parent.title = "TimelapsedHRpQCT"
        parent.categories = ["Bone"]
        parent.dependencies = []
        parent.contributors = ["Matthias Walle", "Codex"]
        parent.helpText = """GUI wrapper for timelapsed-hrpqct pipeline."""
        parent.acknowledgementText = """Built for streamlined longitudinal HR-pQCT workflows."""


class TimelapsedHRpQCTLogic(ScriptedLoadableModuleLogic):
    def __init__(self):
        super().__init__()
        self._proc = None
        self._temp_config_path = None

    def is_pipeline_available(self):
        try:
            import timelapsedhrpqct  # noqa: F401
            return True
        except Exception:
            return False

    def install_or_update_pipeline(self):
        slicer.util.pip_install("timelapsed-hrpqct")

    def default_config_path(self):
        import timelapsedhrpqct

        return Path(timelapsedhrpqct.__file__).resolve().parent / "configs" / "defaults.yml"

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

        proc = qt.QProcess()
        proc.setProcessChannelMode(qt.QProcess.MergedChannels)

        def _read_output():
            text = bytes(proc.readAll()).decode("utf-8", errors="replace")
            if on_output and text:
                on_output(text)

        def _finished(exit_code, exit_status):
            self._proc = None
            if on_finished:
                on_finished(exit_code, exit_status)

        proc.readyRead.connect(_read_output)
        proc.finished.connect(_finished)

        python_exe = shutil.which("PythonSlicer") or shutil.which("python") or shutil.which("python3")
        if python_exe is None:
            raise RuntimeError("Could not find Python executable in Slicer environment")

        full_args = ["-m", "timelapsedhrpqct.cli"] + args
        proc.start(python_exe, full_args)

        if not proc.waitForStarted(3000):
            raise RuntimeError("Failed to start timelapsed-hrpqct process")

        self._proc = proc


class TimelapsedHRpQCTWidget(ScriptedLoadableModuleWidget):
    def setup(self):
        super().setup()
        self.logic = TimelapsedHRpQCTLogic()
        self._queued_commands = []

        self._build_ui()
        self._refresh_patient_list()

    def _build_ui(self):
        depBox = ctk.ctkCollapsibleButton()
        depBox.text = "Dependency"
        depForm = qt.QFormLayout(depBox)
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

        self.inputPath = ctk.ctkPathLineEdit()
        self.inputPath.filters = ctk.ctkPathLineEdit.Dirs
        self.inputPath.setCurrentPath("")
        form.addRow("Dataset root", self.inputPath)

        self.dropArea = _DropArea()
        self.dropArea.setMinimumHeight(80)
        form.addRow("Drop files/folders", self.dropArea)

        parseBtn = qt.QPushButton("Parse input")
        parseBtn.clicked.connect(self._on_parse)
        form.addRow(parseBtn)

        self.parseSummaryLabel = qt.QLabel("Parse summary: not run")
        self.parseSummaryLabel.wordWrap = True
        form.addRow(self.parseSummaryLabel)

        self.parseTable = qt.QTableWidget()
        self.parseTable.setColumnCount(7)
        self.parseTable.setHorizontalHeaderLabels(
            ["Subject", "Site", "Session", "Stack", "Image", "Masks", "Seg"]
        )
        self.parseTable.horizontalHeader().setStretchLastSection(True)
        self.parseTable.setMinimumHeight(160)
        self.parseTable.setEditTriggers(qt.QAbstractItemView.NoEditTriggers)

        settingsBox = ctk.ctkCollapsibleButton()
        settingsBox.text = "Exposed Settings"
        settingsForm = qt.QFormLayout(settingsBox)

        self.maskMethod = qt.QComboBox()
        self.maskMethod.addItems(["adaptive", "global"])
        self.maskLow = ctk.ctkDoubleSpinBox(); self.maskLow.value = 100
        self.maskHigh = ctk.ctkDoubleSpinBox(); self.maskHigh.value = 300
        settingsForm.addRow("Mask method", self.maskMethod)
        settingsForm.addRow("Mask lower threshold", self.maskLow)
        settingsForm.addRow("Mask higher threshold", self.maskHigh)

        self.regMetric = qt.QComboBox(); self.regMetric.addItems(["mattes", "correlation"])
        self.regSampling = ctk.ctkDoubleSlider(); self.regSampling.minimum = 0.0001; self.regSampling.maximum = 0.05; self.regSampling.value = 0.001
        self.regSampling.decimals = 4
        settingsForm.addRow("Registration metric", self.regMetric)
        settingsForm.addRow("Sampling %", self.regSampling)

        self.tlRes = ctk.ctkSpinBox(); self.tlRes.minimum = 1; self.tlRes.maximum = 10; self.tlRes.value = 4
        self.tlIter = ctk.ctkSpinBox(); self.tlIter.minimum = 1; self.tlIter.maximum = 5000; self.tlIter.value = 250
        settingsForm.addRow("Timelapse resolutions", self.tlRes)
        settingsForm.addRow("Timelapse iterations", self.tlIter)

        self.msRes = ctk.ctkSpinBox(); self.msRes.minimum = 1; self.msRes.maximum = 10; self.msRes.value = 4
        self.msIter = ctk.ctkSpinBox(); self.msIter.minimum = 1; self.msIter.maximum = 5000; self.msIter.value = 250
        settingsForm.addRow("Multistack resolutions", self.msRes)
        settingsForm.addRow("Multistack iterations", self.msIter)

        self.analysisThreshold = ctk.ctkDoubleSpinBox(); self.analysisThreshold.value = 225
        self.analysisCluster = ctk.ctkSpinBox(); self.analysisCluster.minimum = 1; self.analysisCluster.maximum = 1000; self.analysisCluster.value = 12
        self.mineralizationToggle = qt.QCheckBox("Enable mineralisation labels")
        self.mineralizationToggle.checked = False
        settingsForm.addRow("Analysis threshold", self.analysisThreshold)
        settingsForm.addRow("Cluster size", self.analysisCluster)
        settingsForm.addRow(self.mineralizationToggle)

        actionLayout = qt.QHBoxLayout()
        self.runMasksBtn = qt.QPushButton("Run mask generation")
        self.runTimelapseBtn = qt.QPushButton("Run timelapse")
        self.runMultistackBtn = qt.QPushButton("Run multistack")
        self.runAnalysisBtn = qt.QPushButton("Analyze")
        for b in [self.runMasksBtn, self.runTimelapseBtn, self.runMultistackBtn, self.runAnalysisBtn]:
            actionLayout.addWidget(b)

        self.runMasksBtn.clicked.connect(self._on_run_masks)
        self.runTimelapseBtn.clicked.connect(self._on_run_timelapse)
        self.runMultistackBtn.clicked.connect(self._on_run_multistack)
        self.runAnalysisBtn.clicked.connect(self._on_run_analysis)

        loadBox = ctk.ctkCollapsibleButton()
        loadBox.text = "Load Processed Data"
        loadForm = qt.QFormLayout(loadBox)
        self.patientCombo = qt.QComboBox()
        self.loadTypeCombo = qt.QComboBox()
        self.loadTypeCombo.addItems(["raw", "transformed", "remodelling image"])
        self.loadDataBtn = qt.QPushButton("Load selected")
        self.loadDataBtn.clicked.connect(self._on_load_selected)
        loadForm.addRow("Patient", self.patientCombo)
        loadForm.addRow("Data type", self.loadTypeCombo)
        loadForm.addRow(self.loadDataBtn)

        self.logText = qt.QPlainTextEdit()
        self.logText.readOnly = True
        self.logText.setMinimumHeight(200)

        self.layout.addLayout(form)
        self.layout.addWidget(self.parseTable)
        self.layout.addWidget(settingsBox)
        self.layout.addLayout(actionLayout)
        self.layout.addWidget(loadBox)
        self.layout.addWidget(self.logText)
        self.layout.addStretch(1)
        self._update_dependency_ui()

    def _dataset_root(self):
        p = self.inputPath.currentPath.strip()
        if p:
            return Path(p)

        dropped = [Path(x) for x in self.dropArea.paths()]
        if not dropped:
            return None
        if all(d.is_file() for d in dropped):
            return Path(os.path.commonpath([str(d.parent) for d in dropped]))
        return Path(os.path.commonpath([str(d) for d in dropped]))

    def _show(self, text):
        message = text.rstrip()
        if hasattr(self, "logText") and self.logText is not None:
            self.logText.appendPlainText(message)
        else:
            print(message)

    def _settings_override(self):
        is_mineral = bool(self.mineralizationToggle.checked)
        label_map = {
            "resorption": 1,
            "quiescent": 2,
            "formation": 3,
        }
        if is_mineral:
            label_map["demineralisation"] = 2
            label_map["mineralisation"] = 2

        if self.regSampling.value > 0.01:
            self._show("[warning] Sampling > 0.01 can be slow or unstable on some datasets.")

        return {
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
                "sampling_percentage": float(self.regSampling.value),
                "number_of_resolutions": int(self.tlRes.value),
                "number_of_iterations": int(self.tlIter.value),
            },
            "multistack_correction": {
                "metric": self.regMetric.currentText,
                "sampling_percentage": float(self.regSampling.value),
                "number_of_resolutions": int(self.msRes.value),
                "number_of_iterations": int(self.msIter.value),
            },
            "analysis": {
                "thresholds": [float(self.analysisThreshold.value)],
                "cluster_sizes": [int(self.analysisCluster.value)],
            },
            "visualization": {
                "threshold": float(self.analysisThreshold.value),
                "cluster_size": int(self.analysisCluster.value),
                "label_map": label_map,
            },
        }

    def _run(self, args):
        try:
            self.logic.run_cli(args, on_output=self._show, on_finished=self._on_finished)
            self._show("[timelapsed-slicer] started: " + " ".join(args))
        except Exception as exc:
            slicer.util.errorDisplay(str(exc))

    def _run_sequence(self, commands):
        if not commands:
            return
        self._queued_commands = [list(cmd) for cmd in commands[1:]]
        self._run(commands[0])

    def _on_parse(self):
        if not self.logic.is_pipeline_available():
            slicer.util.errorDisplay("Please install timelapsed-hrpqct first.")
            return
        root = self._dataset_root()
        if root is None:
            slicer.util.errorDisplay("Select or drop a dataset folder first.")
            return

        sessions, err = self.logic.parse_input(root)
        if err:
            self.parseTable.setRowCount(0)
            self.parseSummaryLabel.text = "Parse summary: failed"
            self.parseSummaryLabel.styleSheet = "color: #cc5500;"
            msg = (
                f"Could not parse input from: {root}\n\n"
                f"Error:\n{err}\n\n"
                "Expected filename pattern examples:\n"
                "SUBJ001_DT_T1.AIM\n"
                "SUBJ001_DT_STACK01_T1.AIM\n"
                "SUBJ001_DT_T1_TRAB_MASK.AIM\n"
                "SUBJ001_DT_T1_CORT_MASK.AIM\n"
                "SUBJ001_DT_T1_REGMASK.AIM\n"
                "SUBJ001_DT_T1_ROI1.AIM"
            )
            slicer.util.warningDisplay(msg)
            return

        self._show(f"[parse] discovered {len(sessions)} sessions under {root}")
        self._populate_parse_table(sessions)

    def _populate_parse_table(self, sessions):
        self.parseTable.setRowCount(len(sessions))
        self.parseSummaryLabel.text = f"Parse summary: {len(sessions)} session(s) discovered"
        self.parseSummaryLabel.styleSheet = "color: #228b22;"

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

            values = [subject, site, session_id, stack_text, image_name, mask_roles, seg_text]
            for col, value in enumerate(values):
                self.parseTable.setItem(row, col, qt.QTableWidgetItem(value))

        self.parseTable.resizeColumnsToContents()

    def _on_run_masks(self):
        if not self.logic.is_pipeline_available():
            slicer.util.errorDisplay("Please install timelapsed-hrpqct first.")
            return
        root = self._dataset_root()
        if root is None:
            slicer.util.errorDisplay("Select a dataset root first.")
            return

        cfg = self.logic.create_override_config(self._settings_override())
        imported = Path(root) / "imported_dataset"
        self._run_sequence(
            [
                ["import", str(root), "--config", cfg],
                ["generate-masks", str(imported), "--config", cfg],
            ]
        )

    def _on_run_timelapse(self):
        if not self.logic.is_pipeline_available():
            slicer.util.errorDisplay("Please install timelapsed-hrpqct first.")
            return
        root = self._dataset_root()
        if root is None:
            slicer.util.errorDisplay("Select a dataset root first.")
            return

        cfg = self.logic.create_override_config(self._settings_override())
        self._run(["run", str(root), "--mode", "regular", "--config", cfg])

    def _on_run_multistack(self):
        if not self.logic.is_pipeline_available():
            slicer.util.errorDisplay("Please install timelapsed-hrpqct first.")
            return
        root = self._dataset_root()
        if root is None:
            slicer.util.errorDisplay("Select a dataset root first.")
            return

        cfg = self.logic.create_override_config(self._settings_override())
        self._run(["run", str(root), "--mode", "multistack", "--config", cfg])

    def _on_run_analysis(self):
        if not self.logic.is_pipeline_available():
            slicer.util.errorDisplay("Please install timelapsed-hrpqct first.")
            return
        root = self._dataset_root()
        if root is None:
            slicer.util.errorDisplay("Select a dataset root first.")
            return

        imported = Path(root) / "imported_dataset"
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

    def _on_finished(self, exit_code, exit_status):
        self._show(f"[timelapsed-slicer] finished with exit code {exit_code}")
        if self._queued_commands and exit_code == 0:
            next_cmd = self._queued_commands.pop(0)
            self._run(next_cmd)
            return
        self._queued_commands = []
        self._refresh_patient_list()

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
        root = self._dataset_root()
        self.patientCombo.clear()
        if root is None:
            return

        derivatives = Path(root) / "imported_dataset" / "derivatives" / "TimelapsedHRpQCT"
        if not derivatives.exists():
            return

        subjects = sorted([p.name for p in derivatives.glob("sub-*") if p.is_dir()])
        self.patientCombo.addItems(subjects)

    def _on_load_selected(self):
        root = self._dataset_root()
        if root is None:
            slicer.util.errorDisplay("Select a dataset root first.")
            return

        patient = self.patientCombo.currentText
        if not patient:
            slicer.util.errorDisplay("No processed patient available to load.")
            return

        derivatives = Path(root) / "imported_dataset" / "derivatives" / "TimelapsedHRpQCT" / patient
        if not derivatives.exists():
            slicer.util.errorDisplay(f"No derivatives found for {patient}")
            return

        data_type = self.loadTypeCombo.currentText

        candidates = []
        if data_type == "raw":
            candidates = sorted(derivatives.glob("ses-*/stacks/*_image.mha"))
        elif data_type == "transformed":
            candidates = sorted(derivatives.glob("transformed/ses-*/images/*.mha"))
            if not candidates:
                candidates = sorted(derivatives.glob("transformed/**/*.mha"))
        else:
            candidates = sorted(derivatives.glob("analysis/**/*.mha"))
            if not candidates:
                candidates = sorted(derivatives.glob("analysis/**/*.nii*"))

        if not candidates:
            slicer.util.warningDisplay(f"No files found for '{data_type}' in {patient}")
            return

        loaded = 0
        for p in candidates:
            if slicer.util.loadVolume(str(p), returnNode=True)[0]:
                loaded += 1

        self._show(f"[load] loaded {loaded}/{len(candidates)} files for {patient} ({data_type})")
