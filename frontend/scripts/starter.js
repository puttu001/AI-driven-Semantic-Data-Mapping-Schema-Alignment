const starterForm = document.getElementById("starterForm");
const cdmInput = document.getElementById("cdmFile");
const mappingInput = document.getElementById("mappingFile");
const cdmFileLabel = document.getElementById("cdmFileLabel");
const mappingFileLabel = document.getElementById("mappingFileLabel");
const runButton = document.getElementById("runProcessButton");
const statusMessage = document.getElementById("statusMessage");
const demoModeCheckbox = document.getElementById("demoModeCheckbox");
const demoDownloadSelect = document.getElementById("demoDownloadSelect");
const demoDownloadButton = document.getElementById("demoDownloadButton");
const starterCard = document.getElementById("starterCard");
const uploadBoxes = document.querySelectorAll(".upload-box");

const defaultCdmLabel = "Drop file or click to upload";
const defaultMappingLabel = "Drop file or click to upload";

let usingDemoFiles = false;
let isSubmitting = false;

function buildApiUrl(path) {
  // Since frontend is now served from FastAPI backend,
  // all API calls should be to the same server
  return path;
}

function updateFileLabel(inputElement, labelElement, fallbackText) {
  if (inputElement.files && inputElement.files[0]) {
    labelElement.textContent = inputElement.files[0].name;
    return;
  }
  labelElement.textContent = fallbackText;
}

function setStatus(message, statusType = "info") {
  statusMessage.textContent = message;
  statusMessage.classList.remove("info", "success", "error");
  if (message) {
    statusMessage.classList.add(statusType);
  }
}

function canRunProcess() {
  if (usingDemoFiles) {
    return true;
  }
  return Boolean(cdmInput.files && cdmInput.files[0] && mappingInput.files && mappingInput.files[0]);
}

function updateRunButton() {
  runButton.disabled = isSubmitting || !canRunProcess();
  runButton.textContent = isSubmitting ? "Submitting..." : "Submit";
}

function clearUploads() {
  cdmInput.value = "";
  mappingInput.value = "";
  updateFileLabel(cdmInput, cdmFileLabel, defaultCdmLabel);
  updateFileLabel(mappingInput, mappingFileLabel, defaultMappingLabel);
}

function toggleDemoMode(enabled) {
  usingDemoFiles = enabled;
  cdmInput.disabled = enabled;
  mappingInput.disabled = enabled;
  starterCard.classList.toggle("demo-mode", enabled);

  if (enabled) {
    clearUploads();
  }

  updateRunButton();
}

function attachDragAndDrop() {
  uploadBoxes.forEach((uploadBox) => {
    const targetInputId = uploadBox.getAttribute("data-input");
    const targetInput = document.getElementById(targetInputId);
    if (!targetInput) {
      return;
    }

    uploadBox.addEventListener("dragover", (event) => {
      if (targetInput.disabled) {
        return;
      }
      event.preventDefault();
      uploadBox.classList.add("drag-over");
    });

    uploadBox.addEventListener("dragleave", () => {
      uploadBox.classList.remove("drag-over");
    });

    uploadBox.addEventListener("drop", (event) => {
      if (targetInput.disabled) {
        return;
      }
      event.preventDefault();
      uploadBox.classList.remove("drag-over");

      const [file] = event.dataTransfer.files;
      if (!file) {
        return;
      }

      const fileTransfer = new DataTransfer();
      fileTransfer.items.add(file);
      targetInput.files = fileTransfer.files;
      targetInput.dispatchEvent(new Event("change"));
    });
  });
}

async function runMappingProcess(event) {
  event.preventDefault();

  if (!canRunProcess()) {
    setStatus("Please upload both files or enable demo mode.", "error");
    return;
  }

  isSubmitting = true;
  updateRunButton();
  setStatus("Starting mapping process. This can take a few minutes.", "info");

  const requestFormData = new FormData();
  const apiKey = "";
  const mongoUri = "";

  if (apiKey) {
    requestFormData.append("api_key", apiKey);
  }
  if (mongoUri) {
    requestFormData.append("mongodb_uri", mongoUri);
  }

  if (!usingDemoFiles) {
    requestFormData.append("cdm_file", cdmInput.files[0]);
    requestFormData.append("mapping_file", mappingInput.files[0]);
  }

  try {
    const apiUrl = buildApiUrl("/api/v1/run-mapping-interactive");
    console.log("📤 Sending request to:", apiUrl);
    
    const response = await fetch(apiUrl, {
      method: "POST",
      body: requestFormData
    });

    console.log("📥 Response status:", response.status);
    
    const payload = await response.json().catch(() => ({}));
    console.log("📦 Response payload:", payload);
    
    if (!response.ok) {
      throw new Error(payload.detail || payload.message || "Failed to start mapping process.");
    }

    const sessionId = payload.session_id || "n/a";
    console.log("✅ Session ID received:", sessionId);
    
    localStorage.setItem("mapping_session_id", sessionId);
    localStorage.setItem("mapping_input_mode", usingDemoFiles ? "demo" : "upload");

    setStatus(
      `Mapping started successfully. Session ID: ${sessionId}. Redirecting...`,
      "success"
    );

    console.log("⏱️  Setting redirect timer for 1 second...");
    
    // Navigate to processing page immediately
    const redirectUrl = `/pages/processing.html?t=${Date.now()}`;
    console.log("🔄 Redirecting to:", redirectUrl, "Current URL:", window.location.href);
    window.location.href = redirectUrl;
  } catch (error) {
    console.error("❌ Error during submission:", error);
    setStatus(error.message || "Unexpected error while starting mapping process.", "error");
  } finally {
    isSubmitting = false;
    updateRunButton();
  }
}

function triggerDownload(url) {
  const downloadAnchor = document.createElement("a");
  downloadAnchor.href = url;
  downloadAnchor.style.display = "none";
  document.body.appendChild(downloadAnchor);
  downloadAnchor.click();
  downloadAnchor.remove();
}

function downloadSelectedDemoFiles() {
  const selectedValue = demoDownloadSelect.value;
  if (!selectedValue) {
    setStatus("Select a demo file option from the dropdown.", "info");
    return;
  }

  if (selectedValue === "cdm") {
    triggerDownload(buildApiUrl("/api/v1/demo-files/download?kind=cdm"));
    setStatus("Downloading demo CDM file.", "success");
    return;
  }

  if (selectedValue === "mapping") {
    triggerDownload(buildApiUrl("/api/v1/demo-files/download?kind=mapping"));
    setStatus("Downloading demo mapping file.", "success");
    return;
  }

  triggerDownload(buildApiUrl("/api/v1/demo-files/download?kind=cdm"));
  setTimeout(() => {
    triggerDownload(buildApiUrl("/api/v1/demo-files/download?kind=mapping"));
  }, 300);
  setStatus("Downloading both demo files.", "success");
}

cdmInput.addEventListener("change", () => {
  updateFileLabel(cdmInput, cdmFileLabel, defaultCdmLabel);
  updateRunButton();
});

mappingInput.addEventListener("change", () => {
  updateFileLabel(mappingInput, mappingFileLabel, defaultMappingLabel);
  updateRunButton();
});

demoModeCheckbox.addEventListener("change", (event) => {
  toggleDemoMode(event.target.checked);
  setStatus(
    event.target.checked
      ? "Demo mode enabled. You can now run mapping without uploading files."
      : "Demo mode disabled. Upload both files to continue.",
    "info"
  );
});

demoDownloadButton.addEventListener("click", downloadSelectedDemoFiles);

starterForm.addEventListener("submit", runMappingProcess);

attachDragAndDrop();
toggleDemoMode(false);
setStatus("Upload your files or enable demo mode to begin.", "info");
