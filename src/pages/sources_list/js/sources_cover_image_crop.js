(function () {
  const CROP_VIEW_WIDTH = 360;
  const CROP_VIEW_HEIGHT = 270;
  const CROP_OUTPUT_WIDTH = 360;
  const CROP_OUTPUT_HEIGHT = 270;
  const MAX_ZOOM = 6;
  const ZOOM_STEPS = 1000;
  const WHEEL_SENSITIVITY = 0.0018;

  const TARGETS = [
    {
      uploadId: "sources-create-cover-image-plus-btn",
      dataId: "sources-create-cover-image-data",
      previewId: "sources-create-cover-image-preview",
    },
    {
      uploadId: "sources-edit-cover-image-plus-btn",
      dataId: "sources-edit-cover-image-data",
      previewId: "sources-edit-cover-image-preview",
    },
  ];

  const PREVIEW_EMPTY_HTML =
    "<div class='sources-cover-image-preview-shell sources-cover-image-preview-shell--clickable' " +
    "role='button' tabindex='0' aria-label='Select source image'>" +
      "<span class='sources-cover-image-preview-empty'>No image selected yet.</span>" +
    "</div>";

  let cropUi = null;
  let cropState = null;
  let bypassInput = null;
  let pendingTarget = null;
  let pendingTargetAt = 0;

  const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

  const getComponentInput = (componentId) =>
    document.querySelector(
      `#${componentId} textarea, #${componentId} input[type="text"], #${componentId} input[type="hidden"], #${componentId} input:not([type])`,
    );

  const setComponentValue = (componentId, value) => {
    const input = getComponentInput(componentId);
    if (!(input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement)) return;
    const nextValue = String(value ?? "");
    if ((input.value || "") === nextValue) return;
    input.value = nextValue;
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
  };

  const getComponentValue = (componentId) => {
    const input = getComponentInput(componentId);
    if (!(input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement)) return "";
    return String(input.value || "");
  };

  const extractFileInputFromEvent = (event) => {
    if (event?.target instanceof HTMLInputElement && event.target.type === "file") {
      return event.target;
    }
    if (typeof event?.composedPath !== "function") return null;
    const path = event.composedPath();
    for (const node of path) {
      if (node instanceof HTMLInputElement && node.type === "file") {
        return node;
      }
    }
    return null;
  };

  const targetForInput = (inputNode) => {
    if (!(inputNode instanceof HTMLInputElement)) return null;
    for (const target of TARGETS) {
      const host = document.getElementById(target.uploadId);
      if (!(host instanceof HTMLElement)) continue;
      if (host.contains(inputNode) || host.shadowRoot?.contains(inputNode)) {
        return target;
      }
    }
    return null;
  };

  const getUploadHost = (target) => {
    if (!target?.uploadId) return null;
    const host = document.getElementById(target.uploadId);
    return host instanceof HTMLElement ? host : null;
  };

  const getUploadInput = (target) => {
    const host = getUploadHost(target);
    if (!(host instanceof HTMLElement)) return null;
    return (
      host.querySelector("input[type='file']") ||
      host.shadowRoot?.querySelector("input[type='file']") ||
      null
    );
  };

  const openUploadPicker = (target) => {
    pendingTarget = null;
    pendingTargetAt = 0;
    const input = getUploadInput(target);
    if (input instanceof HTMLInputElement) {
      try {
        if (typeof input.showPicker === "function") {
          pendingTarget = target || null;
          pendingTargetAt = Date.now();
          input.showPicker();
          return true;
        }
      } catch (error) {
        void error;
      }
      try {
        pendingTarget = target || null;
        pendingTargetAt = Date.now();
        input.click();
        return true;
      } catch (error) {
        void error;
      }
    }

    const host = getUploadHost(target);
    if (host instanceof HTMLElement) {
      try {
        pendingTarget = target || null;
        pendingTargetAt = Date.now();
        host.click();
        return true;
      } catch (error) {
        void error;
      }
    }
    return false;
  };

  const getPreviewHost = (previewId) => {
    const root = document.getElementById(previewId);
    if (!(root instanceof HTMLElement)) return null;
    const existingShell = root.querySelector(".sources-cover-image-preview-shell");
    if (existingShell instanceof HTMLElement && existingShell.parentElement instanceof HTMLElement) {
      return existingShell.parentElement;
    }
    const proseNode = root.querySelector(".prose");
    if (proseNode instanceof HTMLElement) return proseNode;
    const contentNode = root.firstElementChild;
    return contentNode instanceof HTMLElement ? contentNode : root;
  };

  const setPreviewMarkup = (previewId, markup) => {
    const node = getPreviewHost(previewId);
    if (!(node instanceof HTMLElement)) return;
    const nextMarkup = String(markup || "");
    if (node.innerHTML === nextMarkup) return;
    node.innerHTML = nextMarkup;
  };

  const isImageDataUrl = (value) => /^data:image\/[A-Za-z0-9.+-]+;base64,/i.test(String(value || "").trim());

  const escapeHtmlAttr = (value) =>
    String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");

  const buildImagePreviewMarkup = (src) =>
    "<div class='sources-cover-image-preview-shell sources-cover-image-preview-shell--clickable' " +
    "role='button' tabindex='0' aria-label='Change source image'>" +
      `<img class='sources-cover-image-preview-img' src='${escapeHtmlAttr(src)}' alt='Source cover image preview' />` +
    "</div>";

  const readStoredDataUrl = (target) => {
    if (!target?.dataId) return "";
    const rawValue = getComponentValue(target.dataId).trim();
    return isImageDataUrl(rawValue) ? rawValue : "";
  };

  const applyPreviewFromDataUrl = (target, dataUrl) => {
    const resolvedUrl = String(dataUrl || "").trim();
    if (!target?.previewId || !isImageDataUrl(resolvedUrl)) return;
    const root = document.getElementById(target.previewId);
    if (root instanceof HTMLElement) {
      const currentImage = root.querySelector(".sources-cover-image-preview-img");
      if (currentImage instanceof HTMLImageElement && currentImage.getAttribute("src") === resolvedUrl) {
        return;
      }
    }
    setPreviewMarkup(target.previewId, buildImagePreviewMarkup(resolvedUrl));
  };

  const sliderToZoom = (sliderValue) => {
    const slider = Number.parseFloat(String(sliderValue || 0));
    const ratio = clamp(slider / ZOOM_STEPS, 0, 1);
    return 1 + (MAX_ZOOM - 1) * ratio;
  };

  const zoomToSlider = (zoom) => {
    const ratio = clamp((zoom - 1) / (MAX_ZOOM - 1), 0, 1);
    return Math.round(ratio * ZOOM_STEPS);
  };

  const fileToDataUrl = (file) =>
    new Promise((resolve, reject) => {
      if (!(file instanceof Blob)) {
        reject(new Error("Invalid file payload."));
        return;
      }
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ""));
      reader.onerror = () => reject(new Error("Could not encode cropped image."));
      reader.readAsDataURL(file);
    });

  const loadImageForCrop = (objectUrl) =>
    new Promise((resolve, reject) => {
      const image = new Image();
      image.onload = () => resolve(image);
      image.onerror = () => reject(new Error("Image could not be loaded for cropping."));
      image.src = objectUrl;
    });

  const getScale = (state) => {
    const baseScale = Math.max(
      CROP_VIEW_WIDTH / Math.max(1, state.imageWidth),
      CROP_VIEW_HEIGHT / Math.max(1, state.imageHeight),
    );
    return baseScale * Math.max(1, state.zoom);
  };

  const clampOffsets = (state) => {
    const scale = getScale(state);
    const drawWidth = state.imageWidth * scale;
    const drawHeight = state.imageHeight * scale;
    const minOffsetX = Math.min(0, CROP_VIEW_WIDTH - drawWidth);
    const minOffsetY = Math.min(0, CROP_VIEW_HEIGHT - drawHeight);
    state.scale = scale;
    state.drawWidth = drawWidth;
    state.drawHeight = drawHeight;
    state.offsetX = clamp(state.offsetX, minOffsetX, 0);
    state.offsetY = clamp(state.offsetY, minOffsetY, 0);
  };

  const getCanvasPoint = (clientX, clientY) => {
    const canvas = cropUi?.canvas;
    if (!(canvas instanceof HTMLCanvasElement)) {
      return { x: CROP_VIEW_WIDTH / 2, y: CROP_VIEW_HEIGHT / 2, scaleX: 1, scaleY: 1 };
    }
    const rect = canvas.getBoundingClientRect();
    if (!rect.width || !rect.height) {
      return { x: CROP_VIEW_WIDTH / 2, y: CROP_VIEW_HEIGHT / 2, scaleX: 1, scaleY: 1 };
    }
    const scaleX = CROP_VIEW_WIDTH / rect.width;
    const scaleY = CROP_VIEW_HEIGHT / rect.height;
    return {
      x: clamp((clientX - rect.left) * scaleX, 0, CROP_VIEW_WIDTH),
      y: clamp((clientY - rect.top) * scaleY, 0, CROP_VIEW_HEIGHT),
      scaleX,
      scaleY,
    };
  };

  const teardownState = ({ clearInput = false } = {}) => {
    if (!cropState) return;
    if (clearInput && cropState.fileInput instanceof HTMLInputElement) {
      cropState.fileInput.value = "";
    }
    if (cropState.objectUrl) {
      URL.revokeObjectURL(cropState.objectUrl);
    }
    if (cropUi?.canvas instanceof HTMLCanvasElement) {
      cropUi.canvas.classList.remove("is-dragging");
    }
    cropState = null;
  };

  const syncUi = () => {
    if (!cropState || !cropUi) return;
    cropUi.zoomRange.value = String(zoomToSlider(cropState.zoom));
    cropUi.zoomValue.textContent = `${Math.round(cropState.zoom * 100)}%`;
  };

  const renderCrop = () => {
    if (!cropState || !cropUi) return;
    const context = cropUi.canvas.getContext("2d");
    if (!context) return;
    clampOffsets(cropState);
    context.clearRect(0, 0, CROP_VIEW_WIDTH, CROP_VIEW_HEIGHT);
    context.fillStyle = "#f1f5f9";
    context.fillRect(0, 0, CROP_VIEW_WIDTH, CROP_VIEW_HEIGHT);
    context.imageSmoothingEnabled = true;
    context.imageSmoothingQuality = "high";
    context.drawImage(
      cropState.image,
      cropState.offsetX,
      cropState.offsetY,
      cropState.drawWidth,
      cropState.drawHeight,
    );
    syncUi();
  };

  const setZoom = (nextZoom, focusX = CROP_VIEW_WIDTH / 2, focusY = CROP_VIEW_HEIGHT / 2) => {
    if (!cropState) return;
    clampOffsets(cropState);
    const safeZoom = clamp(nextZoom, 1, MAX_ZOOM);
    if (Math.abs(safeZoom - cropState.zoom) < 0.0001) return;

    const previousScale = Math.max(0.0001, cropState.scale || getScale(cropState));
    const imageX = (focusX - cropState.offsetX) / previousScale;
    const imageY = (focusY - cropState.offsetY) / previousScale;

    cropState.zoom = safeZoom;
    const nextScale = getScale(cropState);
    cropState.offsetX = focusX - imageX * nextScale;
    cropState.offsetY = focusY - imageY * nextScale;
    renderCrop();
  };

  const closeModal = ({ clearInput = false } = {}) => {
    if (cropUi?.modal instanceof HTMLElement) {
      cropUi.modal.hidden = true;
      cropUi.modal.classList.remove("is-open");
    }
    document.body.classList.remove("the-list-card-image-crop-open");
    teardownState({ clearInput });
  };

  const buildCroppedFile = async () => {
    if (!cropState) return null;
    clampOffsets(cropState);
    const cropWidthInSource = CROP_VIEW_WIDTH / Math.max(0.0001, cropState.scale);
    const cropHeightInSource = CROP_VIEW_HEIGHT / Math.max(0.0001, cropState.scale);
    const cropX = clamp(
      -cropState.offsetX / Math.max(0.0001, cropState.scale),
      0,
      Math.max(0, cropState.imageWidth - cropWidthInSource),
    );
    const cropY = clamp(
      -cropState.offsetY / Math.max(0.0001, cropState.scale),
      0,
      Math.max(0, cropState.imageHeight - cropHeightInSource),
    );

    const output = document.createElement("canvas");
    output.width = CROP_OUTPUT_WIDTH;
    output.height = CROP_OUTPUT_HEIGHT;
    const outputContext = output.getContext("2d");
    if (!outputContext) return null;
    outputContext.imageSmoothingEnabled = true;
    outputContext.imageSmoothingQuality = "high";
    outputContext.drawImage(
      cropState.image,
      cropX,
      cropY,
      cropWidthInSource,
      cropHeightInSource,
      0,
      0,
      CROP_OUTPUT_WIDTH,
      CROP_OUTPUT_HEIGHT,
    );

    const blob = await new Promise((resolve) => output.toBlob(resolve, "image/png", 0.95));
    if (!(blob instanceof Blob)) return null;

    const sourceName = String(cropState.sourceFile?.name || "source-image");
    const baseName = sourceName.replace(/\.[a-z0-9]+$/i, "") || "source-image";
    return new File([blob], `${baseName}-360x270.png`, {
      type: "image/png",
      lastModified: Date.now(),
    });
  };

  const applyCrop = async () => {
    if (!cropState || !cropUi || cropState.isApplying) return;
    cropState.isApplying = true;
    cropUi.applyButton.disabled = true;
    try {
      const croppedFile = await buildCroppedFile();
      if (!(croppedFile instanceof File)) {
        throw new Error("Could not create cropped image.");
      }
      const croppedDataUrl = await fileToDataUrl(croppedFile);
      setComponentValue(cropState.target.dataId, croppedDataUrl);
      applyPreviewFromDataUrl(cropState.target, croppedDataUrl);

      if (cropState.fileInput instanceof HTMLInputElement && typeof DataTransfer === "function") {
        const transfer = new DataTransfer();
        transfer.items.add(croppedFile);
        cropState.fileInput.files = transfer.files;
        bypassInput = cropState.fileInput;
        cropState.fileInput.dispatchEvent(new Event("change", { bubbles: true, composed: true }));
      }
      closeModal({ clearInput: false });
    } catch (error) {
      console.error("Source image crop failed", error);
      closeModal({ clearInput: true });
    } finally {
      cropUi.applyButton.disabled = false;
      if (cropState) cropState.isApplying = false;
    }
  };

  const ensureCropUi = () => {
    if (cropUi) return cropUi;

    const modal = document.createElement("div");
    modal.id = "sources-cover-image-crop-modal";
    modal.className = "the-list-card-image-crop-modal";
    modal.hidden = true;
    modal.innerHTML = `
      <button type="button" class="the-list-card-image-crop-modal__backdrop" data-action="cancel" aria-label="Crop popup background"></button>
      <section class="the-list-card-image-crop-modal__dialog" role="dialog" aria-modal="true" aria-labelledby="sources-cover-image-crop-title">
        <header class="the-list-card-image-crop-modal__header">
          <h3 id="sources-cover-image-crop-title">Crop source image</h3>
          <button type="button" class="the-list-card-image-crop-modal__close" data-action="cancel" aria-label="Close crop popup">x</button>
        </header>
        <div class="the-list-card-image-crop-modal__body">
          <div class="the-list-card-image-crop-modal__viewport">
            <canvas
              class="the-list-card-image-crop-modal__canvas"
              width="${CROP_VIEW_WIDTH}"
              height="${CROP_VIEW_HEIGHT}"
              aria-label="4:3 crop preview"
            ></canvas>
          </div>
          <p class="the-list-card-image-crop-modal__hint">Drag to move. Scroll to zoom.</p>
          <div class="the-list-card-image-crop-modal__controls">
            <label for="sources-cover-image-crop-zoom">Zoom</label>
            <input
              id="sources-cover-image-crop-zoom"
              class="the-list-card-image-crop-modal__zoom"
              type="range"
              min="0"
              max="${ZOOM_STEPS}"
              step="1"
              value="0"
            />
            <span class="the-list-card-image-crop-modal__zoom-value">100%</span>
            <span class="the-list-card-image-crop-modal__output">Output: ${CROP_OUTPUT_WIDTH} x ${CROP_OUTPUT_HEIGHT}</span>
          </div>
        </div>
        <div class="the-list-card-image-crop-modal__actions">
          <button type="button" class="the-list-card-image-crop-modal__btn the-list-card-image-crop-modal__btn--secondary" data-action="cancel">Cancel</button>
          <button type="button" class="the-list-card-image-crop-modal__btn the-list-card-image-crop-modal__btn--primary" data-action="apply">Apply crop</button>
        </div>
      </section>
    `;
    document.body.appendChild(modal);

    const canvas = modal.querySelector(".the-list-card-image-crop-modal__canvas");
    const zoomRange = modal.querySelector(".the-list-card-image-crop-modal__zoom");
    const zoomValue = modal.querySelector(".the-list-card-image-crop-modal__zoom-value");
    const applyButton = modal.querySelector("[data-action='apply']");
    if (
      !(canvas instanceof HTMLCanvasElement) ||
      !(zoomRange instanceof HTMLInputElement) ||
      !(zoomValue instanceof HTMLElement) ||
      !(applyButton instanceof HTMLButtonElement)
    ) {
      modal.remove();
      return null;
    }

    modal.querySelectorAll("[data-action='cancel']").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        closeModal({ clearInput: true });
      });
    });
    applyButton.addEventListener("click", (event) => {
      event.preventDefault();
      void applyCrop();
    });

    canvas.addEventListener("pointerdown", (event) => {
      if (!cropState) return;
      event.preventDefault();
      const point = getCanvasPoint(event.clientX, event.clientY);
      cropState.dragState = {
        pointerId: event.pointerId,
        startClientX: event.clientX,
        startClientY: event.clientY,
        startOffsetX: cropState.offsetX,
        startOffsetY: cropState.offsetY,
        scaleX: point.scaleX,
        scaleY: point.scaleY,
      };
      canvas.classList.add("is-dragging");
      if (typeof canvas.setPointerCapture === "function") {
        try {
          canvas.setPointerCapture(event.pointerId);
        } catch (error) {
          void error;
        }
      }
    });

    window.addEventListener("pointermove", (event) => {
      if (!cropState?.dragState || event.pointerId !== cropState.dragState.pointerId) return;
      event.preventDefault();
      const dx = (event.clientX - cropState.dragState.startClientX) * cropState.dragState.scaleX;
      const dy = (event.clientY - cropState.dragState.startClientY) * cropState.dragState.scaleY;
      cropState.offsetX = cropState.dragState.startOffsetX + dx;
      cropState.offsetY = cropState.dragState.startOffsetY + dy;
      renderCrop();
    });

    const endDrag = (event) => {
      if (!cropState?.dragState || event.pointerId !== cropState.dragState.pointerId) return;
      cropState.dragState = null;
      canvas.classList.remove("is-dragging");
    };
    window.addEventListener("pointerup", endDrag);
    window.addEventListener("pointercancel", endDrag);

    canvas.addEventListener(
      "wheel",
      (event) => {
        if (!cropState) return;
        event.preventDefault();
        const point = getCanvasPoint(event.clientX, event.clientY);
        const factor = Math.exp(-event.deltaY * WHEEL_SENSITIVITY);
        setZoom(cropState.zoom * factor, point.x, point.y);
      },
      { passive: false },
    );

    zoomRange.addEventListener("input", (event) => {
      const sliderValue = event.target instanceof HTMLInputElement ? event.target.value : "0";
      setZoom(sliderToZoom(sliderValue));
    });

    cropUi = { modal, canvas, zoomRange, zoomValue, applyButton };
    return cropUi;
  };

  const openCropModal = async (target, fileInput, file) => {
    if (!(fileInput instanceof HTMLInputElement) || !(file instanceof File)) return false;
    if (!String(file.type || "").toLowerCase().startsWith("image/")) return false;

    const ui = ensureCropUi();
    if (!ui) return false;

    const objectUrl = URL.createObjectURL(file);
    let image;
    try {
      image = await loadImageForCrop(objectUrl);
    } catch (error) {
      URL.revokeObjectURL(objectUrl);
      return false;
    }

    teardownState({ clearInput: false });
    cropState = {
      target,
      fileInput,
      sourceFile: file,
      objectUrl,
      image,
      imageWidth: Number(image.naturalWidth || image.width || 0),
      imageHeight: Number(image.naturalHeight || image.height || 0),
      zoom: 1,
      offsetX: 0,
      offsetY: 0,
      scale: 1,
      drawWidth: 0,
      drawHeight: 0,
      dragState: null,
      isApplying: false,
    };
    clampOffsets(cropState);
    cropState.offsetX = (CROP_VIEW_WIDTH - cropState.drawWidth) / 2;
    cropState.offsetY = (CROP_VIEW_HEIGHT - cropState.drawHeight) / 2;
    renderCrop();

    ui.modal.hidden = false;
    ui.modal.classList.add("is-open");
    document.body.classList.add("the-list-card-image-crop-open");
    ui.zoomRange.focus();
    return true;
  };

  const handleFileChange = (event) => {
    const inputNode = extractFileInputFromEvent(event);
    if (!(inputNode instanceof HTMLInputElement)) return;
    const pendingStillValid =
      pendingTarget &&
      Date.now() - Number(pendingTargetAt || 0) < 12_000;
    const target = targetForInput(inputNode) || (pendingStillValid ? pendingTarget : null);
    if (!target) return;
    pendingTarget = null;
    pendingTargetAt = 0;

    const file = inputNode.files?.[0] || null;
    if (!(file instanceof File)) return;

    if (bypassInput === inputNode) {
      bypassInput = null;
      applyPreviewFromDataUrl(target, readStoredDataUrl(target));
      return;
    }

    event.preventDefault();
    event.stopImmediatePropagation();
    void openCropModal(target, inputNode, file);
  };

  const bindPreviewOpeners = () => {
    TARGETS.forEach((target) => {
      const root = document.getElementById(target.previewId);
      if (!(root instanceof HTMLElement)) return;
      if (root.dataset.coverPickerBound === "1") return;
      root.dataset.coverPickerBound = "1";

      const open = () => {
        openUploadPicker(target);
      };
      root.addEventListener("click", (event) => {
        event.preventDefault();
        open();
      });
      root.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        open();
      });
    });
  };

  const syncEmptyPreviews = () => {
    TARGETS.forEach((target) => {
      const storedDataUrl = readStoredDataUrl(target);
      if (storedDataUrl) {
        applyPreviewFromDataUrl(target, storedDataUrl);
        return;
      }

      const root = document.getElementById(target.previewId);
      if (!(root instanceof HTMLElement)) return;
      const hasImage = Boolean(root.querySelector(".sources-cover-image-preview-img"));
      const hasEmptyState = Boolean(root.querySelector(".sources-cover-image-preview-empty"));
      if (!hasImage && !hasEmptyState) setPreviewMarkup(target.previewId, PREVIEW_EMPTY_HTML);
    });
  };

  const init = () => {
    document.addEventListener("change", handleFileChange, true);
    let syncScheduled = false;
    const scheduleSync = () => {
      if (syncScheduled) return;
      syncScheduled = true;
      requestAnimationFrame(() => {
        syncScheduled = false;
        syncEmptyPreviews();
        bindPreviewOpeners();
      });
    };
    const observer = new MutationObserver(() => {
      scheduleSync();
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
    syncEmptyPreviews();
    bindPreviewOpeners();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
