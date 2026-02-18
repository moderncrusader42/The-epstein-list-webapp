(() => {
  const TOAST_ROOT_ID = "the-list-toast-root";
  const TOAST_HIDE_DELAY_MS = 2300;
  const TOAST_REMOVE_DELAY_MS = 2620;
  const UPLOAD_STATUS_ID = "unsorted-upload-status";
  const FULLSCREEN_BUTTON_SELECTOR = ".unsorted-preview-fullscreen";
  const FULLSCREEN_WRAP_SELECTOR = ".unsorted-preview-wrap";
  const FULLSCREEN_CARD_SELECTOR = ".unsorted-preview-card";
  const FULLSCREEN_MEDIA_SELECTOR = ".source-preview";
  const FULLSCREEN_ACTIVE_CLASS = "unsorted-preview-wrap--active-fullscreen";
  let fullscreenBound = false;
  let fullscreenRequestedWrap = null;
  let manualFullscreenWrap = null;
  let inlineFullscreenTarget = null;

  const ensureToastRoot = () => {
    let root = document.getElementById(TOAST_ROOT_ID);
    if (root) return root;
    root = document.createElement("div");
    root.id = TOAST_ROOT_ID;
    document.body.appendChild(root);
    return root;
  };

  const showSuccessToast = (message) => {
    const root = ensureToastRoot();
    const toast = document.createElement("div");
    toast.className = "the-list-toast the-list-toast--success";
    toast.textContent = String(message || "").replace(/^\s*✅\s*/, "").trim();
    root.appendChild(toast);
    window.setTimeout(() => {
      toast.classList.add("is-hiding");
    }, TOAST_HIDE_DELAY_MS);
    window.setTimeout(() => {
      toast.remove();
    }, TOAST_REMOVE_DELAY_MS);
  };

  const getStatusMessage = (statusNode) => {
    if (!statusNode) return "";
    return String(statusNode.textContent || "")
      .replace(/\s+/g, " ")
      .trim();
  };

  const clearStatusMessage = (statusNode) => {
    if (!statusNode) return;
    statusNode.textContent = "";
  };

  const bindUploadToast = () => {
    const statusNode = document.getElementById(UPLOAD_STATUS_ID);
    if (!statusNode || statusNode.dataset.toastBound === "1") return;
    statusNode.dataset.toastBound = "1";

    let lastMessage = "";
    const sync = () => {
      const message = getStatusMessage(statusNode);
      if (!message) {
        lastMessage = "";
        return;
      }
      if (message === lastMessage) return;
      lastMessage = message;
      if (!message.startsWith("✅")) return;
      showSuccessToast(message);
      clearStatusMessage(statusNode);
    };

    const observer = new MutationObserver(sync);
    observer.observe(statusNode, {
      childList: true,
      subtree: true,
      characterData: true,
    });
    sync();
  };

  const getFullscreenElement = () =>
    document.fullscreenElement ||
    document.webkitFullscreenElement ||
    document.mozFullScreenElement ||
    document.msFullscreenElement ||
    null;

  const ensurePromise = (value) =>
    value && typeof value.then === "function" ? value : Promise.resolve();

  const setStyleImportant = (node, property, value) => {
    if (!node) return;
    node.style.setProperty(property, value, "important");
  };

  const clearStyle = (node, property) => {
    if (!node) return;
    node.style.removeProperty(property);
  };

  const requestFullscreen = (element) => {
    if (!element) return Promise.reject(new Error("Missing fullscreen target."));
    if (typeof element.requestFullscreen === "function") {
      return ensurePromise(element.requestFullscreen());
    }
    if (typeof element.webkitRequestFullscreen === "function") {
      element.webkitRequestFullscreen();
      return Promise.resolve();
    }
    if (typeof element.mozRequestFullScreen === "function") {
      element.mozRequestFullScreen();
      return Promise.resolve();
    }
    if (typeof element.msRequestFullscreen === "function") {
      element.msRequestFullscreen();
      return Promise.resolve();
    }
    return Promise.reject(new Error("Fullscreen API not supported."));
  };

  const exitFullscreen = () => {
    if (typeof document.exitFullscreen === "function") {
      return ensurePromise(document.exitFullscreen());
    }
    if (typeof document.webkitExitFullscreen === "function") {
      document.webkitExitFullscreen();
      return Promise.resolve();
    }
    if (typeof document.mozCancelFullScreen === "function") {
      document.mozCancelFullScreen();
      return Promise.resolve();
    }
    if (typeof document.msExitFullscreen === "function") {
      document.msExitFullscreen();
      return Promise.resolve();
    }
    return Promise.resolve();
  };

  const isWrapFullscreenActive = (wrap, active) => {
    if (!wrap) return false;
    if (manualFullscreenWrap && manualFullscreenWrap === wrap) return true;
    if (active && (active === wrap || wrap.contains(active))) return true;
    if (!active && fullscreenRequestedWrap && fullscreenRequestedWrap === wrap) return true;
    return false;
  };

  const clearInlineFullscreenStyles = () => {
    const target = inlineFullscreenTarget;
    inlineFullscreenTarget = null;

    const wrap = target?.wrap || null;
    const card = target?.card || null;
    const media = target?.media || null;

    [
      "position",
      "inset",
      "width",
      "height",
      "min-height",
      "max-height",
      "max-width",
      "margin",
      "display",
      "align-items",
      "justify-content",
      "z-index",
      "background",
      "padding-top",
    ].forEach((prop) => clearStyle(wrap, prop));

    ["width", "height", "min-height", "max-height", "max-width", "border", "border-radius", "background"]
      .forEach((prop) => clearStyle(card, prop));

    ["width", "height", "max-height", "margin", "background", "object-fit"].forEach((prop) => clearStyle(media, prop));

    document.documentElement.classList.remove("unsorted-fullscreen-open");
    document.body.classList.remove("unsorted-fullscreen-open");
  };

  const applyInlineFullscreenStyles = (wrap) => {
    if (!wrap) {
      clearInlineFullscreenStyles();
      return;
    }
    if (inlineFullscreenTarget && inlineFullscreenTarget.wrap === wrap) {
      return;
    }

    clearInlineFullscreenStyles();
    const card = wrap.querySelector(FULLSCREEN_CARD_SELECTOR);
    const media = wrap.querySelector(FULLSCREEN_MEDIA_SELECTOR);
    inlineFullscreenTarget = { wrap, card, media };

    setStyleImportant(wrap, "position", "fixed");
    setStyleImportant(wrap, "inset", "0");
    setStyleImportant(wrap, "width", "100vw");
    setStyleImportant(wrap, "height", "100vh");
    setStyleImportant(wrap, "min-height", "0");
    setStyleImportant(wrap, "max-height", "none");
    setStyleImportant(wrap, "max-width", "none");
    setStyleImportant(wrap, "margin", "0");
    setStyleImportant(wrap, "display", "flex");
    setStyleImportant(wrap, "align-items", "stretch");
    setStyleImportant(wrap, "justify-content", "stretch");
    setStyleImportant(wrap, "z-index", "2147483000");
    setStyleImportant(wrap, "background", "#020617");
    setStyleImportant(wrap, "padding-top", "0");

    setStyleImportant(card, "width", "100%");
    setStyleImportant(card, "height", "100%");
    setStyleImportant(card, "min-height", "0");
    setStyleImportant(card, "max-height", "none");
    setStyleImportant(card, "max-width", "none");
    setStyleImportant(card, "border", "0");
    setStyleImportant(card, "border-radius", "0");
    setStyleImportant(card, "background", "#020617");

    setStyleImportant(media, "width", "100%");
    setStyleImportant(media, "height", "100%");
    setStyleImportant(media, "max-height", "none");
    setStyleImportant(media, "margin", "0");
    setStyleImportant(media, "background", "#020617");
    setStyleImportant(media, "object-fit", "contain");

    document.documentElement.classList.add("unsorted-fullscreen-open");
    document.body.classList.add("unsorted-fullscreen-open");
  };

  const syncFullscreenButtons = () => {
    const active = getFullscreenElement();
    if (manualFullscreenWrap && !manualFullscreenWrap.isConnected) {
      manualFullscreenWrap = null;
    }
    if (active) {
      manualFullscreenWrap = null;
      fullscreenRequestedWrap = null;
    }
    if (!active) {
      if (!manualFullscreenWrap) {
        fullscreenRequestedWrap = null;
      }
    }
    let activeWrap = null;
    document.querySelectorAll(FULLSCREEN_WRAP_SELECTOR).forEach((wrap) => {
      const isActive = isWrapFullscreenActive(wrap, active);
      wrap.classList.toggle(FULLSCREEN_ACTIVE_CLASS, isActive);
      if (isActive && !activeWrap) {
        activeWrap = wrap;
      }
    });
    applyInlineFullscreenStyles(activeWrap);
    document.querySelectorAll(FULLSCREEN_BUTTON_SELECTOR).forEach((button) => {
      const wrap = button.closest(FULLSCREEN_WRAP_SELECTOR);
      const isActive = isWrapFullscreenActive(wrap, active);
      const nextLabel = isActive ? "Exit full screen" : "Full screen";
      const nextPressed = isActive ? "true" : "false";
      if (button.textContent !== nextLabel) {
        button.textContent = nextLabel;
      }
      if (button.getAttribute("aria-pressed") !== nextPressed) {
        button.setAttribute("aria-pressed", nextPressed);
      }
    });
  };

  const handleFullscreenClick = (event) => {
    const button = event.target.closest(FULLSCREEN_BUTTON_SELECTOR);
    if (!button) return;

    const wrap = button.closest(FULLSCREEN_WRAP_SELECTOR);
    if (!wrap) return;

    event.preventDefault();
    const active = getFullscreenElement();
    const isCurrent = isWrapFullscreenActive(wrap, active);
    if (isCurrent) {
      const wasManual = Boolean(manualFullscreenWrap && manualFullscreenWrap === wrap);
      manualFullscreenWrap = null;
      fullscreenRequestedWrap = null;
      clearInlineFullscreenStyles();
      if (wasManual) {
        syncFullscreenButtons();
        return;
      }
      exitFullscreen().finally(syncFullscreenButtons);
      return;
    }

    manualFullscreenWrap = null;
    fullscreenRequestedWrap = wrap;
    applyInlineFullscreenStyles(wrap);
    requestFullscreen(wrap)
      .then(() => {
        fullscreenRequestedWrap = null;
      })
      .catch(() => {
        manualFullscreenWrap = wrap;
        fullscreenRequestedWrap = null;
      })
      .finally(syncFullscreenButtons);
  };

  const handleFullscreenKeydown = (event) => {
    if (event.key !== "Escape") return;
    if (!manualFullscreenWrap) return;
    manualFullscreenWrap = null;
    fullscreenRequestedWrap = null;
    clearInlineFullscreenStyles();
    syncFullscreenButtons();
  };

  const bindFullscreenControls = () => {
    if (fullscreenBound) return;
    fullscreenBound = true;
    document.addEventListener("click", handleFullscreenClick);
    document.addEventListener("keydown", handleFullscreenKeydown);
    document.addEventListener("fullscreenchange", syncFullscreenButtons);
    document.addEventListener("webkitfullscreenchange", syncFullscreenButtons);
    document.addEventListener("mozfullscreenchange", syncFullscreenButtons);
    document.addEventListener("MSFullscreenChange", syncFullscreenButtons);
    syncFullscreenButtons();
  };

  const refresh = () => {
    bindUploadToast();
    bindFullscreenControls();
  };

  const start = () => {
    const observer = new MutationObserver(refresh);
    observer.observe(document.body, { childList: true, subtree: true });
    refresh();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
