(() => {
  const TOAST_ROOT_ID = "the-list-toast-root";
  const TOAST_HIDE_DELAY_MS = 2300;
  const TOAST_REMOVE_DELAY_MS = 2620;
  const UPLOAD_STATUS_ID = "unsorted-upload-status";

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

  const refresh = () => {
    bindUploadToast();
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
