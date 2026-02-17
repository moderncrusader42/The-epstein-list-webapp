(function () {
  if (typeof window === "undefined") return;

  const TARGETS = [
    {
      editorId: "sources-create-file-origins-editor",
      stateId: "sources-create-file-origins-state",
    },
    {
      editorId: "sources-edit-file-origins-editor",
      stateId: "sources-edit-file-origins-state",
    },
  ];
  const ROW_CLASS = "sources-file-origin-row";
  const NAME_CLASS = "sources-file-origin-row__name";
  const INPUT_CLASS = "sources-file-origin-row__input";

  const ensureRoot = () => {
    if (typeof window.gradioApp === "function") {
      try {
        const app = window.gradioApp();
        if (app) return app;
      } catch (error) {
        void error;
      }
    }
    const host = document.querySelector("gradio-app");
    return host ? host.shadowRoot || host : document;
  };

  const findStateInput = (root, stateId) => {
    const stateContainer = root.querySelector(`#${stateId}`);
    if (!(stateContainer instanceof HTMLElement)) return null;
    const input = stateContainer.querySelector("textarea, input[type='text']");
    if (input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement) {
      return input;
    }
    return null;
  };

  const findEditor = (root, editorId) => {
    const editor = root.querySelector(`#${editorId}`);
    return editor instanceof HTMLElement ? editor : null;
  };

  const collectRows = (editor) => {
    const rows = [];
    const rowNodes = editor.querySelectorAll(`.${ROW_CLASS}`);
    rowNodes.forEach((rowNode) => {
      if (!(rowNode instanceof HTMLElement)) return;
      const nameFromAttr = rowNode.dataset.fileName || "";
      const nameNode = rowNode.querySelector(`.${NAME_CLASS}`);
      const fileName = String(nameFromAttr || nameNode?.textContent || "").trim();
      const input = rowNode.querySelector(`.${INPUT_CLASS}`);
      const originValue =
        input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement
          ? String(input.value || "").trim()
          : "";
      rows.push([fileName, originValue]);
    });
    return rows;
  };

  const setStateValue = (stateInput, rows) => {
    const serialized = rows.length ? JSON.stringify(rows) : "";
    if (stateInput.value === serialized) return;
    stateInput.value = serialized;
    stateInput.dispatchEvent(new Event("input", { bubbles: true }));
    stateInput.dispatchEvent(new Event("change", { bubbles: true }));
  };

  const bindInput = (input, stateInput, editor) => {
    if (input.dataset.sourcesOriginsBound === "1") return;
    input.dataset.sourcesOriginsBound = "1";
    const sync = () => setStateValue(stateInput, collectRows(editor));
    input.addEventListener("input", sync);
    input.addEventListener("change", sync);
  };

  const syncTarget = (root, target) => {
    const editor = findEditor(root, target.editorId);
    const stateInput = findStateInput(root, target.stateId);
    if (!stateInput) return;
    if (!editor) {
      setStateValue(stateInput, []);
      return;
    }

    const rows = collectRows(editor);
    const inputNodes = editor.querySelectorAll(`.${INPUT_CLASS}`);
    inputNodes.forEach((node) => {
      if (!(node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement)) return;
      bindInput(node, stateInput, editor);
    });
    setStateValue(stateInput, rows);
  };

  const sync = () => {
    const root = ensureRoot();
    TARGETS.forEach((target) => {
      syncTarget(root, target);
    });
  };

  const schedule = () => {
    let attempts = 0;
    const tick = () => {
      sync();
      attempts += 1;
      if (attempts < 120) {
        requestAnimationFrame(tick);
      }
    };
    tick();
    const observer = new MutationObserver(() => {
      sync();
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", schedule);
  } else {
    schedule();
  }
})();
