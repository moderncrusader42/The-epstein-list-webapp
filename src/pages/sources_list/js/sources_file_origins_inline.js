(function () {
  if (typeof window === "undefined") return;

  const TARGETS = [
    {
      editorId: "sources-create-file-origins-editor",
      stateId: "sources-create-file-origins-state",
      removedStateId: "sources-create-file-origin-removed-keys",
      filePickerId: "sources-create-files",
      unsortedStateId: "sources-create-unsorted-files",
      unsortedCatalogStateId: "sources-create-unsorted-catalog-state",
    },
    {
      editorId: "sources-edit-file-origins-editor",
      stateId: "sources-edit-file-origins-state",
    },
  ];
  const ROW_CLASS = "sources-file-origin-row";
  const NAME_CLASS = "sources-file-origin-row__name";
  const INPUT_CLASS = "sources-file-origin-row__input";
  const REMOVE_CLASS = "sources-file-origin-row__remove";
  const ADD_CLASS = "sources-file-origin-add-btn";
  const ADD_MENU_CLASS = "sources-file-origin-add-menu";
  const ADD_MENU_ITEM_CLASS = "sources-file-origin-add-menu__item";
  const UNSORTED_KEY_PREFIX = "unsorted::";
  const UNSORTED_PICKER_BACKDROP_CLASS = "sources-unsorted-picker-backdrop";
  const UNSORTED_PICKER_CLASS = "sources-unsorted-picker";
  const UNSORTED_PICKER_CLOSE_CLASS = "sources-unsorted-picker__close";
  const UNSORTED_PICKER_CANCEL_CLASS = "sources-unsorted-picker__cancel";
  const UNSORTED_PICKER_APPLY_CLASS = "sources-unsorted-picker__apply";

  let activeAddMenu = null;
  let activeAddMenuCleanup = null;
  let activeUnsortedPicker = null;
  let activeUnsortedPickerCleanup = null;

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

  const findFileInput = (root, pickerId) => {
    const picker = root.querySelector(`#${pickerId}`);
    if (!(picker instanceof HTMLElement)) return null;
    const input = picker.querySelector("input[type='file']");
    return input instanceof HTMLInputElement ? input : null;
  };

  const parseStringList = (rawValue) => {
    const value = String(rawValue || "").trim();
    if (!value) return [];
    if (value.startsWith("[") && value.endsWith("]")) {
      try {
        const parsed = JSON.parse(value);
        if (Array.isArray(parsed)) {
          return parsed.map((item) => String(item || "").trim()).filter(Boolean);
        }
      } catch (error) {
        void error;
      }
    }
    return value
      .split(/[,\s]+/g)
      .map((part) => String(part || "").trim())
      .filter(Boolean);
  };

  const setInputValue = (input, value) => {
    if (input.value === value) return;
    input.value = value;
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
  };

  const formatBytes = (rawValue) => {
    const value = Number(rawValue || 0);
    const safe = Number.isFinite(value) ? Math.max(0, value) : 0;
    const units = ["B", "KB", "MB", "GB", "TB"];
    let index = 0;
    let scaled = safe;
    while (scaled >= 1024 && index < units.length - 1) {
      scaled /= 1024;
      index += 1;
    }
    if (units[index] === "B") return `${Math.round(scaled)} B`;
    return `${scaled.toFixed(1)} ${units[index]}`;
  };

  const escapeHtml = (rawValue) =>
    String(rawValue || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const parseUnsortedIdFromOriginKey = (originKey) => {
    const value = String(originKey || "").trim();
    if (!value.startsWith(UNSORTED_KEY_PREFIX)) return 0;
    const parsed = Number.parseInt(value.slice(UNSORTED_KEY_PREFIX.length), 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  };

  const parseUnsortedCatalog = (rawValue) => {
    const value = String(rawValue || "").trim();
    if (!value || !value.startsWith("[") || !value.endsWith("]")) return [];
    try {
      const parsed = JSON.parse(value);
      if (!Array.isArray(parsed)) return [];
      return parsed
        .map((entry) => {
          const id = Number.parseInt(String(entry?.id || "").trim(), 10);
          if (!Number.isFinite(id) || id <= 0) return null;
          return {
            id,
            fileName: String(entry?.file_name || "").trim(),
            sizeBytes: Number(entry?.size_bytes || 0) || 0,
          };
        })
        .filter(Boolean);
    } catch (error) {
      void error;
      return [];
    }
  };

  const closeAddMenu = () => {
    if (typeof activeAddMenuCleanup === "function") {
      activeAddMenuCleanup();
    }
    activeAddMenuCleanup = null;
    if (activeAddMenu && activeAddMenu.parentNode) {
      activeAddMenu.parentNode.removeChild(activeAddMenu);
    }
    activeAddMenu = null;
  };

  const closeUnsortedPicker = () => {
    if (typeof activeUnsortedPickerCleanup === "function") {
      activeUnsortedPickerCleanup();
    }
    activeUnsortedPickerCleanup = null;
    if (activeUnsortedPicker && activeUnsortedPicker.parentNode) {
      activeUnsortedPicker.parentNode.removeChild(activeUnsortedPicker);
    }
    activeUnsortedPicker = null;
  };

  const collectRows = (editor) => {
    const rows = [];
    const rowNodes = editor.querySelectorAll(`.${ROW_CLASS}`);
    rowNodes.forEach((rowNode) => {
      if (!(rowNode instanceof HTMLElement)) return;
      const keyFromAttr = rowNode.dataset.originKey || "";
      const nameFromAttr = rowNode.dataset.fileName || "";
      const nameNode = rowNode.querySelector(`.${NAME_CLASS}`);
      const displayName = String(nameFromAttr || nameNode?.textContent || "").trim();
      const fileName = String(keyFromAttr || displayName).trim();
      const input = rowNode.querySelector(`.${INPUT_CLASS}`);
      const originValue =
        input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement
          ? String(input.value || "").trim()
          : "";
      rows.push([fileName, originValue, displayName]);
    });
    return rows;
  };

  const setStateValue = (stateInput, rows) => {
    const serialized = rows.length ? JSON.stringify(rows) : "";
    setInputValue(stateInput, serialized);
  };

  const appendRemovedKey = (removedStateInput, originKey) => {
    if (!removedStateInput || !originKey) return;
    const keys = parseStringList(removedStateInput.value);
    if (keys.includes(originKey)) return;
    keys.push(originKey);
    setInputValue(removedStateInput, JSON.stringify(keys));
  };

  const removeRemovedKeys = (removedStateInput, keysToRemove) => {
    if (!removedStateInput) return;
    const keySet = new Set((keysToRemove || []).map((key) => String(key || "").trim()).filter(Boolean));
    if (!keySet.size) return;
    const keys = parseStringList(removedStateInput.value);
    const next = keys.filter((key) => !keySet.has(String(key || "").trim()));
    const nextValue = next.length ? JSON.stringify(next) : "";
    setInputValue(removedStateInput, nextValue);
  };

  const updateUnsortedSelection = (unsortedStateInput, selectedIds) => {
    if (!unsortedStateInput) return;
    const normalized = (selectedIds || [])
      .map((candidate) => Number.parseInt(String(candidate || "").trim(), 10))
      .filter((candidate) => Number.isFinite(candidate) && candidate > 0);
    const deduped = [];
    const seen = new Set();
    normalized.forEach((id) => {
      if (seen.has(id)) return;
      seen.add(id);
      deduped.push(id);
    });
    setInputValue(unsortedStateInput, deduped.length ? JSON.stringify(deduped) : "");
  };

  const getSelectedUnsortedIds = (unsortedStateInput) =>
    parseStringList(unsortedStateInput?.value)
      .map((candidate) => Number.parseInt(String(candidate || "").trim(), 10))
      .filter((candidate, index, array) => Number.isFinite(candidate) && candidate > 0 && array.indexOf(candidate) === index);

  const bindInput = (input, stateInput, editor) => {
    if (input.dataset.sourcesOriginsBound === "1") return;
    input.dataset.sourcesOriginsBound = "1";
    const sync = () => setStateValue(stateInput, collectRows(editor));
    input.addEventListener("input", sync);
    input.addEventListener("change", sync);
  };

  const bindRemoveButton = (button, stateInput, removedStateInput, editor, root, target) => {
    if (button.dataset.sourcesOriginsRemoveBound === "1") return;
    button.dataset.sourcesOriginsRemoveBound = "1";
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const rowNode = button.closest(`.${ROW_CLASS}`);
      if (!(rowNode instanceof HTMLElement)) return;
      const originKey = String(rowNode.dataset.originKey || "").trim();
      const unsortedId = parseUnsortedIdFromOriginKey(originKey);
      const unsortedStateInput = target.unsortedStateId ? findStateInput(root, target.unsortedStateId) : null;
      if (unsortedId > 0 && unsortedStateInput) {
        const remainingIds = getSelectedUnsortedIds(unsortedStateInput).filter((id) => id !== unsortedId);
        updateUnsortedSelection(unsortedStateInput, remainingIds);
        removeRemovedKeys(removedStateInput, [originKey]);
      } else {
        appendRemovedKey(removedStateInput, originKey);
      }
      rowNode.remove();
      setStateValue(stateInput, collectRows(editor));
    });
  };

  const openUnsortedPicker = (root, target) => {
    closeUnsortedPicker();

    const unsortedStateInput = target.unsortedStateId ? findStateInput(root, target.unsortedStateId) : null;
    const catalogStateInput = target.unsortedCatalogStateId
      ? findStateInput(root, target.unsortedCatalogStateId)
      : null;
    if (!unsortedStateInput || !catalogStateInput) return;

    const rows = parseUnsortedCatalog(catalogStateInput.value);
    if (!rows.length) return;
    const selectedIds = new Set(getSelectedUnsortedIds(unsortedStateInput));

    const host = document.body || document.documentElement;
    if (!host) return;

    const backdrop = document.createElement("div");
    backdrop.className = UNSORTED_PICKER_BACKDROP_CLASS;
    const listMarkup = rows
      .map((row) => {
        const checked = selectedIds.has(row.id) ? " checked" : "";
        const safeName = escapeHtml(row.fileName || `file-${row.id}`);
        const meta = `${formatBytes(row.sizeBytes)}`;
        return (
          `<label class="sources-unsorted-picker__row">` +
          `<input type="checkbox" value="${row.id}"${checked} />` +
          `<span class="sources-unsorted-picker__name">[#${row.id}] ${safeName}</span>` +
          `<span class="sources-unsorted-picker__meta">${escapeHtml(meta)}</span>` +
          `</label>`
        );
      })
      .join("");
    backdrop.innerHTML =
      `<div class="${UNSORTED_PICKER_CLASS}" role="dialog" aria-modal="true" aria-label="Add unsorted files">` +
      `<div class="sources-unsorted-picker__head">` +
      `<strong>Add from unsorted files</strong>` +
      `<button type="button" class="${UNSORTED_PICKER_CLOSE_CLASS}" aria-label="Close">X</button>` +
      `</div>` +
      `<div class="sources-unsorted-picker__list">${listMarkup}</div>` +
      `<div class="sources-unsorted-picker__actions">` +
      `<button type="button" class="${UNSORTED_PICKER_CANCEL_CLASS}">Cancel</button>` +
      `<button type="button" class="${UNSORTED_PICKER_APPLY_CLASS}">Add selected</button>` +
      `</div>` +
      `</div>`;

    const close = () => {
      closeUnsortedPicker();
    };

    const onBackdropClick = (event) => {
      if (event.target === backdrop) {
        close();
      }
    };
    const onEsc = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        close();
      }
    };

    backdrop.addEventListener("click", onBackdropClick);
    document.addEventListener("keydown", onEsc, true);

    const closeBtn = backdrop.querySelector(`.${UNSORTED_PICKER_CLOSE_CLASS}`);
    const cancelBtn = backdrop.querySelector(`.${UNSORTED_PICKER_CANCEL_CLASS}`);
    const applyBtn = backdrop.querySelector(`.${UNSORTED_PICKER_APPLY_CLASS}`);
    if (closeBtn instanceof HTMLButtonElement) {
      closeBtn.addEventListener("click", close);
    }
    if (cancelBtn instanceof HTMLButtonElement) {
      cancelBtn.addEventListener("click", close);
    }
    if (applyBtn instanceof HTMLButtonElement) {
      applyBtn.addEventListener("click", () => {
        const checkboxes = backdrop.querySelectorAll(".sources-unsorted-picker__row input[type='checkbox']");
        const nextIds = [];
        checkboxes.forEach((node) => {
          if (!(node instanceof HTMLInputElement) || !node.checked) return;
          const parsed = Number.parseInt(String(node.value || "").trim(), 10);
          if (!Number.isFinite(parsed) || parsed <= 0) return;
          nextIds.push(parsed);
        });
        updateUnsortedSelection(unsortedStateInput, nextIds);
        const removedStateInput = target.removedStateId ? findStateInput(root, target.removedStateId) : null;
        removeRemovedKeys(
          removedStateInput,
          nextIds.map((id) => `${UNSORTED_KEY_PREFIX}${id}`),
        );
        close();
      });
    }

    host.appendChild(backdrop);
    activeUnsortedPicker = backdrop;
    activeUnsortedPickerCleanup = () => {
      backdrop.removeEventListener("click", onBackdropClick);
      document.removeEventListener("keydown", onEsc, true);
    };
  };

  const openAddMenu = (button, root, target) => {
    closeAddMenu();
    if (!(button instanceof HTMLElement)) return;

    const catalogStateInput = target.unsortedCatalogStateId
      ? findStateInput(root, target.unsortedCatalogStateId)
      : null;
    const hasUnsortedRows = parseUnsortedCatalog(catalogStateInput?.value).length > 0;

    const host = document.body || document.documentElement;
    if (!host) return;

    const menu = document.createElement("div");
    menu.className = ADD_MENU_CLASS;
    const unsortedDisabled = hasUnsortedRows ? "" : " disabled";
    const unsortedLabel = hasUnsortedRows ? "Add from unsorted" : "Add from unsorted (none)";
    menu.innerHTML =
      `<button type="button" class="${ADD_MENU_ITEM_CLASS}" data-action="upload">Upload file</button>` +
      `<button type="button" class="${ADD_MENU_ITEM_CLASS}" data-action="unsorted"${unsortedDisabled}>${unsortedLabel}</button>`;
    menu.style.position = "fixed";
    menu.style.top = "0px";
    menu.style.left = "0px";
    menu.style.right = "auto";
    menu.style.zIndex = "5000";
    menu.style.visibility = "hidden";
    host.appendChild(menu);

    const placeMenu = () => {
      if (!menu.isConnected) return;
      const rect = button.getBoundingClientRect();
      const menuRect = menu.getBoundingClientRect();
      const gap = 6;
      const viewportPadding = 8;

      const preferredLeft = rect.right - menuRect.width;
      const clampedLeft = Math.max(
        viewportPadding,
        Math.min(preferredLeft, window.innerWidth - menuRect.width - viewportPadding),
      );

      const spaceBelow = window.innerHeight - rect.bottom - gap - viewportPadding;
      const shouldOpenAbove = menuRect.height > spaceBelow && rect.top > menuRect.height + gap + viewportPadding;
      const top = shouldOpenAbove ? rect.top - menuRect.height - gap : rect.bottom + gap;
      const clampedTop = Math.max(
        viewportPadding,
        Math.min(top, window.innerHeight - menuRect.height - viewportPadding),
      );

      menu.style.left = `${Math.round(clampedLeft)}px`;
      menu.style.top = `${Math.round(clampedTop)}px`;
      menu.style.visibility = "visible";
    };
    placeMenu();
    activeAddMenu = menu;

    const onGlobalClick = (event) => {
      const targetNode = event.target;
      if (targetNode instanceof Node && (menu.contains(targetNode) || button.contains(targetNode))) {
        return;
      }
      closeAddMenu();
    };
    const onEsc = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        closeAddMenu();
      }
    };
    const onViewportChange = () => {
      if (activeAddMenu !== menu) return;
      placeMenu();
    };

    const onMenuClick = (event) => {
      const actionNode = event.target;
      if (!(actionNode instanceof HTMLElement)) return;
      const action = String(actionNode.dataset.action || "").trim();
      if (!action) return;
      closeAddMenu();
      if (action === "upload") {
        const pickerId = String(target.filePickerId || "").trim();
        if (!pickerId) return;
        const input = findFileInput(root, pickerId);
        if (!input || input.disabled) return;
        input.click();
        return;
      }
      if (action === "unsorted") {
        openUnsortedPicker(root, target);
      }
    };
    menu.addEventListener("click", onMenuClick);

    const bindGlobals = () => {
      document.addEventListener("pointerdown", onGlobalClick, true);
      document.addEventListener("keydown", onEsc, true);
      window.addEventListener("resize", onViewportChange, true);
      window.addEventListener("scroll", onViewportChange, true);
    };
    requestAnimationFrame(bindGlobals);

    activeAddMenuCleanup = () => {
      menu.removeEventListener("click", onMenuClick);
      document.removeEventListener("pointerdown", onGlobalClick, true);
      document.removeEventListener("keydown", onEsc, true);
      window.removeEventListener("resize", onViewportChange, true);
      window.removeEventListener("scroll", onViewportChange, true);
    };
  };

  const bindAddButton = (button, root, target) => {
    if (button.dataset.sourcesOriginsAddBound === "1") return;
    button.dataset.sourcesOriginsAddBound = "1";
    button.addEventListener("click", (event) => {
      event.preventDefault();
      if (activeAddMenu) {
        closeAddMenu();
        return;
      }
      openAddMenu(button, root, target);
    });
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
    const removedStateInput = target.removedStateId ? findStateInput(root, target.removedStateId) : null;
    const inputNodes = editor.querySelectorAll(`.${INPUT_CLASS}`);
    inputNodes.forEach((node) => {
      if (!(node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement)) return;
      bindInput(node, stateInput, editor);
    });
    const removeButtons = editor.querySelectorAll(`.${REMOVE_CLASS}`);
    removeButtons.forEach((node) => {
      if (!(node instanceof HTMLButtonElement)) return;
      bindRemoveButton(node, stateInput, removedStateInput, editor, root, target);
    });
    const addButtons = editor.querySelectorAll(`.${ADD_CLASS}`);
    addButtons.forEach((node) => {
      if (!(node instanceof HTMLButtonElement)) return;
      bindAddButton(node, root, target);
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
