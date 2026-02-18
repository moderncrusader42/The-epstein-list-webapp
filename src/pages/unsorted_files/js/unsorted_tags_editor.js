(function () {
  if (typeof window === "undefined") return;

  const INPUT_CONTAINER_ID = "unsorted-tags-input";
  const EDITOR_ID = "unsorted-tags-editor";
  const CHIPS_CLASS = "person-detail-card__tags";
  const ADD_BUTTON_CLASS = "person-detail-card__tags-add-btn";
  const TAG_SUGGESTION_LIMIT = 8;
  const ADD_EDITOR_CLASS = "person-detail-card__tag-add-editor";
  const ADD_INPUT_CLASS = "person-detail-card__tag-add-input";
  const ADD_CANCEL_CLASS = "person-detail-card__tag-add-cancel-btn";
  const SUGGESTIONS_CLASS = "person-detail-card__tag-suggestions";
  const SUGGESTION_BUTTON_CLASS = "person-detail-card__tag-suggestion-btn";
  const SUPPRESS_RENDER_DATASET_KEY = "unsortedTagsSuppressRender";
  const addEditorCloseHandlers = new WeakMap();

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

  const normalizeTag = (value) =>
    String(value || "")
      .replace(/[✓✔]/g, " ")
      .replace(/\u00a0/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();

  const parseTagValues = (rawValue) => {
    const values = [];
    const seen = new Set();
    String(rawValue || "")
      .split(/[\n,]+/)
      .map((part) => normalizeTag(part))
      .filter(Boolean)
      .forEach((tag) => {
        if (seen.has(tag)) return;
        seen.add(tag);
        values.push(tag);
      });
    return values;
  };

  const serializeTagValues = (values) => values.join(", ");

  const mergeUniqueTagValues = (...sources) => {
    const merged = [];
    const seen = new Set();
    sources.forEach((source) => {
      const values = Array.isArray(source) ? source : [source];
      values.forEach((value) => {
        const normalized = normalizeTag(value);
        if (!normalized || seen.has(normalized)) return;
        seen.add(normalized);
        merged.push(normalized);
      });
    });
    return merged;
  };

  const findTagInput = (root) => {
    const container = root.querySelector(`#${INPUT_CONTAINER_ID}`);
    if (container instanceof HTMLInputElement || container instanceof HTMLTextAreaElement) {
      return container;
    }
    if (!(container instanceof HTMLElement)) return null;
    const input = container.querySelector(
      "textarea, input[type='text'], input[type='hidden'], input:not([type])",
    );
    if (input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement) {
      return input;
    }
    return null;
  };

  const findEditor = (root) => {
    const editor = root.querySelector(`#${EDITOR_ID}`);
    return editor instanceof HTMLElement ? editor : null;
  };

  const setTagInputValue = (input, nextValue, { suppressRender = false } = {}) => {
    const normalized = String(nextValue || "");
    if (input.value === normalized) return;
    if (suppressRender) {
      input.dataset[SUPPRESS_RENDER_DATASET_KEY] = "2";
    }
    input.value = normalized;
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
  };

  const ensureChipHost = (editor) => {
    const existing = editor.querySelector(`.${CHIPS_CLASS}`);
    if (existing instanceof HTMLElement) return existing;
    const chips = document.createElement("div");
    chips.className = `${CHIPS_CLASS} person-detail-card__tags--editing`;
    editor.appendChild(chips);
    return chips;
  };

  const readTagCatalogFromEditor = (editor) => {
    const rawCatalog = String(editor.dataset.tagCatalog || "").trim();
    if (!rawCatalog) return [];
    try {
      const parsed = JSON.parse(rawCatalog);
      if (!Array.isArray(parsed)) return [];
      return mergeUniqueTagValues(parsed);
    } catch (error) {
      void error;
      return [];
    }
  };

  const writeTagCatalogToEditor = (editor, values) => {
    editor.dataset.tagCatalog = JSON.stringify(mergeUniqueTagValues(values));
  };

  const ensureTagCatalogIncludes = (editor, values) => {
    const merged = mergeUniqueTagValues(readTagCatalogFromEditor(editor), values);
    writeTagCatalogToEditor(editor, merged);
    return merged;
  };

  const buildEditableTagChip = (tag, input) => {
    const chip = document.createElement("span");
    chip.className = "person-tag person-tag--editable";
    chip.dataset.tagValue = tag;

    const label = document.createElement("span");
    label.className = "person-tag__label";
    label.textContent = tag;
    chip.appendChild(label);

    const removeButton = document.createElement("button");
    removeButton.type = "button";
    removeButton.className = "person-tag__remove-btn";
    removeButton.textContent = "×";
    removeButton.title = `Remove "${tag}"`;
    removeButton.setAttribute("aria-label", `Remove tag ${tag}`);
    removeButton.addEventListener("click", (event) => {
      event.preventDefault();
      const remaining = parseTagValues(input.value).filter((entry) => entry !== tag);
      setTagInputValue(input, serializeTagValues(remaining));
    });
    chip.appendChild(removeButton);

    return chip;
  };

  const addTagChipWithoutRender = (chipsHost, addButton, editor, input, rawTagValue) => {
    const normalized = normalizeTag(rawTagValue);
    if (!normalized) return false;

    const current = parseTagValues(input.value);
    if (current.includes(normalized)) return false;

    setTagInputValue(
      input,
      serializeTagValues([...current, normalized]),
      { suppressRender: true },
    );
    ensureTagCatalogIncludes(editor, [normalized]);

    chipsHost
      .querySelectorAll(".source-tag--muted, .person-tag--muted")
      .forEach((node) => node.remove());

    chipsHost.insertBefore(buildEditableTagChip(normalized, input), addButton);
    return true;
  };

  const buildTagAddEditor = (chipsHost, addButton, editor, input) => {
    const addEditor = document.createElement("span");
    addEditor.className = ADD_EDITOR_CLASS;

    const addInput = document.createElement("input");
    addInput.type = "text";
    addInput.className = ADD_INPUT_CLASS;
    addInput.placeholder = "new tag";
    addInput.autocomplete = "off";
    addInput.setAttribute("aria-label", "New tag");
    addEditor.appendChild(addInput);

    const cancelButton = document.createElement("button");
    cancelButton.type = "button";
    cancelButton.className = ADD_CANCEL_CLASS;
    cancelButton.textContent = "×";
    cancelButton.title = "Cancel tag add";
    cancelButton.setAttribute("aria-label", "Cancel tag add");
    addEditor.appendChild(cancelButton);

    const suggestions = document.createElement("div");
    suggestions.className = SUGGESTIONS_CLASS;
    suggestions.hidden = true;
    addEditor.appendChild(suggestions);

    let closed = false;
    const closeEditor = ({ focusButton = false } = {}) => {
      if (closed) return;
      closed = true;
      document.removeEventListener("pointerdown", closeOnOutsidePointerDown, true);
      addEditorCloseHandlers.delete(addEditor);
      addEditor.remove();
      if (focusButton) addButton.focus();
    };

    const renderSuggestions = () => {
      const query = normalizeTag(addInput.value || "");
      suggestions.replaceChildren();
      if (!query) {
        suggestions.hidden = true;
        return;
      }

      const activeTagSet = new Set(parseTagValues(input.value));
      const catalog = ensureTagCatalogIncludes(editor, Array.from(activeTagSet));
      const matches = catalog
        .filter((tag) => !activeTagSet.has(tag) && tag.includes(query))
        .slice(0, TAG_SUGGESTION_LIMIT);

      if (!matches.length) {
        suggestions.hidden = true;
        return;
      }

      matches.forEach((tag) => {
        const optionButton = document.createElement("button");
        optionButton.type = "button";
        optionButton.className = SUGGESTION_BUTTON_CLASS;
        optionButton.textContent = tag;
        optionButton.setAttribute("aria-label", `Use tag ${tag}`);
        optionButton.addEventListener("mousedown", (event) => {
          event.preventDefault();
        });
        optionButton.addEventListener("click", (event) => {
          event.preventDefault();
          addTagChipWithoutRender(chipsHost, addButton, editor, input, tag);
          addInput.value = "";
          renderSuggestions();
          addInput.focus();
        });
        suggestions.appendChild(optionButton);
      });

      suggestions.hidden = false;
    };

    const commitInputValue = () => {
      const normalized = normalizeTag(addInput.value || "");
      if (!normalized) return false;
      const didAdd = addTagChipWithoutRender(chipsHost, addButton, editor, input, normalized);
      addInput.value = "";
      renderSuggestions();
      addInput.focus();
      return didAdd;
    };

    const closeOnOutsidePointerDown = (event) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (addEditor.contains(target) || target === addButton) return;
      closeEditor();
    };

    addInput.addEventListener("input", () => {
      renderSuggestions();
    });
    addInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === ",") {
        event.preventDefault();
        commitInputValue();
        return;
      }
      if (event.key !== "Escape") return;
      event.preventDefault();
      closeEditor({ focusButton: true });
    });

    cancelButton.addEventListener("mousedown", (event) => {
      event.preventDefault();
    });
    cancelButton.addEventListener("click", (event) => {
      event.preventDefault();
      closeEditor({ focusButton: true });
    });

    document.addEventListener("pointerdown", closeOnOutsidePointerDown, true);
    addEditorCloseHandlers.set(addEditor, closeEditor);
    return addEditor;
  };

  const buildAddButton = (input, editor, chipsHost) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = ADD_BUTTON_CLASS;
    button.textContent = "+";
    button.title = "Add tag";
    button.setAttribute("aria-label", "Add tag");
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const existingEditor = chipsHost.querySelector(`.${ADD_EDITOR_CLASS}`);
      if (existingEditor instanceof HTMLElement) {
        const existingInput = existingEditor.querySelector("input");
        if (existingInput instanceof HTMLInputElement) existingInput.focus();
        return;
      }

      const addEditor = buildTagAddEditor(chipsHost, button, editor, input);
      chipsHost.insertBefore(addEditor, button);
      const addInput = addEditor.querySelector("input");
      if (addInput instanceof HTMLInputElement) addInput.focus();
    });
    return button;
  };

  const renderEditor = (editor, input) => {
    const currentValue = String(input.value || "");
    if (editor.dataset.renderedValue === currentValue && editor.dataset.renderedAtLeastOnce === "1") {
      return;
    }

    const tags = parseTagValues(currentValue);
    const chipsHost = ensureChipHost(editor);
    chipsHost.querySelectorAll(`.${ADD_EDITOR_CLASS}`).forEach((node) => {
      const closeHandler = addEditorCloseHandlers.get(node);
      if (typeof closeHandler === "function") {
        closeHandler();
      }
    });
    ensureTagCatalogIncludes(editor, tags);
    chipsHost.replaceChildren();

    if (!tags.length) {
      const muted = document.createElement("span");
      muted.className = "person-tag person-tag--muted";
      muted.textContent = "no-tags";
      chipsHost.appendChild(muted);
    }

    tags.forEach((tag) => {
      chipsHost.appendChild(buildEditableTagChip(tag, input));
    });
    chipsHost.appendChild(buildAddButton(input, editor, chipsHost));

    editor.dataset.renderedValue = currentValue;
    editor.dataset.renderedAtLeastOnce = "1";
  };

  const bindTagInput = (input, editor) => {
    if (input.dataset.unsortedTagsBound === "1") return;
    input.dataset.unsortedTagsBound = "1";
    const rerender = () => {
      const suppressRemaining = Number.parseInt(input.dataset[SUPPRESS_RENDER_DATASET_KEY] || "0", 10);
      if (suppressRemaining > 0) {
        input.dataset[SUPPRESS_RENDER_DATASET_KEY] = String(suppressRemaining - 1);
        return;
      }
      editor.dataset.renderedValue = "__dirty__";
      renderEditor(editor, input);
    };
    input.addEventListener("input", rerender);
    input.addEventListener("change", rerender);
  };

  const sync = () => {
    const root = ensureRoot();
    const input = findTagInput(root);
    const editor = findEditor(root);
    if (!input || !editor) return;
    bindTagInput(input, editor);
    renderEditor(editor, input);
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
