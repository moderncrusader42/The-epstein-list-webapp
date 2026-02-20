(function () {
  if (typeof window === "undefined") return;

  console.log("=== [MD_DEBUG] sources_individual_editor.js LOADED v2 ===");

  const EDIT_SHELL_ID = "sources-edit-shell";
  const NAME_INPUT_ID = "sources-edit-name";
  const TAGS_INPUT_ID = "sources-edit-tags";
  const DELETE_FILES_EDITOR_ID = "sources-edit-existing-files-editor";
  const DELETE_FILES_STATE_ID = "sources-edit-delete-files-state";
  const DELETE_FILE_CARD_SELECTOR = ".sources-edit-delete-card[data-file-id]";
  const DELETE_FILE_BUTTON_SELECTOR = ".sources-edit-delete-card__remove-btn";
  const SUMMARY_INPUT_ID = "sources-edit-summary";
  const SUMMARY_PREVIEW_ID = "sources-edit-summary-preview";
  const SUMMARY_MODE_ID = "sources-edit-summary-view-mode";
  const SUMMARY_SAVE_BUTTON_ID = "sources-edit-save-btn";
  const SUMMARY_VISUAL_EDITOR_CLASS = "sources-visual-editor";
  const SUMMARY_VISUAL_EDITOR_ACTIVE_CLASS = "sources-visual-editor--active";
  const SUMMARY_COMPILE_DELAY_MS = 1000;
  const TITLE_SELECTOR = "#sources-title h2";
  const TAGS_SELECTOR = "#sources-browser-head-meta .source-browser-head__tags";
  const TAG_SUGGESTION_LIMIT = 8;

  let refreshScheduled = false;
  let inlineWasActive = false;
  let summarySyncScheduled = false;
  let summaryCompileTimerId = 0;
  let lastCompiledSummaryMarkdown = "";
  let summaryGlobalHandlersBound = false;
  const boundSummaryEditors = new WeakSet();

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

  const getComponentInput = (componentId) =>
    ensureRoot().querySelector(
      `#${componentId} textarea, #${componentId} input[type="text"], #${componentId} input[type="hidden"], #${componentId} input:not([type])`,
    );

  const getOverlayHost = () => {
    const root = ensureRoot();
    if (root instanceof ShadowRoot) return root;
    if (root instanceof Document) return root.body || root.documentElement;
    return document.body || document.documentElement;
  };

  const getComponentValue = (componentId) => {
    const input = getComponentInput(componentId);
    if (!(input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement)) return "";
    return String(input.value || "");
  };

  const setComponentValue = (componentId, value) => {
    const input = getComponentInput(componentId);
    if (!(input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement)) return;
    const nextValue = String(value ?? "");
    if ((input.value || "") === nextValue) return;
    input.value = nextValue;
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
  };

  const isElementVisible = (node) => {
    if (!(node instanceof HTMLElement)) return false;
    if (node.hidden) return false;
    if (node.getAttribute("aria-hidden") === "true") return false;
    if (node.offsetParent !== null) return true;
    const style = window.getComputedStyle(node);
    return style.display !== "none" && style.visibility !== "hidden";
  };

  const normalizeSingleLineText = (value) =>
    String(value || "")
      .replace(/\u00a0/g, " ")
      .replace(/\s+/g, " ")
      .trim();

  const normalizeTagValue = (value) => normalizeSingleLineText(value).toLowerCase();

  const parseTagValues = (rawValue) => {
    const parsed = [];
    const seen = new Set();
    String(rawValue || "")
      .split(/[\n,]+/)
      .forEach((part) => {
        const cleaned = normalizeTagValue(part);
        if (!cleaned || seen.has(cleaned)) return;
        seen.add(cleaned);
        parsed.push(cleaned);
      });
    return parsed;
  };

  const serializeTagValues = (tags) => tags.join(", ");

  const normalizeFileIdValue = (value) => {
    const parsed = Number.parseInt(String(value || "").trim(), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return "";
    return String(parsed);
  };

  const parseDeleteFileIds = (rawValue) => {
    const textValue = String(rawValue || "").trim();
    if (!textValue) return [];
    let candidates = [];
    if (textValue.startsWith("[") && textValue.endsWith("]")) {
      try {
        const parsed = JSON.parse(textValue);
        if (Array.isArray(parsed)) {
          candidates = parsed;
        }
      } catch (error) {
        void error;
      }
    } else {
      candidates = textValue.split(/[\n,\s]+/);
    }
    const normalized = [];
    const seen = new Set();
    candidates.forEach((candidate) => {
      const fileId = normalizeFileIdValue(candidate);
      if (!fileId || seen.has(fileId)) return;
      seen.add(fileId);
      normalized.push(fileId);
    });
    return normalized;
  };

  const serializeDeleteFileIds = (fileIds) => {
    if (!Array.isArray(fileIds) || !fileIds.length) return "";
    return JSON.stringify(fileIds);
  };

  const getTitleNode = () => ensureRoot().querySelector(TITLE_SELECTOR);
  const getTagsHost = () => ensureRoot().querySelector(TAGS_SELECTOR);
  const getDeleteFilesEditor = () => ensureRoot().querySelector(`#${DELETE_FILES_EDITOR_ID}`);

  const isEditModeActive = () => {
    const shell = ensureRoot().querySelector(`#${EDIT_SHELL_ID}`);
    return isElementVisible(shell);
  };

  const getSummaryRawTextarea = () => ensureRoot().querySelector(`#${SUMMARY_INPUT_ID} textarea`);

  const getSummaryPreviewContainer = () => ensureRoot().querySelector(`#${SUMMARY_PREVIEW_ID}`);

  const getSummaryPreviewEditor = () => {
    const container = getSummaryPreviewContainer();
    if (!(container instanceof HTMLElement)) return null;
    return (
      container.querySelector(".prose") ||
      container.querySelector(".md") ||
      container.querySelector(".markdown-body") ||
      container
    );
  };

  const getSummaryModeFromInput = (input) => {
    if (!(input instanceof HTMLInputElement)) return "";
    const value = String(input.value || "").trim().toLowerCase();
    if (value === "raw") return "raw";
    if (value === "preview" || value === "compiled") return "preview";
    const label = String(input.closest("label")?.textContent || "").trim().toLowerCase();
    if (label.includes("compiled") || label.includes("preview")) return "preview";
    if (label.includes("raw")) return "raw";
    return "";
  };

  const getSummaryModeValue = () => {
    const checked = ensureRoot().querySelector(`#${SUMMARY_MODE_ID} input[type="radio"]:checked`);
    return checked instanceof HTMLInputElement ? getSummaryModeFromInput(checked) : "";
  };

  const isSummaryCompiledMode = () => {
    const mode = getSummaryModeValue();
    return mode === "preview" || mode === "compiled";
  };

  const resolveSummaryModeInput = (targetMode) => {
    const normalizedTarget = String(targetMode || "").trim().toLowerCase();
    const inputs = Array.from(ensureRoot().querySelectorAll(`#${SUMMARY_MODE_ID} input[type="radio"]`)).filter(
      (node) => node instanceof HTMLInputElement,
    );
    for (const input of inputs) {
      const mode = getSummaryModeFromInput(input);
      if (normalizedTarget === "raw") {
        if (mode === "raw") return input;
        continue;
      }
      if (mode === "preview" || mode === "compiled") return input;
    }
    return null;
  };

  const activateSummaryModeInput = (input) => {
    if (!(input instanceof HTMLInputElement)) return;
    const clickable = input.closest("label");
    if (clickable instanceof HTMLElement) {
      clickable.click();
      return;
    }
    input.click();
  };

  const setSummaryRawTextareaValue = (textarea, value, { emitEvents = false, forceEmit = false } = {}) => {
    if (!(textarea instanceof HTMLTextAreaElement)) return false;
    const nextValue = String(value || "");
    const currentValue = textarea.value || "";
    const changed = currentValue !== nextValue;
    console.log("[MD_DEBUG] setSummaryRawTextareaValue:");
    console.log("  Current value:", JSON.stringify(currentValue));
    console.log("  Next value:", JSON.stringify(nextValue));
    console.log("  Changed:", changed, "emitEvents:", emitEvents, "forceEmit:", forceEmit);
    if (changed) {
      textarea.value = nextValue;
    }
    if (emitEvents && (changed || forceEmit)) {
      console.log("[MD_DEBUG] Dispatching input and change events");
      textarea.dispatchEvent(new Event("input", { bubbles: true }));
      textarea.dispatchEvent(new Event("change", { bubbles: true }));
    }
    return changed;
  };

  const collapseInlineText = (value) =>
    String(value || "")
      .replace(/\u00a0/g, " ")
      .replace(/[ \t\r\f\v]+/g, " ")
      .trim();

  const normalizeMarkdownOutput = (value) => {
    const step1 = String(value || "").replace(/\r\n/g, "\n");
    const step2 = step1.replace(/[ \t]+$/gm, "");
    const step3 = step2.trim();
    console.log("[MD_DEBUG] normalizeMarkdownOutput:");
    console.log("  Input:", JSON.stringify(value));
    console.log("  After \\r\\n->\\n:", JSON.stringify(step1));
    console.log("  After trailing space strip:", JSON.stringify(step2));
    console.log("  After trim:", JSON.stringify(step3));
    return step3;
  };

  const renderInlineMarkdown = (node) => {
    if (node instanceof Text) {
      return String(node.textContent || "").replace(/\u00a0/g, " ");
    }
    if (!(node instanceof Element)) return "";
    const tagName = node.tagName.toLowerCase();
    const childInline = () =>
      Array.from(node.childNodes)
        .map((child) => renderInlineMarkdown(child))
        .join("");
    if (tagName === "br") return "\n";
    if (tagName === "strong" || tagName === "b") {
      const content = collapseInlineText(childInline());
      return content ? `**${content}**` : "";
    }
    if (tagName === "em" || tagName === "i") {
      const content = collapseInlineText(childInline());
      return content ? `*${content}*` : "";
    }
    if (tagName === "code" && node.parentElement?.tagName.toLowerCase() !== "pre") {
      const content = collapseInlineText(node.textContent || "");
      return content ? `\`${content.replace(/`/g, "\\`")}\`` : "";
    }
    if (tagName === "a") {
      const label = collapseInlineText(childInline()) || collapseInlineText(node.textContent || "");
      const href = String(node.getAttribute("href") || "").trim();
      if (!href) return label;
      if (!label) return href;
      return `[${label}](${href})`;
    }
    if (tagName === "img") {
      const src = String(node.getAttribute("src") || "").trim();
      if (!src) return "";
      const alt = String(node.getAttribute("alt") || "").trim();
      return `![${alt}](${src})`;
    }
    return childInline();
  };

  const renderListMarkdown = (listNode, ordered, depth) => {
    const items = Array.from(listNode.children).filter((child) => child instanceof HTMLElement && child.tagName.toLowerCase() === "li");
    if (!items.length) return "";
    const indent = "  ".repeat(Math.max(0, depth));
    const lines = items.map((item, index) => {
      const marker = ordered ? `${index + 1}. ` : "- ";
      const inlineParts = [];
      const nestedParts = [];
      Array.from(item.childNodes).forEach((child) => {
        if (!(child instanceof Element)) {
          inlineParts.push(renderInlineMarkdown(child));
          return;
        }
        const childTag = child.tagName.toLowerCase();
        if (childTag === "ul") {
          const nested = renderListMarkdown(child, false, depth + 1).trim();
          if (nested) nestedParts.push(nested);
          return;
        }
        if (childTag === "ol") {
          const nested = renderListMarkdown(child, true, depth + 1).trim();
          if (nested) nestedParts.push(nested);
          return;
        }
        inlineParts.push(renderInlineMarkdown(child));
      });
      const lineContent = collapseInlineText(inlineParts.join(" "));
      let line = `${indent}${marker}${lineContent}`.trimEnd();
      if (!lineContent) {
        line = `${indent}${marker}`.trimEnd();
      }
      if (nestedParts.length) {
        line += `\n${nestedParts.join("\n")}`;
      }
      return line;
    });
    return `${lines.join("\n")}\n\n`;
  };

  const renderTableMarkdown = (tableNode) => {
    const rowNodes = Array.from(tableNode.querySelectorAll("tr"));
    if (!rowNodes.length) return "";
    const rows = rowNodes.map((rowNode) =>
      Array.from(rowNode.children)
        .filter((cell) => cell instanceof HTMLElement)
        .map((cell) => collapseInlineText(renderInlineMarkdown(cell)))
    );
    if (!rows.length) return "";
    const columnCount = rows.reduce((count, row) => Math.max(count, row.length), 0);
    if (columnCount <= 0) return "";
    const normalizedRows = rows.map((row) => {
      const nextRow = row.slice(0, columnCount);
      while (nextRow.length < columnCount) nextRow.push("");
      return nextRow;
    });
    const header = normalizedRows[0];
    const separator = Array.from({ length: columnCount }, () => "---");
    const body = normalizedRows.slice(1);
    const toRow = (cells) => `| ${cells.join(" | ")} |`;
    const lines = [toRow(header), toRow(separator), ...body.map((row) => toRow(row))];
    return `${lines.join("\n")}\n\n`;
  };

  const renderBlockMarkdown = (node, depth = 0) => {
    if (node instanceof Text) {
      const result = collapseInlineText(node.textContent || "");
      console.log(`[MD_DEBUG] renderBlockMarkdown Text node: ${JSON.stringify(node.textContent)} -> ${JSON.stringify(result)}`);
      return result;
    }
    if (!(node instanceof Element)) return "";
    const tagName = node.tagName.toLowerCase();
    console.log(`[MD_DEBUG] renderBlockMarkdown Element: <${tagName}>, innerHTML: ${node.innerHTML.substring(0, 100)}`);
    const blockChildren = () =>
      Array.from(node.childNodes)
        .map((child) => renderBlockMarkdown(child, depth))
        .join("");
    if (tagName === "br") {
      console.log("[MD_DEBUG] renderBlockMarkdown: BR tag -> returning newline");
      return "\n";
    }
    if (/^h[1-6]$/.test(tagName)) {
      const level = Number.parseInt(tagName.charAt(1), 10);
      const content = collapseInlineText(renderInlineMarkdown(node));
      if (!content) return "";
      return `${"#".repeat(Math.min(6, Math.max(1, level)))} ${content}\n\n`;
    }
    if (tagName === "p" || tagName === "div") {
      const content = collapseInlineText(renderInlineMarkdown(node));
      console.log(`[MD_DEBUG] renderBlockMarkdown P/DIV: content=${JSON.stringify(content)}, empty=${!content}`);
      if (!content) {
        console.log("[MD_DEBUG] renderBlockMarkdown: Empty P/DIV -> returning single newline");
        return "\n";
      }
      return `${content}\n\n`;
    }
    if (tagName === "pre") {
      const codeNode = node.querySelector("code");
      const rawCode = String((codeNode || node).textContent || "").replace(/\r\n/g, "\n").trim();
      if (!rawCode) return "";
      let language = "";
      if (codeNode instanceof Element) {
        const className = String(codeNode.getAttribute("class") || "");
        const languageMatch = className.match(/language-([a-z0-9_-]+)/i);
        if (languageMatch) {
          language = languageMatch[1].toLowerCase();
        }
      }
      return `\`\`\`${language}\n${rawCode}\n\`\`\`\n\n`;
    }
    if (tagName === "blockquote") {
      const content = normalizeMarkdownOutput(blockChildren());
      if (!content) return "";
      const quoted = content
        .split("\n")
        .map((line) => (line ? `> ${line}` : ">"))
        .join("\n");
      return `${quoted}\n\n`;
    }
    if (tagName === "ul") {
      return renderListMarkdown(node, false, depth);
    }
    if (tagName === "ol") {
      return renderListMarkdown(node, true, depth);
    }
    if (tagName === "li") {
      const content = collapseInlineText(renderInlineMarkdown(node));
      return content ? `${content}\n` : "";
    }
    if (tagName === "hr") {
      return "---\n\n";
    }
    if (tagName === "table") {
      return renderTableMarkdown(node);
    }
    if (tagName === "img") {
      const inline = renderInlineMarkdown(node);
      return inline ? `${inline}\n\n` : "";
    }
    return blockChildren();
  };

  const summaryHtmlToMarkdown = (root) => {
    if (!(root instanceof HTMLElement)) {
      console.log("[MD_DEBUG] summaryHtmlToMarkdown: root is not HTMLElement");
      return "";
    }
    console.log("[MD_DEBUG] summaryHtmlToMarkdown: root innerHTML:", root.innerHTML);
    console.log("[MD_DEBUG] summaryHtmlToMarkdown: root childNodes count:", root.childNodes.length);
    const markdown = Array.from(root.childNodes)
      .map((child, i) => {
        const result = renderBlockMarkdown(child, 0);
        console.log(`[MD_DEBUG] Child ${i} (${child.nodeName}): rendered as`, JSON.stringify(result));
        return result;
      })
      .join("");
    console.log("[MD_DEBUG] summaryHtmlToMarkdown: raw markdown before normalize:", JSON.stringify(markdown));
    const normalized = normalizeMarkdownOutput(markdown);
    console.log("[MD_DEBUG] summaryHtmlToMarkdown: normalized markdown:", JSON.stringify(normalized));
    return normalized;
  };

  const syncRawSummaryFromPreview = ({ emitEvents = false, force = false, forceEmit = false } = {}) => {
    console.log("[MD_DEBUG] syncRawSummaryFromPreview called with:", { emitEvents, force, forceEmit });
    const previewEditor = getSummaryPreviewEditor();
    const rawTextarea = getSummaryRawTextarea();
    if (!(previewEditor instanceof HTMLElement) || !(rawTextarea instanceof HTMLTextAreaElement)) {
      console.log("[MD_DEBUG] syncRawSummaryFromPreview: missing elements");
      return "";
    }
    if (!force && !isSummaryCompiledMode()) {
      console.log("[MD_DEBUG] syncRawSummaryFromPreview: not in compiled mode and not forced, returning current value");
      return String(rawTextarea.value || "");
    }
    const markdownValue = summaryHtmlToMarkdown(previewEditor);
    console.log("[MD_DEBUG] syncRawSummaryFromPreview: setting textarea to:", JSON.stringify(markdownValue));
    setSummaryRawTextareaValue(rawTextarea, markdownValue, { emitEvents, forceEmit });
    return markdownValue;
  };

  const scheduleRawSummarySync = () => {
    if (summarySyncScheduled) return;
    summarySyncScheduled = true;
    window.requestAnimationFrame(() => {
      summarySyncScheduled = false;
      syncRawSummaryFromPreview({ emitEvents: false });
    });
  };

  const rerenderSummaryPreviewFromRaw = () => {
    if (!isSummaryCompiledMode()) return;
    const rawInput = resolveSummaryModeInput("raw");
    const compiledInput = resolveSummaryModeInput("compiled");
    if (!(rawInput instanceof HTMLInputElement) || !(compiledInput instanceof HTMLInputElement)) return;
    activateSummaryModeInput(rawInput);
    window.setTimeout(() => {
      activateSummaryModeInput(compiledInput);
    }, 40);
  };

  const scheduleSummaryCompiledRerender = ({ immediate = false } = {}) => {
    if (summaryCompileTimerId) {
      window.clearTimeout(summaryCompileTimerId);
      summaryCompileTimerId = 0;
    }
    const run = () => {
      summaryCompileTimerId = 0;
      if (!isSummaryCompiledMode()) return;
      const markdownValue = String(syncRawSummaryFromPreview({ emitEvents: false, force: true }) || "");
      if (markdownValue === lastCompiledSummaryMarkdown) return;
      lastCompiledSummaryMarkdown = markdownValue;
      rerenderSummaryPreviewFromRaw();
    };
    if (immediate) {
      run();
      return;
    }
    summaryCompileTimerId = window.setTimeout(run, SUMMARY_COMPILE_DELAY_MS);
  };

  const refreshCompiledSummarySnapshot = () => {
    const rawTextarea = getSummaryRawTextarea();
    if (!(rawTextarea instanceof HTMLTextAreaElement)) return;
    lastCompiledSummaryMarkdown = String(rawTextarea.value || "");
  };

  const updateSummaryPreviewEditorMode = () => {
    const previewEditor = getSummaryPreviewEditor();
    if (!(previewEditor instanceof HTMLElement)) return;
    const editable = isEditModeActive() && isSummaryCompiledMode();
    previewEditor.setAttribute("contenteditable", editable ? "true" : "false");
    previewEditor.setAttribute("spellcheck", "true");
    previewEditor.classList.add(SUMMARY_VISUAL_EDITOR_CLASS);
    previewEditor.classList.toggle(SUMMARY_VISUAL_EDITOR_ACTIVE_CLASS, editable);
    if (editable) {
      previewEditor.setAttribute("role", "textbox");
      previewEditor.setAttribute("aria-label", "Compiled description editor");
    } else {
      previewEditor.removeAttribute("role");
      previewEditor.removeAttribute("aria-label");
    }
  };

  const bindSummaryPreviewEditor = () => {
    const previewEditor = getSummaryPreviewEditor();
    if (!(previewEditor instanceof HTMLElement) || boundSummaryEditors.has(previewEditor)) return;
    boundSummaryEditors.add(previewEditor);
    previewEditor.addEventListener("paste", (event) => {
      if (!isSummaryCompiledMode()) return;
      const text = (event.clipboardData || window.clipboardData)?.getData("text/plain");
      if (!text) return;
      event.preventDefault();
      if (document.queryCommandSupported?.("insertText")) {
        document.execCommand("insertText", false, text);
      } else {
        const selection = window.getSelection();
        if (selection && selection.rangeCount) {
          selection.deleteFromDocument();
          selection.getRangeAt(0).insertNode(document.createTextNode(text));
        }
      }
    });
  };

  const bindSummaryGlobalHandlers = () => {
    if (summaryGlobalHandlersBound) return;
    summaryGlobalHandlersBound = true;
    const root = ensureRoot();
    const eventRoots = [document];
    if (root instanceof ShadowRoot || root instanceof HTMLElement || root instanceof Document) {
      if (!eventRoots.includes(root)) {
        eventRoots.push(root);
      }
    }

    eventRoots.forEach((eventRoot) => {
      eventRoot.addEventListener(
        "change",
        (event) => {
          const target = event.target;
          if (!(target instanceof HTMLElement)) return;
          if (!target.closest(`#${SUMMARY_MODE_ID}`)) return;
          console.log("[MD_DEBUG] Mode change event triggered");
          let nextMode = "";
          if (target instanceof HTMLInputElement) {
            nextMode = getSummaryModeFromInput(target);
          } else {
            const modeInput = target.closest("label")?.querySelector("input[type='radio']");
            if (modeInput instanceof HTMLInputElement) {
              nextMode = getSummaryModeFromInput(modeInput);
            }
          }
          if (!nextMode) {
            nextMode = getSummaryModeValue();
          }
          console.log("[MD_DEBUG] Next mode:", nextMode);
          if (nextMode === "raw") {
            console.log("[MD_DEBUG] Switching to raw mode, calling syncRawSummaryFromPreview");
            syncRawSummaryFromPreview({ emitEvents: true, force: true });
          }
          window.setTimeout(() => {
            refreshCompiledSummarySnapshot();
            scheduleRefresh();
          }, 0);
        },
        true,
      );
      eventRoot.addEventListener(
        "pointerdown",
        (event) => {
          const target = event.target;
          if (!(target instanceof HTMLElement)) return;
          if (!target.closest(`#${SUMMARY_SAVE_BUTTON_ID}`)) return;
          if (isSummaryCompiledMode()) {
            syncRawSummaryFromPreview({ emitEvents: true, force: true, forceEmit: true });
            refreshCompiledSummarySnapshot();
          }
        },
        true,
      );
    });
  };

  const refreshSummaryEditor = () => {
    bindSummaryGlobalHandlers();
    bindSummaryPreviewEditor();
    updateSummaryPreviewEditorMode();
  };

  const collectRenderedTags = (host) =>
    Array.from(host.querySelectorAll(".source-tag"))
      .map((node) => normalizeTagValue(node.textContent || ""))
      .filter(Boolean);

  const readTagCatalogFromHost = (host) => {
    if (!(host instanceof HTMLElement)) return [];
    const rawCatalog = String(host.dataset.tagCatalog || "").trim();
    if (!rawCatalog) return [];
    try {
      const parsed = JSON.parse(rawCatalog);
      if (!Array.isArray(parsed)) return [];
      return parsed.map((tag) => normalizeTagValue(tag)).filter(Boolean);
    } catch (error) {
      void error;
      return [];
    }
  };

  const writeTagCatalogToHost = (host, tags) => {
    if (!(host instanceof HTMLElement)) return;
    const seen = new Set();
    const normalized = [];
    tags.forEach((tag) => {
      const cleaned = normalizeTagValue(tag);
      if (!cleaned || seen.has(cleaned)) return;
      seen.add(cleaned);
      normalized.push(cleaned);
    });
    host.dataset.tagCatalog = JSON.stringify(normalized);
  };

  const ensureTagCatalogIncludes = (host, tags) => {
    const merged = [...readTagCatalogFromHost(host), ...tags];
    writeTagCatalogToHost(host, merged);
    return readTagCatalogFromHost(host);
  };

  const setInlineTitleMode = (titleNode, active) => {
    if (!(titleNode instanceof HTMLElement)) return;
    titleNode.setAttribute("contenteditable", active ? "true" : "false");
    titleNode.setAttribute("spellcheck", "true");
    titleNode.classList.toggle("source-detail-field--active", active);
    if (active) {
      titleNode.setAttribute("role", "textbox");
    } else {
      titleNode.removeAttribute("role");
    }
  };

  const bindInlineTitle = (titleNode) => {
    if (!(titleNode instanceof HTMLElement) || titleNode.dataset.inlineSourceTitleBound === "1") return;
    titleNode.dataset.inlineSourceTitleBound = "1";

    titleNode.addEventListener("input", () => {
      setComponentValue(NAME_INPUT_ID, normalizeSingleLineText(titleNode.textContent || ""));
    });
    titleNode.addEventListener("blur", () => {
      const cleaned = normalizeSingleLineText(titleNode.textContent || "");
      titleNode.textContent = cleaned;
      setComponentValue(NAME_INPUT_ID, cleaned);
    });
    titleNode.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      titleNode.blur();
    });
    titleNode.addEventListener("paste", (event) => {
      const text = (event.clipboardData || window.clipboardData)?.getData("text/plain");
      if (!text) return;
      event.preventDefault();
      if (document.queryCommandSupported?.("insertText")) {
        document.execCommand("insertText", false, text);
        return;
      }
      const selection = window.getSelection();
      if (!selection || !selection.rangeCount) return;
      selection.deleteFromDocument();
      selection.getRangeAt(0).insertNode(document.createTextNode(text));
    });
  };

  const collectEditableTagValues = (host) =>
    Array.from(host.querySelectorAll(".source-tag--editable"))
      .map((node) => normalizeTagValue(node.dataset.tagValue || ""))
      .filter(Boolean);

  const closeInlineTagAddEditor = (host) => {
    if (!(host instanceof HTMLElement)) return;
    const editor = host.querySelector(".sources-create-tags-editor__add-editor");
    if (!(editor instanceof HTMLElement)) return;
    const closeHandler = editor.__sourceCloseTagAddEditor;
    if (typeof closeHandler === "function") {
      closeHandler();
      return;
    }
    editor.remove();
  };

  const syncTagsToInput = (host) => {
    const tags = collectEditableTagValues(host);
    setComponentValue(TAGS_INPUT_ID, serializeTagValues(tags));
  };

  const buildEditableTagNode = (tagValue, host) => {
    const tag = normalizeTagValue(tagValue);
    const node = document.createElement("span");
    node.className = "source-tag source-tag--editable";
    node.dataset.tagValue = tag;

    const label = document.createElement("span");
    label.className = "source-tag__label";
    label.textContent = tag;
    node.appendChild(label);

    const removeButton = document.createElement("button");
    removeButton.type = "button";
    removeButton.className = "source-tag__remove-btn";
    removeButton.textContent = "×";
    removeButton.title = `Remove "${tag}"`;
    removeButton.setAttribute("aria-label", `Remove tag ${tag}`);
    removeButton.addEventListener("click", (event) => {
      event.preventDefault();
      node.remove();
      syncTagsToInput(host);
    });
    node.appendChild(removeButton);
    return node;
  };

  const addTagToHost = (host, addButton, tagValue) => {
    const normalized = normalizeTagValue(tagValue);
    if (!normalized) return false;
    const existing = collectEditableTagValues(host);
    if (existing.includes(normalized)) return false;
    host.insertBefore(buildEditableTagNode(normalized, host), addButton);
    ensureTagCatalogIncludes(host, [normalized]);
    syncTagsToInput(host);
    return true;
  };

  const buildTagAddEditor = (host, addButton) => {
    const editor = document.createElement("span");
    editor.className = "sources-create-tags-editor__add-editor";

    const input = document.createElement("input");
    input.type = "text";
    input.className = "sources-create-tags-editor__add-input";
    input.placeholder = "new tag";
    input.autocomplete = "off";
    input.setAttribute("aria-label", "New tag");
    editor.appendChild(input);

    const cancelButton = document.createElement("button");
    cancelButton.type = "button";
    cancelButton.className = "sources-create-tags-editor__add-cancel-btn";
    cancelButton.textContent = "×";
    cancelButton.title = "Cancel tag add";
    cancelButton.setAttribute("aria-label", "Cancel tag add");
    editor.appendChild(cancelButton);

    const suggestions = document.createElement("div");
    suggestions.className = "sources-create-tags-editor__suggestions";
    suggestions.hidden = true;
    suggestions.style.position = "fixed";
    suggestions.style.zIndex = "100000";
    suggestions.style.left = "0px";
    suggestions.style.top = "0px";
    getOverlayHost().appendChild(suggestions);

    let closed = false;
    const positionSuggestions = () => {
      if (suggestions.hidden) return;
      const rect = input.getBoundingClientRect();
      if (!rect || rect.width <= 0 || rect.height <= 0) {
        suggestions.hidden = true;
        return;
      }
      const viewportPadding = 8;
      const viewportWidth = Math.max(320, window.innerWidth || document.documentElement.clientWidth || 0);
      const maxWidth = Math.max(170, Math.min(300, viewportWidth - viewportPadding * 2));
      const desiredWidth = Math.max(170, Math.round(rect.width));
      const width = Math.min(maxWidth, desiredWidth);
      let left = Math.round(rect.left);
      if (left + width > viewportWidth - viewportPadding) {
        left = Math.max(viewportPadding, viewportWidth - width - viewportPadding);
      }
      suggestions.style.left = `${left}px`;
      suggestions.style.top = `${Math.round(rect.bottom + 6)}px`;
      suggestions.style.minWidth = `${width}px`;
      suggestions.style.maxWidth = `${maxWidth}px`;
    };
    const repositionSuggestions = () => {
      positionSuggestions();
    };

    const closeEditor = ({ focusButton = false } = {}) => {
      if (closed) return;
      closed = true;
      document.removeEventListener("pointerdown", closeOnOutsidePointerDown, true);
      window.removeEventListener("resize", repositionSuggestions, true);
      window.removeEventListener("scroll", repositionSuggestions, true);
      suggestions.remove();
      editor.remove();
      addButton.hidden = false;
      addButton.disabled = false;
      if (focusButton) addButton.focus();
    };
    editor.__sourceCloseTagAddEditor = closeEditor;

    const renderSuggestions = () => {
      const query = normalizeTagValue(input.value || "");
      suggestions.replaceChildren();
      if (!query) {
        suggestions.hidden = true;
        return;
      }
      const activeSet = new Set(collectEditableTagValues(host));
      const catalog = ensureTagCatalogIncludes(host, Array.from(activeSet));
      const matches = catalog.filter((tag) => !activeSet.has(tag) && tag.includes(query)).slice(0, TAG_SUGGESTION_LIMIT);
      if (!matches.length) {
        suggestions.hidden = true;
        return;
      }
      matches.forEach((tag) => {
        const optionButton = document.createElement("button");
        optionButton.type = "button";
        optionButton.className = "sources-create-tags-editor__suggestion-btn";
        optionButton.textContent = tag;
        optionButton.setAttribute("aria-label", `Use tag ${tag}`);
        optionButton.addEventListener("mousedown", (event) => {
          event.preventDefault();
        });
        optionButton.addEventListener("click", (event) => {
          event.preventDefault();
          addTagToHost(host, addButton, tag);
          input.value = "";
          renderSuggestions();
          input.focus();
        });
        suggestions.appendChild(optionButton);
      });
      suggestions.hidden = false;
      positionSuggestions();
    };

    const commitInputValue = () => {
      const normalized = normalizeTagValue(input.value || "");
      if (!normalized) return false;
      const added = addTagToHost(host, addButton, normalized);
      input.value = "";
      renderSuggestions();
      input.focus();
      return added;
    };

    const closeOnOutsidePointerDown = (event) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (editor.contains(target) || suggestions.contains(target) || target === addButton) return;
      closeEditor();
    };

    input.addEventListener("input", () => {
      renderSuggestions();
    });
    input.addEventListener("keydown", (event) => {
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
    window.addEventListener("resize", repositionSuggestions, true);
    window.addEventListener("scroll", repositionSuggestions, true);
    return editor;
  };

  const buildTagAddButton = (host) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "source-create-tags__add-btn";
    button.textContent = "+";
    button.title = "Add tag";
    button.setAttribute("aria-label", "Add tag");
    button.addEventListener("click", (event) => {
      event.preventDefault();
      const existingEditor = host.querySelector(".sources-create-tags-editor__add-editor");
      if (existingEditor instanceof HTMLElement) {
        const existingInput = existingEditor.querySelector("input");
        if (existingInput instanceof HTMLInputElement) existingInput.focus();
        return;
      }
      button.hidden = true;
      button.disabled = true;
      const editor = buildTagAddEditor(host, button);
      host.insertBefore(editor, button);
      const input = editor.querySelector("input");
      if (input instanceof HTMLInputElement) input.focus();
    });
    return button;
  };

  const renderEditableTags = (host, tags) => {
    if (!(host instanceof HTMLElement)) return;
    closeInlineTagAddEditor(host);
    const normalizedTags = parseTagValues(Array.isArray(tags) ? tags.join(",") : String(tags || ""));
    ensureTagCatalogIncludes(host, normalizedTags);
    host.replaceChildren();
    normalizedTags.forEach((tag) => {
      host.appendChild(buildEditableTagNode(tag, host));
    });
    host.appendChild(buildTagAddButton(host));
    host.classList.add("source-browser-head__tags--editing");
    syncTagsToInput(host);
  };

  const renderReadonlyTags = (host, tags) => {
    if (!(host instanceof HTMLElement)) return;
    closeInlineTagAddEditor(host);
    const normalizedTags = parseTagValues(Array.isArray(tags) ? tags.join(",") : String(tags || ""));
    host.replaceChildren();
    host.classList.remove("source-browser-head__tags--editing");
    if (!normalizedTags.length) {
      const muted = document.createElement("span");
      muted.className = "source-tag source-tag--muted";
      muted.textContent = "no-tags";
      host.appendChild(muted);
      return;
    }
    normalizedTags.forEach((tag) => {
      const chip = document.createElement("span");
      chip.className = "source-tag";
      chip.textContent = tag;
      host.appendChild(chip);
    });
  };

  const enterInlineEdit = (titleNode, tagsHost) => {
    bindInlineTitle(titleNode);
    setInlineTitleMode(titleNode, true);

    const shouldInitialize = tagsHost.dataset.inlineSourceMode !== "1";
    if (!shouldInitialize) return;

    const currentName = normalizeSingleLineText(getComponentValue(NAME_INPUT_ID)) || normalizeSingleLineText(titleNode.textContent || "");
    titleNode.textContent = currentName;
    setComponentValue(NAME_INPUT_ID, currentName);

    const currentTags = parseTagValues(getComponentValue(TAGS_INPUT_ID));
    const seededTags = currentTags.length ? currentTags : collectRenderedTags(tagsHost);
    renderEditableTags(tagsHost, seededTags);
    tagsHost.dataset.inlineSourceMode = "1";
  };

  const leaveInlineEdit = (titleNode, tagsHost) => {
    setInlineTitleMode(titleNode, false);
    const currentName = normalizeSingleLineText(getComponentValue(NAME_INPUT_ID));
    if (currentName) {
      titleNode.textContent = currentName;
    }
    const currentTags = parseTagValues(getComponentValue(TAGS_INPUT_ID));
    renderReadonlyTags(tagsHost, currentTags);
    tagsHost.dataset.inlineSourceMode = "0";
  };

  const refreshInlineEditor = () => {
    const titleNode = getTitleNode();
    const tagsHost = getTagsHost();
    const active = isEditModeActive();

    if (!(titleNode instanceof HTMLElement) || !(tagsHost instanceof HTMLElement)) {
      inlineWasActive = false;
      return;
    }

    if (active) {
      enterInlineEdit(titleNode, tagsHost);
      inlineWasActive = true;
      return;
    }

    if (inlineWasActive || tagsHost.dataset.inlineSourceMode === "1") {
      leaveInlineEdit(titleNode, tagsHost);
    } else {
      setInlineTitleMode(titleNode, false);
    }
    inlineWasActive = false;
  };

  const setDeleteCardSelectedState = (card, selected) => {
    if (!(card instanceof HTMLElement)) return;
    card.classList.toggle("is-selected", selected);
    const button = card.querySelector(DELETE_FILE_BUTTON_SELECTOR);
    if (!(button instanceof HTMLButtonElement)) return;
    button.setAttribute("aria-pressed", selected ? "true" : "false");
    button.title = selected ? "Undo delete" : `Delete ${String(card.dataset.fileName || "file").trim() || "file"}`;
    button.setAttribute("aria-label", button.title);
  };

  const collectDeleteSelections = (editor) => {
    const selectedIds = [];
    const seen = new Set();
    editor.querySelectorAll(DELETE_FILE_CARD_SELECTOR).forEach((cardNode) => {
      if (!(cardNode instanceof HTMLElement) || !cardNode.classList.contains("is-selected")) return;
      const fileId = normalizeFileIdValue(cardNode.dataset.fileId);
      if (!fileId || seen.has(fileId)) return;
      seen.add(fileId);
      selectedIds.push(fileId);
    });
    return selectedIds;
  };

  const syncDeleteStateFromEditor = (editor) => {
    if (!(editor instanceof HTMLElement)) return;
    const selectedIds = collectDeleteSelections(editor);
    setComponentValue(DELETE_FILES_STATE_ID, serializeDeleteFileIds(selectedIds));
  };

  const applyDeleteStateToEditor = (editor) => {
    if (!(editor instanceof HTMLElement)) return;
    const selectedSet = new Set(parseDeleteFileIds(getComponentValue(DELETE_FILES_STATE_ID)));
    editor.querySelectorAll(DELETE_FILE_CARD_SELECTOR).forEach((cardNode) => {
      if (!(cardNode instanceof HTMLElement)) return;
      const fileId = normalizeFileIdValue(cardNode.dataset.fileId);
      setDeleteCardSelectedState(cardNode, Boolean(fileId && selectedSet.has(fileId)));
    });
  };

  const bindDeleteCard = (editor, card) => {
    if (!(editor instanceof HTMLElement) || !(card instanceof HTMLElement)) return;
    if (card.dataset.inlineSourceDeleteBound === "1") return;
    card.dataset.inlineSourceDeleteBound = "1";

    const button = card.querySelector(DELETE_FILE_BUTTON_SELECTOR);
    if (!(button instanceof HTMLButtonElement)) return;

    button.addEventListener("click", (event) => {
      event.preventDefault();
      const nextSelected = !card.classList.contains("is-selected");
      setDeleteCardSelectedState(card, nextSelected);
      syncDeleteStateFromEditor(editor);
    });
  };

  const refreshDeleteFilesEditor = () => {
    const editor = getDeleteFilesEditor();
    if (!(editor instanceof HTMLElement)) {
      if (getComponentValue(DELETE_FILES_STATE_ID)) {
        setComponentValue(DELETE_FILES_STATE_ID, "");
      }
      return;
    }

    const cards = Array.from(editor.querySelectorAll(DELETE_FILE_CARD_SELECTOR)).filter((node) => node instanceof HTMLElement);
    if (!cards.length) {
      if (getComponentValue(DELETE_FILES_STATE_ID)) {
        setComponentValue(DELETE_FILES_STATE_ID, "");
      }
      return;
    }

    cards.forEach((card) => bindDeleteCard(editor, card));
    applyDeleteStateToEditor(editor);
    syncDeleteStateFromEditor(editor);
  };

  const scheduleRefresh = () => {
    if (refreshScheduled) return;
    refreshScheduled = true;
    requestAnimationFrame(() => {
      refreshScheduled = false;
      refreshSummaryEditor();
      refreshInlineEditor();
      refreshDeleteFilesEditor();
    });
  };

  const init = () => {
    scheduleRefresh();
    refreshCompiledSummarySnapshot();
    const observer = new MutationObserver(() => {
      scheduleRefresh();
    });
    const root = ensureRoot();
    if (root instanceof Node && root !== document.documentElement) {
      observer.observe(root, { childList: true, subtree: true });
    }
    observer.observe(document.documentElement, { childList: true, subtree: true });
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
