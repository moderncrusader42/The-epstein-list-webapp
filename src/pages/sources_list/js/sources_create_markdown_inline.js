(function () {
  if (typeof window === "undefined") return;

  console.log("=== [MD_CREATE_DEBUG] sources_create_markdown_inline.js LOADED v24 ===");

  const SUMMARY_INPUT_ID = "sources-create-summary";
  const SUMMARY_PREVIEW_ID = "sources-create-summary-preview";
  const SUMMARY_MODE_ID = "sources-create-summary-view-mode";
  const SUMMARY_ACTIONS_ID = "sources-create-page-actions";
  const SUMMARY_VISUAL_EDITOR_CLASS = "sources-visual-editor";
  const SUMMARY_VISUAL_EDITOR_ACTIVE_CLASS = "sources-visual-editor--active";
  const COMPILE_DELAY_MS = 1000;

  let refreshScheduled = false;
  let summarySyncScheduled = false;
  let compileTimerId = 0;
  let lastRenderedMarkdown = "";

  const observedRoots = new WeakSet();
  const boundGlobalRoots = new WeakSet();
  const boundPreviewContainers = new WeakSet();
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

  const isElementVisible = (node) => {
    if (!(node instanceof HTMLElement)) return false;
    if (node.hidden) return false;
    if (node.getAttribute("aria-hidden") === "true") return false;
    if (node.offsetParent !== null) return true;
    const style = window.getComputedStyle(node);
    return style.display !== "none" && style.visibility !== "hidden";
  };

  const getRawTextarea = () => {
    const root = ensureRoot();
    const textarea = root.querySelector(`#${SUMMARY_INPUT_ID} textarea`);
    return textarea instanceof HTMLTextAreaElement ? textarea : null;
  };

  const getPreviewContainer = () => {
    const root = ensureRoot();
    const container = root.querySelector(`#${SUMMARY_PREVIEW_ID}`);
    return container instanceof HTMLElement ? container : null;
  };

  const getSummaryPreviewEditor = () => {
    const container = getPreviewContainer();
    if (!(container instanceof HTMLElement)) return null;
    return (
      container.querySelector(".prose") ||
      container.querySelector(".md") ||
      container.querySelector(".markdown-body") ||
      container
    );
  };

  const getSummaryModeValue = () => {
    const root = ensureRoot();
    const checked = root.querySelector(`#${SUMMARY_MODE_ID} input[type="radio"]:checked`);
    if (!(checked instanceof HTMLInputElement)) return "";
    const value = String(checked.value || "").trim().toLowerCase();
    if (value) return value;
    const label = String(checked.closest("label")?.textContent || "").trim().toLowerCase();
    if (label.includes("compiled") || label.includes("preview")) return "preview";
    if (label.includes("raw")) return "raw";
    return "";
  };

  const isSummaryCompiledMode = () => {
    const mode = getSummaryModeValue();
    return mode === "preview" || mode === "compiled";
  };

  const dispatchRawTextareaEvents = (textarea) => {
    console.log("[MD_CREATE_DEBUG] Dispatching input and change events on textarea");
    textarea.dispatchEvent(new Event("input", { bubbles: true, composed: true }));
    textarea.dispatchEvent(new Event("change", { bubbles: true, composed: true }));
    textarea.dispatchEvent(new Event("blur"));
  };

  const setRawTextareaValue = (textarea, value, { emitEvents = false, forceEmit = false } = {}) => {
    if (!(textarea instanceof HTMLTextAreaElement)) return false;
    const nextValue = String(value || "");
    const currentValue = textarea.value || "";
    const changed = currentValue !== nextValue;
    console.log("[MD_CREATE_DEBUG] setRawTextareaValue:", { 
      changed, 
      emitEvents, 
      forceEmit,
      currentLen: currentValue.length,
      nextLen: nextValue.length
    });
    if (changed) {
      textarea.value = nextValue;
    }
    if (emitEvents && (changed || forceEmit)) {
      dispatchRawTextareaEvents(textarea);
    }
    return changed;
  };

  const collapseInlineText = (value) =>
    String(value || "")
      .replace(/\u00a0/g, " ")
      .replace(/[ \t\r\f\v]+/g, " ")
      .trim();

  const normalizeMarkdownOutput = (value) =>
    String(value || "")
      .replace(/\r\n/g, "\n")
      .replace(/[ \t]+$/gm, "");

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
    const items = Array.from(listNode.children).filter(
      (child) => child instanceof HTMLElement && child.tagName.toLowerCase() === "li",
    );
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
        .map((cell) => collapseInlineText(renderInlineMarkdown(cell))),
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
      return collapseInlineText(node.textContent || "");
    }
    if (!(node instanceof Element)) return "";
    const tagName = node.tagName.toLowerCase();
    const blockChildren = () =>
      Array.from(node.childNodes)
        .map((child) => renderBlockMarkdown(child, depth))
        .join("");
    if (tagName === "br") {
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
      // Check if this is an nbsp-only paragraph (used for preserving blank lines)
      const rawText = String(node.textContent || "");
      const isNbspOnly = !content && /^[\s\u00a0]+$/.test(rawText) && rawText.includes("\u00a0");
      if (isNbspOnly) {
        // Return single newline - previous element already ends with \n\n
        // so this adds one more blank line (total 3 newlines = 2 blank lines)
        return "\n";
      }
      if (!content) return "\n";
      return `${content}\n\n`;
    }
    if (tagName === "pre") {
      const codeNode = node.querySelector("code");
      const rawCode = String((codeNode || node).textContent || "")
        .replace(/\r\n/g, "\n")
        .trim();
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
    if (!(root instanceof HTMLElement)) return "";
    const markdown = Array.from(root.childNodes)
      .map((child) => renderBlockMarkdown(child, 0))
      .join("");
    return normalizeMarkdownOutput(markdown);
  };

  const ensurePreviewCaretAnchor = (editor) => {
    if (!(editor instanceof HTMLElement)) return;
    const existingMarkdown = summaryHtmlToMarkdown(editor);
    if (existingMarkdown) return;
    if (editor.querySelector("br")) return;
    editor.replaceChildren();
    const paragraph = document.createElement("p");
    paragraph.appendChild(document.createElement("br"));
    editor.appendChild(paragraph);
  };

  const focusPreviewEditorAtEnd = (editor) => {
    if (!(editor instanceof HTMLElement)) return;
    ensurePreviewCaretAnchor(editor);
    editor.focus();
    if (isPreviewEditorEffectivelyEmpty(editor) && placeCaretAtStartOfEmptyEditor(editor)) {
      return;
    }
    const selection = window.getSelection();
    if (!selection) return;
    const range = document.createRange();
    range.selectNodeContents(editor);
    range.collapse(false);
    selection.removeAllRanges();
    selection.addRange(range);
  };

  const isPreviewEditorEffectivelyEmpty = (editor) => {
    if (!(editor instanceof HTMLElement)) return true;
    if (editor.querySelector("img, hr, table, blockquote, pre, ul, ol, h1, h2, h3, h4, h5, h6")) {
      return false;
    }
    return !collapseInlineText(editor.textContent || "");
  };

  const placeCaretAtStartOfEmptyEditor = (editor) => {
    if (!(editor instanceof HTMLElement)) return false;
    const selection = window.getSelection();
    if (!selection) return false;
    const firstBlock =
      editor.querySelector("p, div") ||
      (editor.firstElementChild instanceof HTMLElement ? editor.firstElementChild : null);
    try {
      const range = document.createRange();
      if (firstBlock instanceof HTMLElement) {
        range.setStart(firstBlock, 0);
      } else {
        range.setStart(editor, 0);
      }
      range.collapse(true);
      selection.removeAllRanges();
      selection.addRange(range);
      return true;
    } catch (error) {
      void error;
      return false;
    }
  };

  const _selectionOffsetWithin = (root, node, offset) => {
    if (!(root instanceof HTMLElement) || !(node instanceof Node)) return null;
    try {
      const range = document.createRange();
      range.selectNodeContents(root);
      range.setEnd(node, offset);
      return range.toString().length;
    } catch (error) {
      void error;
      return null;
    }
  };

  const captureSelectionState = (root) => {
    if (!(root instanceof HTMLElement)) return null;
    const selection = window.getSelection();
    if (!selection || selection.rangeCount < 1) return null;
    const range = selection.getRangeAt(0);
    if (!root.contains(range.startContainer) || !root.contains(range.endContainer)) return null;
    const start = _selectionOffsetWithin(root, range.startContainer, range.startOffset);
    const end = _selectionOffsetWithin(root, range.endContainer, range.endOffset);
    if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
    
    // Get total text length to compute offset from end
    const totalLength = root.textContent?.length || 0;
    const state = {
      start: Math.max(0, Number(start)),
      end: Math.max(0, Number(end)),
      offsetFromEnd: Math.max(0, totalLength - Number(end)),
      totalLength: totalLength,
      collapsed: range.collapsed,
    };
    console.log("[MD_CREATE_DEBUG] captureSelectionState:", state);
    return state;
  };

  const resolveSelectionOffset = (root, targetOffset) => {
    if (!(root instanceof HTMLElement)) return null;
    const safeTarget = Number.isFinite(targetOffset) ? Math.max(0, Number(targetOffset)) : 0;
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    let current = walker.nextNode();
    if (!(current instanceof Text)) {
      return {
        node: root,
        offset: 0,
      };
    }
    let remaining = safeTarget;
    let lastTextNode = current;
    while (current instanceof Text) {
      lastTextNode = current;
      const segment = String(current.textContent || "");
      const length = segment.length;
      if (remaining <= length) {
        return {
          node: current,
          offset: Math.max(0, Math.min(length, remaining)),
        };
      }
      remaining -= length;
      current = walker.nextNode();
    }
    return {
      node: lastTextNode,
      offset: String(lastTextNode.textContent || "").length,
    };
  };

  const restoreSelectionState = (root, state) => {
    if (!(root instanceof HTMLElement) || !state) return false;
    const selection = window.getSelection();
    if (!selection) return false;

    const newTotalLength = root.textContent?.length || 0;
    
    // Use offset from END for restoration - this handles the common case
    // of typing at the end where the start offset changes but end offset stays at 0
    let targetOffset;
    if (state.offsetFromEnd <= 2) {
      // Cursor was at or near end, place at end
      targetOffset = newTotalLength;
      console.log("[MD_CREATE_DEBUG] restoreSelectionState: placing at end");
    } else if (state.start <= 2) {
      // Cursor was at start, place at start
      targetOffset = 0;
      console.log("[MD_CREATE_DEBUG] restoreSelectionState: placing at start");
    } else {
      // Use offset from end to compute new position
      targetOffset = Math.max(0, newTotalLength - state.offsetFromEnd);
      console.log("[MD_CREATE_DEBUG] restoreSelectionState: using offsetFromEnd", state.offsetFromEnd, "->" , targetOffset);
    }
    
    const position = resolveSelectionOffset(root, targetOffset);
    if (!position) return false;
    
    console.log("[MD_CREATE_DEBUG] restoreSelectionState: resolved to", {
      node: position.node?.nodeName,
      offset: position.offset,
      targetOffset: targetOffset,
      newTotalLength: newTotalLength
    });
    
    try {
      const range = document.createRange();
      range.setStart(position.node, position.offset);
      range.collapse(true);
      selection.removeAllRanges();
      selection.addRange(range);
      console.log("[MD_CREATE_DEBUG] restoreSelectionState: SUCCESS");
      return true;
    } catch (error) {
      console.log("[MD_CREATE_DEBUG] restoreSelectionState: FAILED", error);
      return false;
    }
  };

  const escapeHtml = (value) =>
    String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const sanitizeHref = (value) => {
    const normalized = String(value || "").trim();
    if (!normalized) return "";
    const lowered = normalized.toLowerCase();
    if (
      lowered.startsWith("http://") ||
      lowered.startsWith("https://") ||
      lowered.startsWith("/") ||
      lowered.startsWith("./") ||
      lowered.startsWith("../") ||
      lowered.startsWith("#") ||
      lowered.startsWith("mailto:")
    ) {
      return normalized;
    }
    return "";
  };

  const sanitizeImageSrc = (value) => {
    const normalized = String(value || "").trim();
    if (!normalized) return "";
    const lowered = normalized.toLowerCase();
    if (
      lowered.startsWith("http://") ||
      lowered.startsWith("https://") ||
      lowered.startsWith("/") ||
      lowered.startsWith("./") ||
      lowered.startsWith("../") ||
      lowered.startsWith("data:image/")
    ) {
      return normalized;
    }
    return "";
  };

  const renderInlineMarkdownHtml = (rawText) => {
    const tokenMap = new Map();
    let tokenCount = 0;
    const reserve = (fragment) => {
      const token = `@@SRCMDTOKEN${tokenCount}@@`;
      tokenCount += 1;
      tokenMap.set(token, fragment);
      return token;
    };

    let working = String(rawText || "");
    working = working.replace(/`([^`]+)`/g, (_match, codeValue) => reserve(`<code>${escapeHtml(codeValue)}</code>`));
    working = working.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_match, rawLabel, rawHref) => {
      const href = sanitizeHref(rawHref);
      const label = escapeHtml(String(rawLabel || "").trim() || String(rawHref || "").trim());
      if (!href) return reserve(label);
      return reserve(`<a href='${escapeHtml(href)}' target='_blank' rel='noopener noreferrer'>${label}</a>`);
    });

    working = escapeHtml(working);
    working = working.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
    working = working.replace(/(^|[^*])\*([^*]+)\*/g, "$1<em>$2</em>");

    for (const [token, fragment] of tokenMap.entries()) {
      working = working.replaceAll(token, fragment);
    }
    return working;
  };

  const renderMarkdownHtml = (markdownText) => {
    const normalized = String(markdownText || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
    const lines = normalized.split("\n");
    if (!lines.some((line) => String(line || "").trim())) {
      return "<p><br /></p>";
    }

    const rendered = [];
    const paragraphLines = [];
    let listType = "";
    let inCodeBlock = false;
    const codeLines = [];
    let consecutiveEmptyLines = 0;

    const flushParagraph = () => {
      if (!paragraphLines.length) return;
      const paragraph = paragraphLines
        .map((line) => String(line || "").trim())
        .filter(Boolean)
        .join(" ");
      paragraphLines.length = 0;
      if (paragraph) {
        rendered.push(`<p>${renderInlineMarkdownHtml(paragraph)}</p>`);
      }
    };

    const flushList = () => {
      if (!listType) return;
      rendered.push(`</${listType}>`);
      listType = "";
    };

    const flushCodeBlock = () => {
      if (!inCodeBlock) return;
      rendered.push(`<pre><code>${escapeHtml(codeLines.join("\n"))}</code></pre>`);
      codeLines.length = 0;
      inCodeBlock = false;
    };

    const flushBlankLines = () => {
      // Preserve extra blank lines (3+ newlines = 2+ empty lines)
      // by rendering nbsp paragraphs for each extra line beyond 1
      console.log("[MD_CREATE_DEBUG] flushBlankLines: consecutiveEmptyLines =", consecutiveEmptyLines);
      if (consecutiveEmptyLines > 1) {
        for (let i = 1; i < consecutiveEmptyLines; i++) {
          console.log("[MD_CREATE_DEBUG] Adding nbsp paragraph for extra blank line");
          rendered.push("<p>\u00a0</p>");
        }
      }
      consecutiveEmptyLines = 0;
    };

    lines.forEach((rawLine) => {
      const line = String(rawLine || "").replace(/\s+$/g, "");
      const stripped = line.trim();

      if (inCodeBlock) {
        if (stripped.startsWith("```")) {
          flushCodeBlock();
          return;
        }
        codeLines.push(String(rawLine || ""));
        return;
      }

      if (stripped.startsWith("```")) {
        flushBlankLines();
        flushParagraph();
        flushList();
        inCodeBlock = true;
        codeLines.length = 0;
        return;
      }

      if (!stripped) {
        flushParagraph();
        flushList();
        consecutiveEmptyLines++;
        return;
      }

      // Non-empty line: flush any accumulated blank lines first
      flushBlankLines();

      const headingMatch = line.match(/^\s{0,3}(#{1,6})\s+(.*)$/);
      if (headingMatch) {
        flushParagraph();
        flushList();
        const level = Math.min(6, Math.max(1, headingMatch[1].length));
        rendered.push(`<h${level}>${renderInlineMarkdownHtml(headingMatch[2].trim())}</h${level}>`);
        return;
      }

      if (/^\s{0,3}([-*_])(\s*\1){2,}\s*$/.test(line)) {
        flushParagraph();
        flushList();
        rendered.push("<hr />");
        return;
      }

      const imageMatch = line.match(/^\s*!\[([^\]]*)\]\(([^)]+)\)\s*$/);
      if (imageMatch) {
        flushParagraph();
        flushList();
        const src = sanitizeImageSrc(imageMatch[2]);
        if (!src) return;
        const alt = escapeHtml(String(imageMatch[1] || "").trim() || "Image");
        rendered.push(`<p><img src='${escapeHtml(src)}' alt='${alt}' loading='lazy' /></p>`);
        return;
      }

      const unorderedMatch = line.match(/^\s{0,3}[-*+]\s+(.*)$/);
      if (unorderedMatch) {
        flushParagraph();
        if (listType !== "ul") {
          flushList();
          rendered.push("<ul>");
          listType = "ul";
        }
        rendered.push(`<li>${renderInlineMarkdownHtml(unorderedMatch[1].trim())}</li>`);
        return;
      }

      const orderedMatch = line.match(/^\s{0,3}\d+\.\s+(.*)$/);
      if (orderedMatch) {
        flushParagraph();
        if (listType !== "ol") {
          flushList();
          rendered.push("<ol>");
          listType = "ol";
        }
        rendered.push(`<li>${renderInlineMarkdownHtml(orderedMatch[1].trim())}</li>`);
        return;
      }

      const quoteMatch = line.match(/^\s{0,3}>\s?(.*)$/);
      if (quoteMatch) {
        flushParagraph();
        flushList();
        rendered.push(`<blockquote><p>${renderInlineMarkdownHtml(quoteMatch[1].trim())}</p></blockquote>`);
        return;
      }

      paragraphLines.push(line);
    });

    flushBlankLines();
    flushParagraph();
    flushList();
    flushCodeBlock();
    const result = rendered.length ? rendered.join("\n") : "<p><br /></p>";
    console.log("[MD_CREATE_DEBUG] renderMarkdownHtml result:", result);
    return result;
  };

  const rerenderPreviewFromRaw = ({ preserveFocus = false } = {}) => {
    if (!isSummaryCompiledMode()) return;
    const previewEditor = getSummaryPreviewEditor();
    if (!(previewEditor instanceof HTMLElement)) return;

    const textarea = getRawTextarea();
    let markdownValue = textarea instanceof HTMLTextAreaElement ? String(textarea.value || "") : "";
    if (!markdownValue) {
      markdownValue = summaryHtmlToMarkdown(previewEditor);
    }
    
    // Skip re-render if markdown hasn't changed since last render
    if (markdownValue === lastRenderedMarkdown) {
      console.log("[MD_CREATE_DEBUG] rerenderPreviewFromRaw: skipping, markdown unchanged");
      return;
    }
    
    const htmlValue = renderMarkdownHtml(markdownValue);
    console.log("[MD_CREATE_DEBUG] rerenderPreviewFromRaw: rendering, markdown length:", markdownValue.length, "html length:", htmlValue.length);
    const hadFocus =
      preserveFocus &&
      document.activeElement instanceof Node &&
      previewEditor.contains(document.activeElement);

    // Capture selection state before changing HTML
    const selectionState = hadFocus ? captureSelectionState(previewEditor) : null;
    
    lastRenderedMarkdown = markdownValue;
    previewEditor.innerHTML = htmlValue;
    updatePreviewEmptyState(previewEditor);
    
    if (hadFocus) {
      previewEditor.focus();
      // Try to restore selection position, fall back to end
      const restored = restoreSelectionState(previewEditor, selectionState);
      if (!restored) {
        const selection = window.getSelection();
        if (selection) {
          const range = document.createRange();
          range.selectNodeContents(previewEditor);
          range.collapse(false); // false = collapse to end
          selection.removeAllRanges();
          selection.addRange(range);
        }
      }
    }
  };

  const updatePreviewEmptyState = (editor) => {
    const container = getPreviewContainer();
    if (!(container instanceof HTMLElement) || !(editor instanceof HTMLElement)) return;
    const markdownValue = summaryHtmlToMarkdown(editor);
    container.dataset.createPreviewEmpty = markdownValue ? "0" : "1";
  };

  const syncRawSummaryFromPreview = ({ emitEvents = false, force = false, forceEmit = false } = {}) => {
    console.log("[MD_CREATE_DEBUG] syncRawSummaryFromPreview called:", { emitEvents, force, forceEmit });
    const previewEditor = getSummaryPreviewEditor();
    const rawTextarea = getRawTextarea();
    if (!(previewEditor instanceof HTMLElement) || !(rawTextarea instanceof HTMLTextAreaElement)) {
      console.log("[MD_CREATE_DEBUG] Missing elements, returning");
      return;
    }
    const compiledMode = isSummaryCompiledMode();
    console.log("[MD_CREATE_DEBUG] compiledMode:", compiledMode);
    if (!force && !compiledMode) {
      console.log("[MD_CREATE_DEBUG] Not in compiled mode and not forced, returning");
      return;
    }
    const markdownValue = summaryHtmlToMarkdown(previewEditor);
    console.log("[MD_CREATE_DEBUG] Synced markdown:", JSON.stringify(markdownValue));
    setRawTextareaValue(rawTextarea, markdownValue, { emitEvents, forceEmit });
    updatePreviewEmptyState(previewEditor);
  };

  const scheduleRawSummarySync = () => {
    if (summarySyncScheduled) return;
    summarySyncScheduled = true;
    window.requestAnimationFrame(() => {
      summarySyncScheduled = false;
      // Don't emit events during typing to avoid preview re-render which loses cursor
      syncRawSummaryFromPreview({ emitEvents: false });
    });
  };

  const scheduleCompileSync = ({ immediate = false, emitToGradio = false } = {}) => {
    if (compileTimerId) {
      window.clearTimeout(compileTimerId);
      compileTimerId = 0;
    }
    const run = () => {
      compileTimerId = 0;
      console.log("[MD_CREATE_DEBUG] scheduleCompileSync running, emitToGradio:", emitToGradio);
      // Sync markdown to textarea (always) - this preserves the content for saving
      syncRawSummaryFromPreview({ emitEvents: false, force: true });
      // Re-render to show compiled markdown with cursor preservation
      rerenderPreviewFromRaw({ preserveFocus: true });
      // Emit to Gradio on blur for persistence
      if (emitToGradio) {
        console.log("[MD_CREATE_DEBUG] Emitting events to Gradio for persistence");
        syncRawSummaryFromPreview({ emitEvents: true, force: true, forceEmit: true });
      }
    };
    if (!immediate) return;
    run();
  };

  const updateSummaryPreviewEditorMode = () => {
    const previewEditor = getSummaryPreviewEditor();
    const previewContainer = getPreviewContainer();
    if (!(previewEditor instanceof HTMLElement) || !(previewContainer instanceof HTMLElement)) return;

    const editable = isSummaryCompiledMode() && isElementVisible(previewContainer);
    previewEditor.classList.add(SUMMARY_VISUAL_EDITOR_CLASS);
    previewEditor.classList.toggle(SUMMARY_VISUAL_EDITOR_ACTIVE_CLASS, editable);
    previewEditor.setAttribute("contenteditable", editable ? "true" : "false");
    previewEditor.setAttribute("spellcheck", "true");

    if (editable) {
      previewEditor.setAttribute("role", "textbox");
      previewEditor.setAttribute("aria-label", "Compiled description editor");
      previewEditor.setAttribute("aria-multiline", "true");
      ensurePreviewCaretAnchor(previewEditor);
    } else {
      previewEditor.removeAttribute("role");
      previewEditor.removeAttribute("aria-label");
      previewEditor.removeAttribute("aria-multiline");
    }
    updatePreviewEmptyState(previewEditor);
  };

  const bindSummaryPreviewContainer = () => {
    const previewContainer = getPreviewContainer();
    if (!(previewContainer instanceof HTMLElement) || boundPreviewContainers.has(previewContainer)) return;
    boundPreviewContainers.add(previewContainer);

    previewContainer.addEventListener(
      "pointerdown",
      (event) => {
        if (!isSummaryCompiledMode()) return;
        const target = event.target;
        if (!(target instanceof Node)) return;
        if (target instanceof HTMLElement && target.closest("a, button, input, textarea, select, label")) return;
        const previewEditor = getSummaryPreviewEditor();
        if (!(previewEditor instanceof HTMLElement)) return;
        if (previewEditor.contains(target)) return;
        event.preventDefault();
        focusPreviewEditorAtEnd(previewEditor);
      },
      true,
    );
  };

  const bindSummaryPreviewEditor = () => {
    const previewEditor = getSummaryPreviewEditor();
    if (!(previewEditor instanceof HTMLElement) || boundSummaryEditors.has(previewEditor)) return;
    boundSummaryEditors.add(previewEditor);

    previewEditor.addEventListener("focus", () => {
      if (!isSummaryCompiledMode()) return;
      ensurePreviewCaretAnchor(previewEditor);
      updatePreviewEmptyState(previewEditor);
    });

    previewEditor.addEventListener("input", () => {
      console.log("[MD_CREATE_DEBUG] Preview editor input event");
      if (!isSummaryCompiledMode()) {
        console.log("[MD_CREATE_DEBUG] Not in compiled mode, ignoring input");
        return;
      }
      scheduleRawSummarySync();
    });

    previewEditor.addEventListener("blur", () => {
      console.log("[MD_CREATE_DEBUG] Preview editor blur event");
      if (!isSummaryCompiledMode()) {
        console.log("[MD_CREATE_DEBUG] Not in compiled mode, ignoring blur");
        return;
      }
      console.log("[MD_CREATE_DEBUG] Triggering immediate compile on blur with Gradio emit");
      scheduleCompileSync({ immediate: true, emitToGradio: true });
    });

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
      scheduleRawSummarySync();
    });
  };

  const bindSummaryGlobalHandlers = () => {
    const root = ensureRoot();
    [document, root].forEach((eventRoot) => {
      if (!(eventRoot instanceof Document || eventRoot instanceof ShadowRoot || eventRoot instanceof HTMLElement)) return;
      if (boundGlobalRoots.has(eventRoot)) return;
      boundGlobalRoots.add(eventRoot);

      eventRoot.addEventListener(
        "change",
        (event) => {
          const target = event.target;
          if (!(target instanceof HTMLElement)) return;
          if (!target.closest(`#${SUMMARY_MODE_ID}`)) return;
          // Only sync from preview when switching TO raw mode
          // When switching to preview mode, Python handles the rendering
          const mode = getSummaryModeValue();
          console.log("[MD_CREATE_DEBUG] Mode change detected, mode:", mode);
          if (mode === "raw") {
            console.log("[MD_CREATE_DEBUG] Switching to raw mode, syncing from preview");
            syncRawSummaryFromPreview({ emitEvents: true, force: true, forceEmit: true });
          }
          window.setTimeout(() => {
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
          if (!target.closest(`#${SUMMARY_ACTIONS_ID} button`)) return;
          syncRawSummaryFromPreview({ emitEvents: true, force: true, forceEmit: true });
        },
        true,
      );
    });
  };

  const refreshSummaryEditor = () => {
    bindSummaryGlobalHandlers();
    bindSummaryPreviewContainer();
    bindSummaryPreviewEditor();
    updateSummaryPreviewEditorMode();
  };

  const scheduleRefresh = () => {
    if (refreshScheduled) return;
    refreshScheduled = true;
    window.requestAnimationFrame(() => {
      refreshScheduled = false;
      refreshSummaryEditor();
    });
  };

  const attachMutationObserver = () => {
    const root = ensureRoot();
    const target = root instanceof Document ? root.documentElement : root;
    if (!(target instanceof Node) || observedRoots.has(target)) return;
    observedRoots.add(target);
    const observer = new MutationObserver(() => {
      scheduleRefresh();
    });
    observer.observe(target, { childList: true, subtree: true });
  };

  const start = () => {
    attachMutationObserver();
    scheduleRefresh();
    let frames = 0;
    const tick = () => {
      scheduleRefresh();
      frames += 1;
      if (frames < 180) {
        window.requestAnimationFrame(tick);
      }
    };
    tick();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
