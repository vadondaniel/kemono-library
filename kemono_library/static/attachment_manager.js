(() => {
  const overlay = document.querySelector("[data-attachment-retry-overlay]");
  const messageTarget = document.querySelector("[data-attachment-retry-message]");
  const currentTarget = document.querySelector("[data-attachment-retry-current]");
  const spinner = document.querySelector("[data-attachment-retry-spinner]");
  const progressCurrent = document.querySelector("[data-attachment-retry-progress-current]");
  const progressTotal = document.querySelector("[data-attachment-retry-progress-total]");
  const progressFailures = document.querySelector("[data-attachment-retry-progress-failures]");
  const progressPercent = document.querySelector("[data-attachment-retry-progress-percent]");
  const progressLine = document.querySelector(".import-submit-progress-line");
  const progressTrack = document.querySelector("[data-attachment-retry-progress-track]");
  const progressFill = document.querySelector("[data-attachment-retry-progress-fill]");
  const resultPanel = document.querySelector("[data-attachment-retry-result]");
  const resultSummary = document.querySelector("[data-attachment-retry-result-summary]");
  const resultExamples = document.querySelector("[data-attachment-retry-result-examples]");
  const resultClose = document.querySelector("[data-attachment-retry-close]");
  const dock = document.querySelector("[data-attachment-retry-dock]");
  const dockSpinner = document.querySelector("[data-attachment-retry-dock-spinner]");
  const dockMessage = document.querySelector("[data-attachment-retry-dock-message]");
  const dockMeta = document.querySelector("[data-attachment-retry-dock-meta]");
  const dockRestore = document.querySelector("[data-attachment-retry-restore]");
  const attachmentTreeRoot = document.querySelector("[data-attachment-tree-root]");
  const filterForm = document.querySelector("[data-attachment-filter-form]");
  const filterSearch = document.querySelector("[data-attachment-filter-search]");
  if (!(overlay instanceof HTMLElement)) {
    return;
  }

  let active = false;
  let minimized = false;
  let previewObserver = null;
  let filterSubmitTimer = null;
  let retryErrorTooltip = null;
  let retryErrorTooltipTarget = null;
  const measureCanvas = document.createElement("canvas");
  const measureContext = measureCanvas.getContext("2d");

  function formatBytes(value) {
    const size = Number(value);
    if (!Number.isFinite(size) || size < 0) {
      return "-";
    }
    const units = ["B", "KB", "MB", "GB", "TB"];
    let scaled = size;
    let unit = units[0];
    for (const candidate of units) {
      unit = candidate;
      if (scaled < 1024 || candidate === units[units.length - 1]) {
        break;
      }
      scaled /= 1024;
    }
    return unit === "B" ? `${Math.round(scaled)} ${unit}` : `${scaled.toFixed(1)} ${unit}`;
  }

  function measureTextWidth(element, value) {
    if (!(element instanceof HTMLElement) || !measureContext) {
      return value.length * 8;
    }
    const style = window.getComputedStyle(element);
    const font = style.font && style.font !== "normal normal normal normal 16px / normal sans-serif"
      ? style.font
      : `${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
    measureContext.font = font;
    return measureContext.measureText(value).width;
  }

  function middleEllipsizeToFit(element, value) {
    const available = Math.max(0, Math.floor(element.clientWidth || element.getBoundingClientRect().width));
    if (available <= 0 || !value) {
      return value;
    }
    if (measureTextWidth(element, value) <= available) {
      return value;
    }

    const dotIndex = value.lastIndexOf(".");
    const likelyExtension =
      dotIndex > 0 && dotIndex < value.length - 1 && value.length - dotIndex <= 9
        ? value.slice(dotIndex)
        : "";
    const minRight = likelyExtension ? Math.max(8, likelyExtension.length + 5) : 8;

    let low = 6;
    let high = Math.max(6, value.length - 4);
    let best = "";
    while (low <= high) {
      const keep = Math.floor((low + high) / 2);
      let right = Math.max(minRight, Math.round(keep * 0.4));
      right = Math.min(right, Math.max(4, value.length - 5));
      let left = keep - right;
      if (left < 3) {
        left = 3;
      }
      if (left + right + 3 > value.length) {
        right = Math.max(4, value.length - left - 3);
      }
      const candidate = `${value.slice(0, left)}...${value.slice(value.length - right)}`;
      if (measureTextWidth(element, candidate) <= available) {
        best = candidate;
        low = keep + 1;
      } else {
        high = keep - 1;
      }
    }
    if (best) {
      return best;
    }
    return `${value.slice(0, 3)}...${value.slice(-Math.max(4, minRight))}`;
  }

  function applyMiddleEllipsis(element, options = {}) {
    if (!(element instanceof HTMLElement)) {
      return;
    }
    const { setTitle = true } = options;
    const fullText = (element.dataset.fullText || element.textContent || "").trim();
    if (!fullText) {
      return;
    }
    element.dataset.fullText = fullText;
    if (setTitle) {
      element.title = fullText;
    } else {
      element.removeAttribute("title");
    }
    element.textContent = middleEllipsizeToFit(element, fullText);
  }

  function applyRetryResultMiddleEllipsis() {
    if (!(resultExamples instanceof HTMLElement)) {
      return;
    }
    Array.from(resultExamples.querySelectorAll(".attachment-retry-result-name")).forEach((node) => {
      if (node instanceof HTMLElement) {
        applyMiddleEllipsis(node, { setTitle: false });
      }
    });
  }

  function ensureRetryErrorTooltip() {
    if (retryErrorTooltip instanceof HTMLElement) {
      return retryErrorTooltip;
    }
    const tooltip = document.createElement("div");
    tooltip.className = "attachment-retry-error-tooltip";
    tooltip.hidden = true;
    document.body.appendChild(tooltip);
    retryErrorTooltip = tooltip;
    return tooltip;
  }

  function hideRetryErrorTooltip() {
    retryErrorTooltipTarget = null;
    if (!(retryErrorTooltip instanceof HTMLElement)) {
      return;
    }
    retryErrorTooltip.hidden = true;
    retryErrorTooltip.replaceChildren();
    retryErrorTooltip.removeAttribute("data-placement");
    retryErrorTooltip.style.removeProperty("--retry-tooltip-arrow-top");
    retryErrorTooltip.style.left = "";
    retryErrorTooltip.style.top = "";
    retryErrorTooltip.style.visibility = "";
  }

  function normalizeRetryFailureError(rawError, name = "") {
    const fallback = "Retry failed.";
    let text = typeof rawError === "string" ? rawError.trim() : "";
    if (!text) {
      return fallback;
    }

    const normalizedName = typeof name === "string" ? name.trim() : "";
    if (normalizedName && text.toLowerCase().startsWith(`${normalizedName.toLowerCase()}:`)) {
      text = text.slice(normalizedName.length + 1).trim();
    }

    const chunks = text
      .split(/\s*;\s+/)
      .map((chunk) => chunk.replace(/^https?:\/\/\S+\s*:\s*/i, "").trim())
      .filter(Boolean);
    if (chunks.length > 0) {
      text = chunks.join("\n");
    }

    text = text.replace(/\s+\n/g, "\n").replace(/\n\s+/g, "\n").trim();
    return text || fallback;
  }

  function setRetryErrorTooltipContent(failure) {
    if (!(retryErrorTooltip instanceof HTMLElement)) {
      return;
    }
    const normalized =
      failure && typeof failure === "object"
        ? failure
        : {
            name: "",
            context: "",
            error: typeof failure === "string" ? failure : "Retry failed.",
          };
    const title = document.createElement("strong");
    title.className = "attachment-retry-error-tooltip-title";
    title.textContent = "Error details";
    const meta = document.createElement("div");
    meta.className = "attachment-retry-error-tooltip-meta";
    const contextText = typeof normalized.context === "string" ? normalized.context.trim() : "";
    if (contextText) {
      const context = document.createElement("span");
      context.className = "attachment-retry-error-tooltip-context";
      context.textContent = `Source: ${contextText}`;
      context.title = contextText;
      meta.appendChild(context);
    }
    const body = document.createElement("span");
    body.className = "attachment-retry-error-tooltip-body";
    body.textContent =
      typeof normalized.error === "string" && normalized.error.trim() ? normalized.error.trim() : "Retry failed.";
    const children = [title];
    if (meta.childElementCount > 0) {
      children.push(meta);
    }
    children.push(body);
    retryErrorTooltip.replaceChildren(...children);
  }

  function showRetryErrorTooltip(target, failure) {
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const text =
      failure && typeof failure === "object"
        ? typeof failure.error === "string"
          ? failure.error.trim()
          : ""
        : typeof failure === "string"
          ? failure.trim()
          : "";
    if (!text) {
      hideRetryErrorTooltip();
      return;
    }

    const tooltip = ensureRetryErrorTooltip();
    retryErrorTooltipTarget = target;
    setRetryErrorTooltipContent(failure);
    tooltip.hidden = false;
    tooltip.style.visibility = "hidden";

    const targetRect = target.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();
    const viewportPadding = 12;
    const offset = 10;

    let placement = "right";
    let left = targetRect.right + offset;
    if (left + tooltipRect.width > window.innerWidth - viewportPadding) {
      placement = "left";
      left = targetRect.left - tooltipRect.width - offset;
    }
    if (left < viewportPadding) {
      placement = "right";
      left = targetRect.left;
    }
    if (left + tooltipRect.width > window.innerWidth - viewportPadding) {
      left = window.innerWidth - viewportPadding - tooltipRect.width;
    }
    left = Math.max(viewportPadding, Math.round(left));

    let top = targetRect.top + (targetRect.height - tooltipRect.height) / 2;
    if (top + tooltipRect.height > window.innerHeight - viewportPadding) {
      top = window.innerHeight - viewportPadding - tooltipRect.height;
    }
    if (top + tooltipRect.height > window.innerHeight - viewportPadding) {
      top = targetRect.top - tooltipRect.height - offset;
    }
    top = Math.max(viewportPadding, Math.round(top));

    tooltip.dataset.placement = placement;
    const arrowTop = Math.max(10, Math.min(tooltipRect.height - 10, targetRect.top + targetRect.height / 2 - top));
    tooltip.style.setProperty("--retry-tooltip-arrow-top", `${Math.round(arrowTop)}px`);
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
    tooltip.style.visibility = "";
  }

  function bindRetryFailureTooltip(item, failure) {
    if (!(item instanceof HTMLElement)) {
      return;
    }
    const text =
      failure && typeof failure === "object"
        ? typeof failure.error === "string"
          ? failure.error.trim()
          : ""
        : typeof failure === "string"
          ? failure.trim()
          : "";
    if (!text) {
      return;
    }

    item.addEventListener("mouseenter", () => {
      showRetryErrorTooltip(item, failure);
    });
    item.addEventListener("mouseleave", () => {
      if (retryErrorTooltipTarget === item) {
        hideRetryErrorTooltip();
      }
    });
    item.addEventListener("focusin", () => {
      showRetryErrorTooltip(item, failure);
    });
    item.addEventListener("focusout", () => {
      if (retryErrorTooltipTarget === item) {
        hideRetryErrorTooltip();
      }
    });
  }

  function buildFailureItemsFromPayload(payload) {
    const failures = [];
    const results = Array.isArray(payload?.results) ? payload.results : [];
    results.forEach((entry, index) => {
      if (!entry || entry.success) {
        return;
      }
      const name =
        typeof entry.name === "string" && entry.name.trim()
          ? entry.name.trim()
          : typeof entry.display_name === "string" && entry.display_name.trim()
            ? entry.display_name.trim()
            : `Attachment ${index + 1}`;
      let context = "";
      const displayName = typeof entry.display_name === "string" ? entry.display_name.trim() : "";
      if (displayName) {
        const segments = displayName.split(" / ").map((segment) => segment.trim()).filter(Boolean);
      if (segments.length > 1 && name && segments[segments.length - 1].toLowerCase() === name.toLowerCase()) {
        segments.pop();
      }
      context = segments.slice(0, 2).join(" / ");
    }
      const error = normalizeRetryFailureError(entry.error, name);
      failures.push({ name, context, error });
    });
    if (failures.length) {
      return failures;
    }

    const examples = Array.isArray(payload?.failure_examples) ? payload.failure_examples : [];
    examples.forEach((entry, index) => {
      if (typeof entry !== "string") {
        return;
      }
      const sample = entry.trim();
      if (!sample) {
        return;
      }
      const dividerIndex = sample.indexOf(": ");
      if (dividerIndex > 0) {
        const failureName = sample.slice(0, dividerIndex).trim() || `Attachment ${index + 1}`;
        failures.push({
          name: failureName,
          context: "",
          error: normalizeRetryFailureError(sample.slice(dividerIndex + 2).trim(), failureName),
        });
        return;
      }
      failures.push({ name: sample, context: "", error: normalizeRetryFailureError(sample, sample) });
    });
    return failures;
  }

  function buildLocalFileUrl(relativePath) {
    if (typeof relativePath !== "string" || !relativePath.trim()) {
      return "";
    }
    const segments = relativePath
      .trim()
      .replaceAll("\\", "/")
      .split("/")
      .filter(Boolean)
      .map((segment) => encodeURIComponent(segment));
    return segments.length ? `/files/${segments.join("/")}` : "";
  }

  function setOverlayMessage(message) {
    if (messageTarget instanceof HTMLElement) {
      messageTarget.textContent =
        typeof message === "string" && message.trim() ? message.trim() : "Retrying missing attachments...";
    }
  }

  function setCurrentFile(value) {
    if (currentTarget instanceof HTMLElement) {
      currentTarget.textContent =
        typeof value === "string" && value.trim() ? value.trim() : "Large retry batches can take a while.";
    }
  }

  function updateProgress(completed, total) {
    const normalizedCompleted = Number.isFinite(completed) ? Math.max(0, Number(completed)) : 0;
    const normalizedTotal = Number.isFinite(total) ? Math.max(0, Number(total)) : 0;
    const percent =
      normalizedTotal > 0 ? Math.max(0, Math.min(100, Math.round((normalizedCompleted / normalizedTotal) * 100))) : 0;

    if (progressCurrent instanceof HTMLElement) {
      progressCurrent.textContent = String(normalizedCompleted);
    }
    if (progressTotal instanceof HTMLElement) {
      progressTotal.textContent = String(normalizedTotal);
    }
    if (progressPercent instanceof HTMLElement) {
      progressPercent.textContent = `${percent}%`;
    }
    if (progressFill instanceof HTMLElement) {
      progressFill.style.width = `${percent}%`;
    }
  }

  function updateFailureCount(count) {
    if (progressFailures instanceof HTMLElement) {
      const normalizedCount = Number.isFinite(count) ? Math.max(0, Number(count)) : 0;
      progressFailures.textContent = `${normalizedCount} failed`;
    }
  }

  function updateDock(message, completed, total, failures, percent) {
    if (dockMessage instanceof HTMLElement) {
      dockMessage.textContent =
        typeof message === "string" && message.trim() ? message.trim() : "Retrying missing attachments...";
    }
    if (dockMeta instanceof HTMLElement) {
      const normalizedCompleted = Number.isFinite(completed) ? Math.max(0, Number(completed)) : 0;
      const normalizedTotal = Number.isFinite(total) ? Math.max(0, Number(total)) : 0;
      const normalizedFailures = Number.isFinite(failures) ? Math.max(0, Number(failures)) : 0;
      const normalizedPercent = Number.isFinite(percent) ? Math.max(0, Math.min(100, Number(percent))) : 0;
      dockMeta.textContent = `${normalizedCompleted} / ${normalizedTotal} files, ${normalizedFailures} failed, ${normalizedPercent}%`;
    }
  }

  function setDockVisible(visible) {
    if (dock instanceof HTMLElement) {
      dock.hidden = !visible;
    }
    if (dockSpinner instanceof HTMLElement) {
      dockSpinner.hidden = !visible || resultPanel?.hidden === false;
    }
  }

  function setResultVisible(visible) {
    if (resultPanel instanceof HTMLElement) {
      resultPanel.hidden = !visible;
    }
    if (spinner instanceof HTMLElement) {
      spinner.hidden = visible;
    }
    if (progressLine instanceof HTMLElement) {
      progressLine.hidden = false;
    }
    if (progressPercent instanceof HTMLElement) {
      progressPercent.hidden = visible;
    }
    if (progressTrack instanceof HTMLElement) {
      progressTrack.hidden = visible;
    }
    if (currentTarget instanceof HTMLElement) {
      currentTarget.hidden = visible;
    }
  }

  function setResultSummary(text) {
    if (resultSummary instanceof HTMLElement) {
      resultSummary.textContent = typeof text === "string" ? text : "";
    }
  }

  function setResultFailures(failures) {
    if (!(resultExamples instanceof HTMLElement)) {
      return;
    }
    hideRetryErrorTooltip();
    resultExamples.replaceChildren();
    const normalized = Array.isArray(failures)
      ? failures.filter((entry) => entry && typeof entry.name === "string" && entry.name.trim())
      : [];
    resultExamples.hidden = normalized.length === 0;
    normalized.forEach((entry) => {
      const item = document.createElement("li");
      item.className = "attachment-retry-result-example";
      item.tabIndex = 0;
      item.setAttribute("aria-label", `Failed attachment: ${entry.name.trim()}`);
      const label = document.createElement("span");
      label.className = "attachment-retry-result-name";
      label.dataset.fullText = entry.name.trim();
      label.textContent = entry.name.trim();
      item.appendChild(label);
      bindRetryFailureTooltip(item, entry);
      resultExamples.appendChild(item);
    });
    window.requestAnimationFrame(() => {
      applyRetryResultMiddleEllipsis();
    });
  }

  function setFormsDisabled(disabled) {
    const retryForms = Array.from(document.querySelectorAll("[data-attachment-retry-form]"));
    retryForms.forEach((form) => {
      if (!(form instanceof HTMLFormElement)) {
        return;
      }
      Array.from(form.elements).forEach((field) => {
        if (
          field instanceof HTMLButtonElement ||
          field instanceof HTMLInputElement ||
          field instanceof HTMLSelectElement ||
          field instanceof HTMLTextAreaElement
        ) {
          field.disabled = disabled;
        }
      });
    });
  }

  function showOverlay(message) {
    active = true;
    minimized = false;
    setOverlayMessage(message);
    setCurrentFile("");
    updateProgress(0, 0);
    updateFailureCount(0);
    updateDock(message, 0, 0, 0, 0);
    setResultVisible(false);
    setResultSummary("");
    setResultFailures([]);
    overlay.hidden = false;
    document.body.classList.add("is-busy-overlay-open");
    setDockVisible(false);
    setFormsDisabled(true);
  }

  function hideOverlay() {
    active = false;
    minimized = false;
    setResultVisible(false);
    setResultSummary("");
    setResultFailures([]);
    overlay.hidden = true;
    document.body.classList.remove("is-busy-overlay-open");
    setDockVisible(false);
    setFormsDisabled(false);
  }

  function minimizeOverlay() {
    if (!active) {
      return;
    }
    minimized = true;
    overlay.hidden = true;
    document.body.classList.remove("is-busy-overlay-open");
    setDockVisible(true);
  }

  function restoreOverlay() {
    if (!active) {
      return;
    }
    minimized = false;
    overlay.hidden = false;
    document.body.classList.add("is-busy-overlay-open");
    setDockVisible(false);
  }

  function updateGroupSummary(node) {
    if (!(node instanceof HTMLElement)) {
      return;
    }
    const summary = node.querySelector("[data-attachment-group-summary]");
    if (!(summary instanceof HTMLElement)) {
      return;
    }
    const fileCount = Number(node.dataset.fileCount || 0);
    const sizeBytes = Number(node.dataset.sizeBytes || 0);
    const missingCount = Number(node.dataset.missingCount || 0);
    const publishedDisplay = node.dataset.publishedDisplay || "";
    let text = `${fileCount} files, ${formatBytes(sizeBytes)}, ${missingCount} missing`;
    if (node.dataset.attachmentGroup === "post" && publishedDisplay) {
      text += `, ${publishedDisplay}`;
    }
    summary.textContent = text;

    const retryForm = node.querySelector("[data-attachment-retry-scope-form]");
    if (retryForm instanceof HTMLElement && missingCount <= 0) {
      retryForm.remove();
    }
  }

  function updateGlobalSummary(deltaSize, deltaMissing) {
    const missingNode = document.querySelector("[data-attachment-summary-missing-count]");
    const sizeNode = document.querySelector("[data-attachment-summary-size]");
    const retryAllForm = document.querySelector("[data-attachment-retry-all-form]");

    if (missingNode instanceof HTMLElement) {
      const currentMissing = Number(missingNode.textContent || 0);
      const nextMissing = Math.max(0, currentMissing + deltaMissing);
      missingNode.textContent = String(nextMissing);
      if (retryAllForm instanceof HTMLElement && nextMissing <= 0) {
        retryAllForm.remove();
      }
    }
    if (sizeNode instanceof HTMLElement) {
      const currentSize = Number(sizeNode.dataset.sizeBytes || 0);
      const nextSize = Math.max(0, currentSize + deltaSize);
      sizeNode.dataset.sizeBytes = String(nextSize);
      sizeNode.textContent = formatBytes(nextSize);
    }
  }

  function ensureOpenLocalLink(actions, localPath) {
    if (!(actions instanceof HTMLElement) || typeof localPath !== "string" || !localPath.trim()) {
      return;
    }
    if (actions.querySelector("[data-attachment-open-local]")) {
      return;
    }
    const openPostLink = actions.querySelector('a[href*="/posts/"]');
    const link = document.createElement("a");
    link.className = "btn btn-link btn--ghost";
    link.target = "_blank";
    link.rel = "noopener";
    link.href = buildLocalFileUrl(localPath);
    link.textContent = "Open local";
    link.setAttribute("data-attachment-open-local", "");
    if (openPostLink instanceof HTMLElement) {
      actions.insertBefore(link, openPostLink);
    } else {
      actions.appendChild(link);
    }
  }

  function applyRetryResults(results) {
    if (!Array.isArray(results)) {
      return;
    }
    results.forEach((result) => {
      if (!result || !result.success) {
        return;
      }
      const card = document.querySelector(`[data-attachment-card][data-attachment-id="${result.id}"]`);
      if (!(card instanceof HTMLElement) || card.dataset.localAvailable === "1") {
        return;
      }

      const fileSize = Number(result.file_size || 0);
      card.dataset.localAvailable = "1";
      card.classList.remove("is-missing");

      const badge = card.querySelector("[data-attachment-state-badge]");
      if (badge instanceof HTMLElement) {
        badge.textContent = "saved";
        badge.classList.remove("is-missing");
      }

      const sizeNode = card.querySelector("[data-attachment-file-size]");
      if (sizeNode instanceof HTMLElement) {
        sizeNode.dataset.sizeBytes = String(fileSize);
        sizeNode.textContent = formatBytes(fileSize);
      }

      const localPathNode = card.querySelector("[data-attachment-local-path]");
      if (localPathNode instanceof HTMLElement) {
        localPathNode.textContent =
          typeof result.local_path === "string" && result.local_path.trim() ? result.local_path.trim() : "No local path yet";
      }

      const inlineRetryForm = card.querySelector("[data-attachment-retry-inline]");
      if (inlineRetryForm instanceof HTMLElement) {
        inlineRetryForm.remove();
      }

      const actions = card.querySelector("[data-attachment-file-actions]");
      ensureOpenLocalLink(actions, result.local_path);

      ["[data-attachment-group=\"post\"]", "[data-attachment-group=\"series\"]", "[data-attachment-group=\"creator\"]"].forEach(
        (selector) => {
          const node = card.closest(selector);
          if (!(node instanceof HTMLElement)) {
            return;
          }
          const currentMissing = Number(node.dataset.missingCount || 0);
          const currentSize = Number(node.dataset.sizeBytes || 0);
          node.dataset.missingCount = String(Math.max(0, currentMissing - 1));
          node.dataset.sizeBytes = String(Math.max(0, currentSize + fileSize));
          updateGroupSummary(node);
        }
      );

      updateGlobalSummary(fileSize, -1);
    });
  }

  function showResult(payload, options = {}) {
    const completed = Number(payload.completed || 0);
    const total = Number(payload.total || 0);
    const failures = Number(payload.failure_count || 0);
    const successes = Number(payload.success_count || 0);
    const attempted = Math.max(total, completed);
    const summary =
      attempted <= 0
        ? "No missing attachments matched this retry."
        : failures <= 0
          ? "All retries succeeded."
          : successes <= 0
            ? "All retries failed."
            : "Retry finished with partial failures.";
    setOverlayMessage(options.failed ? "Retry stopped" : "Retry finished");
    setCurrentFile("");
    updateProgress(completed, total);
    updateFailureCount(failures);
    setResultSummary(summary);
    setResultFailures(buildFailureItemsFromPayload(payload));
    setResultVisible(true);
    window.requestAnimationFrame(() => {
      applyRetryResultMiddleEllipsis();
    });
    updateDock(options.failed ? "Retry stopped" : "Retry finished", completed, total, failures, total > 0 ? Math.round((completed / total) * 100) : 0);
    if (minimized) {
      setDockVisible(true);
    }
  }

  function delay(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  function submitFilterForm() {
    if (!(filterForm instanceof HTMLFormElement)) {
      return;
    }
    if (filterSubmitTimer !== null) {
      window.clearTimeout(filterSubmitTimer);
      filterSubmitTimer = null;
    }
    if (typeof filterForm.requestSubmit === "function") {
      filterForm.requestSubmit();
      return;
    }
    filterForm.submit();
  }

  function hydratePreviewImage(image) {
    if (!(image instanceof HTMLImageElement)) {
      return;
    }
    if (image.getAttribute("src")) {
      return;
    }
    const previewSrc = image.dataset.previewSrc || "";
    if (!previewSrc.trim()) {
      return;
    }
    // Reset stale broken state before a new load attempt.
    setPreviewBrokenState(image, false);
    image.src = previewSrc;
  }

  function setPreviewBrokenState(image, isBroken) {
    if (!(image instanceof HTMLImageElement)) {
      return;
    }
    const trigger = image.closest(".attachment-file-preview-trigger");
    if (!(trigger instanceof HTMLElement)) {
      return;
    }
    const placeholder = trigger.querySelector("[data-attachment-preview-placeholder]");
    trigger.classList.toggle("is-broken", Boolean(isBroken));
    if (placeholder instanceof HTMLElement) {
      placeholder.hidden = !isBroken;
    }
  }

  function bindPreviewImageState(image) {
    if (!(image instanceof HTMLImageElement)) {
      return;
    }
    if (image.dataset.previewStateBound === "1") {
      return;
    }
    image.dataset.previewStateBound = "1";
    image.addEventListener("error", () => {
      setPreviewBrokenState(image, true);
    });
    image.addEventListener("load", () => {
      setPreviewBrokenState(image, false);
    });
    const currentSrc = image.getAttribute("src");
    if (currentSrc && image.complete) {
      setPreviewBrokenState(image, image.naturalWidth <= 0);
    }
  }

  function setupPreviewLoading() {
    const previewImages = Array.from(document.querySelectorAll("[data-attachment-preview-image]"));
    if (!previewImages.length) {
      return;
    }
    previewImages.forEach((image) => {
      if (image instanceof HTMLImageElement) {
        bindPreviewImageState(image);
      }
    });

    if ("IntersectionObserver" in window) {
      if (!(previewObserver instanceof IntersectionObserver)) {
        previewObserver = new IntersectionObserver(
          (entries) => {
            entries.forEach((entry) => {
              if (!entry.isIntersecting) {
                return;
              }
              const target = entry.target;
              if (target instanceof HTMLImageElement) {
                hydratePreviewImage(target);
                if (previewObserver instanceof IntersectionObserver) {
                  previewObserver.unobserve(target);
                }
              }
            });
          },
          {
            rootMargin: "180px 0px",
            threshold: 0.01,
          }
        );
      }
      previewImages.forEach((image) => {
        if (image instanceof HTMLImageElement && !image.getAttribute("src")) {
          previewObserver.observe(image);
        }
      });
      return;
    }

    previewImages.forEach((image) => {
      if (!(image instanceof HTMLImageElement)) {
        return;
      }
      const detailsParent = image.closest("details");
      if (!(detailsParent instanceof HTMLDetailsElement) || detailsParent.open) {
        hydratePreviewImage(image);
      }
    });
  }

  async function hydrateDeferredAttachmentTree() {
    if (!(attachmentTreeRoot instanceof HTMLElement)) {
      return;
    }
    if (!attachmentTreeRoot.hasAttribute("data-attachment-tree-deferred")) {
      return;
    }
    const treeUrl = attachmentTreeRoot.dataset.attachmentTreeUrl || "";
    if (!treeUrl.trim()) {
      return;
    }

    attachmentTreeRoot.classList.add("is-loading");
    try {
      const response = await fetch(treeUrl, {
        method: "GET",
        headers: { Accept: "text/html", "X-Requested-With": "XMLHttpRequest" },
        cache: "no-store",
      });
      const html = await response.text();
      if (!response.ok) {
        throw new Error("Could not load the attachment tree.");
      }
      attachmentTreeRoot.innerHTML = html;
      attachmentTreeRoot.classList.remove("is-hydrating");
      attachmentTreeRoot.removeAttribute("data-attachment-tree-deferred");
      attachmentTreeRoot.removeAttribute("data-attachment-tree-url");
      setupPreviewLoading();
    } catch (_error) {
      attachmentTreeRoot.innerHTML =
        '<article class="panel"><p class="creator-empty">Could not load attachment tree. Reload to retry.</p></article>';
    } finally {
      attachmentTreeRoot.classList.remove("is-loading");
    }
  }

  async function pollJob(statusUrl) {
    while (true) {
      const response = await fetch(statusUrl, {
        method: "GET",
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Could not read retry progress.");
      }

      setOverlayMessage(payload.message || "Retrying missing attachments...");
      setCurrentFile(payload.current_file || "");
      const completed = Number(payload.completed || 0);
      const total = Number(payload.total || 0);
      const failures = Number(payload.failure_count || 0);
      const percent = total > 0 ? Math.round((completed / total) * 100) : 0;
      updateProgress(completed, total);
      updateFailureCount(failures);
      updateDock(payload.message || "Retrying missing attachments...", completed, total, failures, percent);

      if (payload.status === "completed") {
        applyRetryResults(payload.results);
        showResult(payload);
        return;
      }
      if (payload.status === "failed") {
        applyRetryResults(payload.results);
        showResult(payload, { failed: true });
        return;
      }
      await delay(400);
    }
  }

  async function startRetry(form, formData) {
    const startUrl = form.dataset.retryStartUrl || "";
    if (!startUrl) {
      form.submit();
      return;
    }
    const response = await fetch(startUrl, {
      method: "POST",
      body: formData,
      headers: { "X-Requested-With": "XMLHttpRequest", Accept: "application/json" },
      cache: "no-store",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.status_url) {
      throw new Error(payload.error || "Could not start attachment retry.");
    }
    await pollJob(payload.status_url);
  }

  document.addEventListener("submit", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLFormElement) || !target.matches("[data-attachment-retry-form]")) {
      return;
    }
    event.preventDefault();
    if (active) {
      return;
    }
    const formData = new FormData(target);
    showOverlay(target.dataset.retryMessage || "");
    startRetry(target, formData).catch((error) => {
      hideOverlay();
      const message = error instanceof Error ? error.message : "Attachment retry failed.";
      window.alert(message);
    });
  });

  if (dockRestore instanceof HTMLButtonElement) {
    dockRestore.addEventListener("click", restoreOverlay);
  }

  if (resultClose instanceof HTMLButtonElement) {
    resultClose.addEventListener("click", () => {
      const showingResult = resultPanel instanceof HTMLElement && !resultPanel.hidden;
      if (showingResult) {
        hideOverlay();
        return;
      }
      minimizeOverlay();
    });
  }

  if (filterForm instanceof HTMLFormElement) {
    filterForm.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLSelectElement)) {
        return;
      }
      submitFilterForm();
    });
  }

  if (filterSearch instanceof HTMLInputElement) {
    filterSearch.addEventListener("input", () => {
      if (filterSubmitTimer !== null) {
        window.clearTimeout(filterSubmitTimer);
      }
      filterSubmitTimer = window.setTimeout(() => {
        submitFilterForm();
      }, 280);
    });
  }

  document.addEventListener("toggle", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLDetailsElement) || !target.open) {
      return;
    }
    Array.from(target.querySelectorAll("[data-attachment-preview-image]")).forEach((image) => {
      if (image instanceof HTMLImageElement) {
        if (previewObserver instanceof IntersectionObserver) {
          previewObserver.observe(image);
        } else {
          hydratePreviewImage(image);
        }
      }
    });
  });

  window.addEventListener("resize", () => {
    applyRetryResultMiddleEllipsis();
    hideRetryErrorTooltip();
  });

  setupPreviewLoading();
  window.requestAnimationFrame(() => {
    hydrateDeferredAttachmentTree();
  });
})();
