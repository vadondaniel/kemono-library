(() => {
  const overlay = document.querySelector("[data-attachment-retry-overlay]");
  const messageTarget = document.querySelector("[data-attachment-retry-message]");
  const currentTarget = document.querySelector("[data-attachment-retry-current]");
  const spinner = document.querySelector("[data-attachment-retry-spinner]");
  const progressCurrent = document.querySelector("[data-attachment-retry-progress-current]");
  const progressTotal = document.querySelector("[data-attachment-retry-progress-total]");
  const progressFailures = document.querySelector("[data-attachment-retry-progress-failures]");
  const progressPercent = document.querySelector("[data-attachment-retry-progress-percent]");
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
  const filterForm = document.querySelector("[data-attachment-filter-form]");
  const filterSearch = document.querySelector("[data-attachment-filter-search]");
  if (!(overlay instanceof HTMLElement)) {
    return;
  }

  let active = false;
  let minimized = false;
  let previewObserver = null;
  let filterSubmitTimer = null;
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

  function applyMiddleEllipsis(element) {
    if (!(element instanceof HTMLElement)) {
      return;
    }
    const fullText = (element.dataset.fullText || element.textContent || "").trim();
    if (!fullText) {
      return;
    }
    element.dataset.fullText = fullText;
    element.title = fullText;
    element.textContent = middleEllipsizeToFit(element, fullText);
  }

  function applyRetryResultMiddleEllipsis() {
    if (!(resultExamples instanceof HTMLElement)) {
      return;
    }
    Array.from(resultExamples.querySelectorAll(".attachment-retry-result-example")).forEach((node) => {
      if (node instanceof HTMLElement) {
        applyMiddleEllipsis(node);
      }
    });
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

  function setResultExamples(examples) {
    if (!(resultExamples instanceof HTMLElement)) {
      return;
    }
    resultExamples.replaceChildren();
    const normalized = Array.isArray(examples) ? examples.filter((value) => typeof value === "string" && value.trim()) : [];
    resultExamples.hidden = normalized.length === 0;
    normalized.forEach((example) => {
      const item = document.createElement("li");
      item.className = "attachment-retry-result-example";
      item.setAttribute("data-middle-ellipsis", "");
      item.dataset.fullText = example;
      item.textContent = example;
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
    setResultExamples([]);
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
    setResultExamples([]);
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
      failures > 0
        ? `${successes} succeeded, ${failures} failed, ${attempted} total.`
        : attempted > 0
          ? `${successes} succeeded, ${attempted} total.`
          : "No missing attachments matched this retry.";
    setOverlayMessage(options.failed ? "Retry stopped" : "Retry finished");
    setCurrentFile("");
    updateProgress(completed, total);
    updateFailureCount(failures);
    setResultSummary(summary);
    setResultExamples(payload.failure_examples);
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
    image.src = previewSrc;
  }

  function setupPreviewLoading() {
    const previewImages = Array.from(document.querySelectorAll("[data-attachment-preview-image]"));
    if (!previewImages.length) {
      return;
    }

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
  });

  setupPreviewLoading();
})();
