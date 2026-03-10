(() => {
  const form = document.getElementById("file-selection-form");
  if (!form) {
    return;
  }

  let isSubmitting = false;
  const submitOverlay = form.querySelector("[data-import-submit-overlay]");
  const buttons = Array.from(form.querySelectorAll("button"));
  const submitButtons = buttons.filter((button) => button.type === "submit");
  const submitButtonText = new Map(submitButtons.map((button) => [button, button.textContent]));
  const startUrl = form.dataset.importStartUrl || "";
  const statusMessage = form.querySelector("[data-import-submit-message]");
  const progressCurrent = form.querySelector("[data-import-progress-current]");
  const progressTotal = form.querySelector("[data-import-progress-total]");
  const progressPercent = form.querySelector("[data-import-progress-percent]");
  const progressFill = form.querySelector("[data-import-progress-fill]");
  const currentFile = form.querySelector("[data-import-submit-current]");

  function shortenForDisplay(value, maxLength) {
    if (typeof value !== "string") {
      return "";
    }
    if (!Number.isFinite(maxLength) || maxLength < 8 || value.length <= maxLength) {
      return value;
    }
    const head = Math.max(4, Math.floor(maxLength * 0.68));
    const tail = Math.max(3, maxLength - head - 1);
    return `${value.slice(0, head)}…${value.slice(-tail)}`;
  }

  function setSubmittingState() {
    if (isSubmitting) {
      return;
    }
    isSubmitting = true;
    form.setAttribute("aria-busy", "true");
    buttons.forEach((button) => {
      if (button.type === "submit") {
        button.textContent = "Importing...";
      }
      button.disabled = true;
    });
    if (submitOverlay instanceof HTMLElement) {
      submitOverlay.hidden = false;
    }
  }

  function clearSubmittingState() {
    isSubmitting = false;
    form.removeAttribute("aria-busy");
    buttons.forEach((button) => {
      button.disabled = false;
      if (button.type === "submit" && submitButtonText.has(button)) {
        button.textContent = submitButtonText.get(button);
      }
    });
    if (submitOverlay instanceof HTMLElement) {
      submitOverlay.hidden = true;
    }
    updateSubmitAvailability();
  }

  function setOverlayMessage(message) {
    if (statusMessage instanceof HTMLElement) {
      const text = typeof message === "string" ? message : "";
      statusMessage.textContent = shortenForDisplay(text, 120);
      statusMessage.title = text.length > 120 ? text : "";
    }
  }

  function updateProgress(completed, total, fileName, message) {
    if (progressCurrent instanceof HTMLElement) {
      progressCurrent.textContent = String(completed);
    }
    if (progressTotal instanceof HTMLElement) {
      progressTotal.textContent = String(total);
    }
    const normalizedPercent = total > 0 ? Math.max(0, Math.min(100, Math.round((completed / total) * 100))) : 0;
    if (progressPercent instanceof HTMLElement) {
      progressPercent.textContent = `${normalizedPercent}%`;
    }
    if (progressFill instanceof HTMLElement) {
      progressFill.style.width = `${normalizedPercent}%`;
    }
    if (currentFile instanceof HTMLElement) {
      const rawName = typeof fileName === "string" ? fileName : "";
      currentFile.textContent = shortenForDisplay(rawName, 96);
      currentFile.title = rawName.length > 96 ? rawName : "";
    }
    if (message) {
      setOverlayMessage(message);
    }
  }

  function delay(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  async function pollImportStatus(statusUrl) {
    while (true) {
      const response = await fetch(statusUrl, {
        method: "GET",
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload.error || "Could not read import progress.");
      }

      const total = Number(payload.total || 0);
      const completed = Number(payload.completed || 0);
      updateProgress(completed, total, payload.current_file || "", payload.message || "");

      if (payload.status === "completed") {
        if (payload.redirect_url) {
          window.location.replace(payload.redirect_url);
          return;
        }
        throw new Error("Import finished but no destination was returned.");
      }

      if (payload.status === "failed") {
        throw new Error(payload.error || payload.message || "Import failed.");
      }
      await delay(400);
    }
  }

  async function startImport() {
    if (!startUrl) {
      form.submit();
      return;
    }

    setOverlayMessage("Starting import...");
    updateProgress(0, 0, "", "");

    const response = await fetch(startUrl, {
      method: "POST",
      body: new FormData(form),
      headers: { "X-Requested-With": "XMLHttpRequest", Accept: "application/json" },
      cache: "no-store",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.status_url) {
      throw new Error(payload.error || "Could not start import.");
    }
    await pollImportStatus(payload.status_url);
  }

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    if (isSubmitting) {
      return;
    }
    if (!isFormValidForSubmit()) {
      updateSubmitAvailability();
      return;
    }
    setSubmittingState();
    startImport().catch((error) => {
      clearSubmittingState();
      const message = error instanceof Error ? error.message : "Import failed.";
      window.alert(message);
    });
  });

  const cards = Array.from(form.querySelectorAll("[data-file-card]"));
  const searchInput = document.getElementById("file-search");
  const filtersRoot = document.getElementById("kind-filters");
  const selectedCount = document.getElementById("selected-count");
  const totalCount = document.getElementById("total-count");
  const visibleCount = document.getElementById("visible-count");
  const selectedCountFooter = document.getElementById("selected-count-footer");
  const totalCountFooter = document.getElementById("total-count-footer");
  const actionButtons = Array.from(form.querySelectorAll("[data-select-action]"));
  const targetModeRadios = Array.from(form.querySelectorAll("input[name='import_target_mode']"));
  const targetPostSelect = document.getElementById("target-post-id");
  const hiddenTargetPostInput = form.querySelector("input[name='target_post_id'][type='hidden']");
  const targetPostSearch = document.getElementById("target-post-search");
  const targetChoices = Array.from(form.querySelectorAll(".import-target-choice"));
  const overwriteCheckbox = form.querySelector("input[name='overwrite_matching_version'][type='checkbox']");
  const setAsDefaultCheckbox = form.querySelector("input[name='set_as_default'][type='checkbox']");
  const versionLabelInput = document.getElementById("version-label");
  const decisionSummary = document.getElementById("import-decision-summary");
  const existingOnlyRows = Array.from(form.querySelectorAll("[data-only-existing]"));
  const duplicateWarning = document.getElementById("duplicate-warning");
  const skipKnownDuplicates = document.getElementById("skip-known-duplicates");
  const attachmentIndexScript = document.getElementById("target-attachment-index");

  let targetAttachmentIndex = {};
  if (attachmentIndexScript && attachmentIndexScript.textContent) {
    try {
      const parsed = JSON.parse(attachmentIndexScript.textContent);
      if (parsed && typeof parsed === "object") {
        targetAttachmentIndex = parsed;
      }
    } catch (_error) {
      targetAttachmentIndex = {};
    }
  }

  let activeQuickFilter = "all";
  let searchQuery = "";
  let duplicatesCount = 0;
  let missingLocalCount = 0;

  const quickFilterDefs = [
    { key: "all", label: "All" },
    { key: "images", label: "Images" },
    { key: "archives", label: "Archives" },
    { key: "inline_only", label: "Inline only" },
    { key: "missing_local", label: "Missing local" },
    { key: "known_duplicates", label: "Known duplicates" },
  ];
  const quickFilterButtons = new Map();
  const allTargetOptions =
    targetPostSelect instanceof HTMLSelectElement
      ? Array.from(targetPostSelect.options)
          .filter((option) => option.value)
          .map((option) => ({ value: option.value, text: option.textContent || "" }))
      : [];

  function cardCheckbox(card) {
    return card.querySelector(".file-check");
  }

  function sanitizeLikeBackend(rawValue) {
    if (typeof rawValue !== "string") {
      return "";
    }
    const cleaned = rawValue.replace(/[^A-Za-z0-9._-]+/g, "_").replace(/^[._]+|[._]+$/g, "");
    return cleaned.toLowerCase();
  }

  function remotePathKey(rawUrl) {
    if (typeof rawUrl !== "string" || !rawUrl.trim()) {
      return "";
    }
    const value = rawUrl.trim();
    try {
      const parsed = new URL(value, window.location.origin);
      return parsed.pathname.toLowerCase();
    } catch (_error) {
      const withoutHost = value.replace(/^https?:\/\/[^/]+/i, "");
      return withoutHost.toLowerCase();
    }
  }

  function cardNameKey(card) {
    return `name:${sanitizeLikeBackend(card.dataset.name || "")}`;
  }

  function cardPathKey(card) {
    return `path:${remotePathKey(card.dataset.url || "")}`;
  }

  function selectedMode() {
    if (!targetModeRadios.length) {
      return "existing";
    }
    return targetModeRadios.find((radio) => radio.checked)?.value || "new";
  }

  function selectedTargetPostId() {
    if (!(targetPostSelect instanceof HTMLSelectElement)) {
      if (hiddenTargetPostInput instanceof HTMLInputElement) {
        return String(hiddenTargetPostInput.value || "").trim();
      }
      return "";
    }
    return String(targetPostSelect.value || "").trim();
  }

  function syncDuplicateState() {
    duplicatesCount = 0;
    missingLocalCount = 0;
    const mode = selectedMode();
    const targetPostId = selectedTargetPostId();
    const duplicateMap =
      mode === "existing" && targetPostId && targetAttachmentIndex[targetPostId]
        ? targetAttachmentIndex[targetPostId]
        : {};

    cards.forEach((card) => {
      const nameKey = cardNameKey(card);
      const pathKey = cardPathKey(card);
      const nameState = Number(duplicateMap[nameKey] || 0);
      const pathState = Number(duplicateMap[pathKey] || 0);
      const state = Math.max(nameState, pathState);
      const isDuplicate = state > 0;
      const isMissingLocal = state === 1;
      if (isDuplicate) {
        duplicatesCount += 1;
      }
      if (isMissingLocal) {
        missingLocalCount += 1;
      }

      card.dataset.knownDuplicate = isDuplicate ? "1" : "0";
      card.dataset.missingLocal = isMissingLocal ? "1" : "0";
      card.classList.toggle("is-duplicate", isDuplicate);
      card.classList.toggle("is-missing-local", isMissingLocal);
    });

    if (duplicateWarning instanceof HTMLElement) {
      if (duplicatesCount > 0) {
        const missingText = missingLocalCount > 0 ? `, ${missingLocalCount} missing local` : "";
        duplicateWarning.hidden = false;
        duplicateWarning.textContent = `${duplicatesCount} known duplicates in target post${missingText}.`;
      } else {
        duplicateWarning.hidden = true;
        duplicateWarning.textContent = "";
      }
    }
  }

  function applySkipKnownDuplicates() {
    if (!(skipKnownDuplicates instanceof HTMLInputElement) || !skipKnownDuplicates.checked) {
      return;
    }
    cards.forEach((card) => {
      if (card.dataset.knownDuplicate !== "1") {
        return;
      }
      const checkbox = cardCheckbox(card);
      if (checkbox) {
        checkbox.checked = false;
      }
      syncCardSelectedState(card);
    });
  }

  function updateDecisionSummary() {
    if (!(decisionSummary instanceof HTMLElement)) {
      return;
    }
    const label = versionLabelInput instanceof HTMLInputElement ? versionLabelInput.value.trim() : "";
    const effectiveLabel = label || "Unnamed";
    const mode = selectedMode();
    const selected = cards.filter((card) => cardCheckbox(card)?.checked).length;
    const filesPart = cards.length ? ` importing ${selected} files` : " importing post content only";

    if (mode === "new") {
      decisionSummary.textContent = `Will create a new post as version "${effectiveLabel}"${filesPart} and set it as default.`;
      return;
    }

    let targetLabel = "selected post";
    if (targetPostSelect instanceof HTMLSelectElement && targetPostSelect.value) {
      const selectedOption = targetPostSelect.options[targetPostSelect.selectedIndex];
      if (selectedOption) {
        targetLabel = selectedOption.textContent || targetLabel;
      }
    }
    const overwriteText =
      overwriteCheckbox instanceof HTMLInputElement && overwriteCheckbox.checked
        ? " Existing source version will be overwritten if matched."
        : "";
    decisionSummary.textContent = `Will add/update version "${effectiveLabel}" on ${targetLabel}${filesPart}.${overwriteText}`;
  }

  function updateSubmitAvailability() {
    const valid = isFormValidForSubmit();
    submitButtons.forEach((button) => {
      button.disabled = !valid || isSubmitting;
      button.title = valid ? "" : "Select a target post for existing-post import mode.";
    });
  }

  function isFormValidForSubmit() {
    if (selectedMode() !== "existing") {
      return true;
    }
    if (targetPostSelect instanceof HTMLSelectElement) {
      return !!targetPostSelect.value;
    }
    if (hiddenTargetPostInput instanceof HTMLInputElement) {
      return !!hiddenTargetPostInput.value;
    }
    return false;
  }

  function updateCounters() {
    const selected = cards.filter((card) => cardCheckbox(card)?.checked).length;
    const visible = cards.filter((card) => !card.classList.contains("is-hidden")).length;
    if (selectedCount) {
      selectedCount.textContent = String(selected);
    }
    if (totalCount) {
      totalCount.textContent = String(cards.length);
    }
    if (visibleCount) {
      visibleCount.textContent = String(visible);
    }
    if (selectedCountFooter) {
      selectedCountFooter.textContent = String(selected);
    }
    if (totalCountFooter) {
      totalCountFooter.textContent = String(cards.length);
    }
    updateDecisionSummary();
    updateSubmitAvailability();
  }

  function syncCardSelectedState(card) {
    const checkbox = cardCheckbox(card);
    card.classList.toggle("is-selected", !!checkbox?.checked);
  }

  function syncTargetModeState() {
    const mode = selectedMode();
    const needsTarget = mode === "existing";
    if (targetPostSelect instanceof HTMLSelectElement) {
      targetPostSelect.disabled = !needsTarget;
      targetPostSelect.required = needsTarget;
    }
    if (targetPostSearch instanceof HTMLInputElement) {
      targetPostSearch.disabled = !needsTarget;
    }

    targetChoices.forEach((choice) => {
      const radio = choice.querySelector("input[name='import_target_mode']");
      choice.classList.toggle("is-active", !!radio?.checked);
    });

    existingOnlyRows.forEach((node) => {
      node.classList.toggle("is-hidden-row", !needsTarget);
    });

    if (!needsTarget && overwriteCheckbox instanceof HTMLInputElement) {
      overwriteCheckbox.checked = false;
    }
    if (!needsTarget && setAsDefaultCheckbox instanceof HTMLInputElement) {
      setAsDefaultCheckbox.checked = true;
    }
    syncDuplicateState();
    applySkipKnownDuplicates();
    updateQuickFilterLabels();
    applyFilter();
    updateDecisionSummary();
    updateSubmitAvailability();
  }

  function matches(card) {
    const name = card.dataset.name || "";
    const url = card.dataset.url || "";
    let quickOk = true;
    if (activeQuickFilter === "images") {
      quickOk = card.dataset.mediaGroup === "image";
    } else if (activeQuickFilter === "archives") {
      quickOk = card.dataset.mediaGroup === "archive";
    } else if (activeQuickFilter === "inline_only") {
      quickOk = card.dataset.inlineOnly === "1";
    } else if (activeQuickFilter === "missing_local") {
      quickOk = card.dataset.missingLocal === "1";
    } else if (activeQuickFilter === "known_duplicates") {
      quickOk = card.dataset.knownDuplicate === "1";
    }
    const searchOk = !searchQuery || name.includes(searchQuery) || url.includes(searchQuery);
    return quickOk && searchOk;
  }

  function applyFilter() {
    if (!cards.length) {
      updateCounters();
      return;
    }
    cards.forEach((card) => {
      card.classList.toggle("is-hidden", !matches(card));
    });
    updateCounters();
  }

  function setAll(checked, visibleOnly) {
    cards.forEach((card) => {
      if (visibleOnly && card.classList.contains("is-hidden")) {
        return;
      }
      const checkbox = cardCheckbox(card);
      if (checkbox) {
        checkbox.checked = checked;
        syncCardSelectedState(card);
      }
    });
    updateCounters();
  }

  function updateQuickFilterLabels() {
    if (!filtersRoot) {
      return;
    }
    const counts = {
      all: cards.length,
      images: cards.filter((card) => card.dataset.mediaGroup === "image").length,
      archives: cards.filter((card) => card.dataset.mediaGroup === "archive").length,
      inline_only: cards.filter((card) => card.dataset.inlineOnly === "1").length,
      missing_local: cards.filter((card) => card.dataset.missingLocal === "1").length,
      known_duplicates: cards.filter((card) => card.dataset.knownDuplicate === "1").length,
    };
    quickFilterButtons.forEach((button, key) => {
      const found = quickFilterDefs.find((entry) => entry.key === key);
      if (!found) {
        return;
      }
      button.textContent = `${found.label} (${counts[key] || 0})`;
    });
  }

  function buildQuickFilters() {
    if (!filtersRoot) {
      return;
    }
    quickFilterDefs.forEach((item) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "kind-filter";
      button.dataset.quickFilter = item.key;
      if (item.key === "all") {
        button.classList.add("is-active");
      }
      filtersRoot.appendChild(button);
      quickFilterButtons.set(item.key, button);
    });
    updateQuickFilterLabels();

    filtersRoot.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const button = target.closest("[data-quick-filter]");
      if (!(button instanceof HTMLElement)) {
        return;
      }
      activeQuickFilter = button.dataset.quickFilter || "all";
      Array.from(filtersRoot.querySelectorAll("[data-quick-filter]")).forEach((el) => {
        el.classList.toggle("is-active", el === button);
      });
      applyFilter();
    });
  }

  function rebuildTargetPostOptions(query) {
    if (!(targetPostSelect instanceof HTMLSelectElement)) {
      return;
    }
    const normalizedQuery = typeof query === "string" ? query.trim().toLowerCase() : "";
    const currentValue = targetPostSelect.value;
    const matches = !normalizedQuery
      ? allTargetOptions
      : allTargetOptions.filter((option) => {
          return option.value.includes(normalizedQuery) || option.text.toLowerCase().includes(normalizedQuery);
        });

    targetPostSelect.innerHTML = "";
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = matches.length ? "Select post" : "No matching posts";
    targetPostSelect.appendChild(placeholder);
    matches.forEach((option) => {
      const node = document.createElement("option");
      node.value = option.value;
      node.textContent = option.text;
      targetPostSelect.appendChild(node);
    });

    const stillExists = matches.some((option) => option.value === currentValue);
    targetPostSelect.value = stillExists ? currentValue : "";
  }

  cards.forEach((card) => {
    const checkbox = cardCheckbox(card);
    syncCardSelectedState(card);
    if (checkbox) {
      checkbox.addEventListener("change", () => {
        syncCardSelectedState(card);
        updateCounters();
      });
    }
  });

  if (searchInput) {
    searchInput.addEventListener("input", () => {
      searchQuery = searchInput.value.trim().toLowerCase();
      applyFilter();
    });
  }

  actionButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const action = button.getAttribute("data-select-action");
      if (action === "all") {
        setAll(true, false);
      } else if (action === "visible") {
        setAll(true, true);
      } else if (action === "none") {
        setAll(false, false);
      }
    });
  });

  if (targetPostSearch instanceof HTMLInputElement) {
    targetPostSearch.addEventListener("input", () => {
      rebuildTargetPostOptions(targetPostSearch.value);
      syncDuplicateState();
      applySkipKnownDuplicates();
      updateQuickFilterLabels();
      applyFilter();
      updateDecisionSummary();
      updateSubmitAvailability();
    });
  }

  if (targetPostSelect instanceof HTMLSelectElement) {
    targetPostSelect.addEventListener("change", () => {
      syncDuplicateState();
      applySkipKnownDuplicates();
      updateQuickFilterLabels();
      applyFilter();
      updateDecisionSummary();
      updateSubmitAvailability();
    });
  }

  if (overwriteCheckbox instanceof HTMLInputElement) {
    overwriteCheckbox.addEventListener("change", () => {
      updateDecisionSummary();
      updateSubmitAvailability();
    });
  }

  if (setAsDefaultCheckbox instanceof HTMLInputElement) {
    setAsDefaultCheckbox.addEventListener("change", updateDecisionSummary);
  }

  if (versionLabelInput instanceof HTMLInputElement) {
    versionLabelInput.addEventListener("input", updateDecisionSummary);
  }

  if (skipKnownDuplicates instanceof HTMLInputElement) {
    skipKnownDuplicates.addEventListener("change", () => {
      applySkipKnownDuplicates();
      updateCounters();
      applyFilter();
    });
  }

  targetModeRadios.forEach((radio) => {
    radio.addEventListener("change", syncTargetModeState);
  });

  syncDuplicateState();
  applySkipKnownDuplicates();
  buildQuickFilters();
  syncTargetModeState();
  updateDecisionSummary();
  updateSubmitAvailability();
  updateCounters();
  applyFilter();
})();
