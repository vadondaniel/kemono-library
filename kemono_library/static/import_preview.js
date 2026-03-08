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
          window.location.assign(payload.redirect_url);
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
    setSubmittingState();
    startImport().catch((error) => {
      clearSubmittingState();
      const message = error instanceof Error ? error.message : "Import failed.";
      window.alert(message);
    });
  });

  const cards = Array.from(form.querySelectorAll("[data-file-card]"));
  if (!cards.length) {
    return;
  }

  const searchInput = document.getElementById("file-search");
  const filtersRoot = document.getElementById("kind-filters");
  const selectedCount = document.getElementById("selected-count");
  const totalCount = document.getElementById("total-count");
  const visibleCount = document.getElementById("visible-count");
  const actionButtons = Array.from(form.querySelectorAll("[data-select-action]"));

  let activeKind = "all";
  let searchQuery = "";

  function cardCheckbox(card) {
    return card.querySelector(".file-check");
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
  }

  function matches(card) {
    const kind = card.dataset.kind || "";
    const name = card.dataset.name || "";
    const url = card.dataset.url || "";
    const kindOk = activeKind === "all" || kind === activeKind;
    const searchOk = !searchQuery || name.includes(searchQuery) || url.includes(searchQuery);
    return kindOk && searchOk;
  }

  function applyFilter() {
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
      }
    });
    updateCounters();
  }

  function buildKindFilters() {
    if (!filtersRoot) {
      return;
    }
    const kindCount = new Map();
    const order = [];
    cards.forEach((card) => {
      const kind = card.dataset.kind || "unknown";
      if (!kindCount.has(kind)) {
        kindCount.set(kind, 0);
        order.push(kind);
      }
      kindCount.set(kind, (kindCount.get(kind) || 0) + 1);
    });

    const allButton = document.createElement("button");
    allButton.type = "button";
    allButton.className = "kind-filter is-active";
    allButton.dataset.kindFilter = "all";
    allButton.textContent = `All (${cards.length})`;
    filtersRoot.appendChild(allButton);

    order.forEach((kind) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "kind-filter";
      button.dataset.kindFilter = kind;
      button.textContent = `${kind} (${kindCount.get(kind)})`;
      filtersRoot.appendChild(button);
    });

    filtersRoot.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const button = target.closest("[data-kind-filter]");
      if (!(button instanceof HTMLElement)) {
        return;
      }
      activeKind = button.dataset.kindFilter || "all";
      Array.from(filtersRoot.querySelectorAll("[data-kind-filter]")).forEach((el) => {
        el.classList.toggle("is-active", el === button);
      });
      applyFilter();
    });
  }

  cards.forEach((card) => {
    const checkbox = cardCheckbox(card);
    if (checkbox) {
      checkbox.addEventListener("change", updateCounters);
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

  buildKindFilters();
  updateCounters();
  applyFilter();
})();
