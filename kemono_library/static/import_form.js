(function () {
  const root = document.querySelector("[data-import-form-tabs]");
  if (!root) {
    return;
  }

  const triggers = Array.from(root.querySelectorAll("[data-import-tab-trigger]"));
  const panels = Array.from(root.querySelectorAll("[data-import-tab-panel]"));
  const triggerByTab = new Map();
  const panelByTab = new Map();
  const titleSingle = root.getAttribute("data-import-title-single") || "";
  const titleQuick = root.getAttribute("data-import-title-quick") || titleSingle;

  function syncDocumentTitle(tab) {
    const nextTitle = tab === "quick" ? titleQuick : titleSingle;
    if (nextTitle) {
      document.title = nextTitle;
    }
  }

  for (const trigger of triggers) {
    const tab = trigger.getAttribute("data-import-tab-trigger");
    if (!tab) {
      continue;
    }
    triggerByTab.set(tab, trigger);
  }
  for (const panel of panels) {
    const tab = panel.getAttribute("data-import-tab-panel");
    if (!tab) {
      continue;
    }
    panelByTab.set(tab, panel);
  }

  function activateTab(tab) {
    const resolved = panelByTab.has(tab) ? tab : "single";
    for (const [name, trigger] of triggerByTab.entries()) {
      const active = name === resolved;
      trigger.classList.toggle("is-active", active);
      trigger.setAttribute("aria-selected", active ? "true" : "false");
      trigger.setAttribute("tabindex", active ? "0" : "-1");
    }
    for (const [name, panel] of panelByTab.entries()) {
      const active = name === resolved;
      panel.hidden = !active;
      panel.classList.toggle("is-active", active);
    }
    syncDocumentTitle(resolved);
  }

  for (const trigger of triggers) {
    trigger.addEventListener("click", () => {
      const tab = trigger.getAttribute("data-import-tab-trigger");
      if (tab) {
        activateTab(tab);
      }
    });
  }

  const defaultTab = root.getAttribute("data-import-default-tab");
  activateTab(defaultTab === "quick" ? "quick" : "single");

  const quickForm = root.querySelector("[data-quick-import-form]");
  if (!(quickForm instanceof HTMLFormElement)) {
    return;
  }
  const linkInput = quickForm.querySelector("[data-quick-link-input]");
  const addButton = quickForm.querySelector("[data-quick-link-add]");
  const pasteButton = quickForm.querySelector("[data-quick-link-paste]");
  const linkList = quickForm.querySelector("[data-quick-link-list]");
  const emptyState = quickForm.querySelector("[data-quick-link-empty]");
  const hiddenUrls = quickForm.querySelector("[data-quick-hidden-urls]");

  if (
    !(linkInput instanceof HTMLInputElement) ||
    !(addButton instanceof HTMLButtonElement) ||
    !(pasteButton instanceof HTMLButtonElement) ||
    !(linkList instanceof HTMLElement) ||
    !(emptyState instanceof HTMLElement) ||
    !(hiddenUrls instanceof HTMLTextAreaElement)
  ) {
    return;
  }

  const urls = [];
  const urlSet = new Set();

  function parseRawUrls(text) {
    return String(text || "")
      .split(/\r?\n/)
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
  }

  function addUrl(raw) {
    const normalized = String(raw || "").trim();
    if (!normalized || urlSet.has(normalized)) {
      return false;
    }
    urlSet.add(normalized);
    urls.push(normalized);
    return true;
  }

  function addUrlsFromText(rawText) {
    let changed = false;
    for (const line of String(rawText || "").split(/\r?\n/)) {
      if (addUrl(line)) {
        changed = true;
      }
    }
    if (changed) {
      renderUrls();
    }
    return changed;
  }

  function removeUrl(value) {
    if (!urlSet.has(value)) {
      return;
    }
    urlSet.delete(value);
    const idx = urls.indexOf(value);
    if (idx >= 0) {
      urls.splice(idx, 1);
    }
  }

  function syncHiddenUrls() {
    hiddenUrls.value = urls.join("\n");
  }

  function renderUrls() {
    linkList.innerHTML = "";
    const hasUrls = urls.length > 0;
    emptyState.hidden = hasUrls;
    for (const url of urls) {
      const item = document.createElement("li");
      item.className = "quick-import-link-item";

      const text = document.createElement("span");
      text.className = "quick-import-link-text";
      text.textContent = url;

      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "quick-import-link-remove";
      remove.textContent = "Remove";
      remove.setAttribute("data-quick-link-remove", url);
      remove.setAttribute("aria-label", `Remove ${url}`);

      item.append(text, remove);
      linkList.append(item);
    }
    syncHiddenUrls();
  }

  function ingestFromInput() {
    const raw = linkInput.value;
    if (!raw.trim()) {
      return;
    }
    const changed = addUrlsFromText(raw);
    linkInput.value = "";
    linkInput.setCustomValidity("");
    if (!changed) {
      linkInput.setCustomValidity("That URL is already in the list.");
      linkInput.reportValidity();
    }
  }

  function promptManualPaste() {
    const pasted = window.prompt("Paste Kemono post URL(s), one per line:", "");
    if (pasted === null) {
      return;
    }
    const changed = addUrlsFromText(pasted);
    if (!changed && pasted.trim()) {
      linkInput.focus();
      linkInput.setCustomValidity("Pasted URLs were already in the list.");
      linkInput.reportValidity();
    }
  }

  for (const rawUrl of parseRawUrls(hiddenUrls.value)) {
    addUrl(rawUrl);
  }
  renderUrls();

  addButton.addEventListener("click", ingestFromInput);
  pasteButton.addEventListener("click", async () => {
    linkInput.setCustomValidity("");
    const clipboardApi = navigator.clipboard;
    if (!clipboardApi || typeof clipboardApi.readText !== "function") {
      promptManualPaste();
      return;
    }

    try {
      const pasted = await clipboardApi.readText();
      if (!pasted.trim()) {
        return;
      }
      const changed = addUrlsFromText(pasted);
      if (!changed) {
        linkInput.focus();
        linkInput.setCustomValidity("Pasted URLs were already in the list.");
        linkInput.reportValidity();
      }
    } catch (_error) {
      promptManualPaste();
    }
  });
  linkInput.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") {
      return;
    }
    event.preventDefault();
    ingestFromInput();
  });
  linkList.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const removeTarget = target.closest("[data-quick-link-remove]");
    if (!(removeTarget instanceof HTMLElement)) {
      return;
    }
    const value = removeTarget.getAttribute("data-quick-link-remove");
    if (!value) {
      return;
    }
    removeUrl(value);
    renderUrls();
  });

  quickForm.addEventListener("submit", (event) => {
    ingestFromInput();
    for (const value of parseRawUrls(hiddenUrls.value)) {
      addUrl(value);
    }
    renderUrls();
    if (urls.length <= 0) {
      event.preventDefault();
      linkInput.setCustomValidity("Add at least one Kemono post URL.");
      linkInput.reportValidity();
      return;
    }
    linkInput.setCustomValidity("");
    for (const node of quickForm.querySelectorAll("input[data-quick-hidden-value]")) {
      node.remove();
    }
    const fragment = document.createDocumentFragment();
    for (const url of urls) {
      const hidden = document.createElement("input");
      hidden.type = "hidden";
      hidden.name = "post_url_values";
      hidden.value = url;
      hidden.setAttribute("data-quick-hidden-value", "1");
      fragment.append(hidden);
    }
    quickForm.append(fragment);
  });
})();
