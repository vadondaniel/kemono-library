(() => {
  const syncSiteHeaderHeight = () => {
    const header = document.querySelector(".site-header");
    if (!(header instanceof HTMLElement)) {
      return;
    }
    const rect = header.getBoundingClientRect();
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
    const visibleTop = Math.max(0, rect.top);
    const visibleBottom = Math.min(viewportHeight, rect.bottom);
    const visibleHeight = Math.max(0, Math.round(visibleBottom - visibleTop));
    document.documentElement.style.setProperty("--site-header-height", `${visibleHeight}px`);
  };
  syncSiteHeaderHeight();
  window.addEventListener("resize", syncSiteHeaderHeight);
  window.addEventListener("scroll", syncSiteHeaderHeight, { passive: true });
  window.requestAnimationFrame(syncSiteHeaderHeight);

  const modeSwitcher = document.querySelector("[data-post-view-mode-switcher]");
  if (modeSwitcher instanceof HTMLElement) {
    const creatorId = modeSwitcher.dataset.creatorId || "";
    const currentMode = modeSwitcher.dataset.currentMode || "classic";
    const viewParamPresent = modeSwitcher.dataset.viewParamPresent === "1";
    const storageKey = creatorId ? `kemono-post-view-mode:creator:${creatorId}` : "";

    const isKnownMode = (value) => value === "classic" || value === "reader" || value === "gallery";
    const savePreferredMode = (value) => {
      if (!storageKey || !isKnownMode(value)) {
        return;
      }
      try {
        window.localStorage.setItem(storageKey, value);
      } catch {
        // Ignore storage failures in private or restricted contexts.
      }
    };
    const readPreferredMode = () => {
      if (!storageKey) {
        return null;
      }
      try {
        const saved = window.localStorage.getItem(storageKey);
        return isKnownMode(saved) ? saved : null;
      } catch {
        return null;
      }
    };

    if (!viewParamPresent) {
      const preferredMode = readPreferredMode();
      if (preferredMode && preferredMode !== currentMode) {
        const nextUrl = new URL(window.location.href);
        nextUrl.searchParams.set("view", preferredMode);
        window.location.replace(nextUrl.toString());
        return;
      }
    }

    Array.from(modeSwitcher.querySelectorAll("[data-post-view-mode-option]")).forEach((link) => {
      if (!(link instanceof HTMLAnchorElement)) {
        return;
      }
      link.addEventListener("click", () => {
        savePreferredMode(link.dataset.viewMode || "");
      });
    });

    const modeSelect = modeSwitcher.querySelector("[data-post-view-mode-select]");
    if (modeSelect instanceof HTMLSelectElement) {
      modeSelect.addEventListener("change", () => {
        const selected = modeSelect.selectedOptions[0];
        if (!selected) {
          return;
        }
        savePreferredMode(selected.dataset.viewMode || "");
        const href = modeSelect.value;
        if (typeof href === "string" && href.trim()) {
          window.location.assign(href);
        }
      });
    }
  }

  const versionMenu = document.querySelector("[data-post-version-menu]");
  if (versionMenu instanceof HTMLDetailsElement) {
    const closeVersionMenu = () => {
      versionMenu.open = false;
    };

    document.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (!versionMenu.contains(target)) {
        closeVersionMenu();
      }
    });

    window.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeVersionMenu();
      }
    });
  }

  const pageRoot = document.querySelector("[data-post-view-root]");
  const viewMode = pageRoot instanceof HTMLElement ? pageRoot.dataset.postViewMode || "classic" : "classic";
  const isReaderView = viewMode === "reader";
  const isGalleryView = viewMode === "gallery";
  const isImageFocusView = isReaderView || isGalleryView;
  const isPageAtBottom = () => {
    const scrollingElement = document.scrollingElement;
    if (!(scrollingElement instanceof HTMLElement)) {
      return true;
    }
    const remaining = scrollingElement.scrollHeight - scrollingElement.clientHeight - scrollingElement.scrollTop;
    return remaining <= 2;
  };
  const contentRoot = document.querySelector("[data-post-content]");
  const contentSettingsRoot = document.querySelector("[data-post-content-settings]");
  const readerNavOpenButtons = Array.from(document.querySelectorAll("[data-post-reader-nav-open]")).filter(
    (node) => node instanceof HTMLElement
  );
  const readerNavCloseButton = document.querySelector("[data-post-reader-nav-close]");
  const readerNavPinButton = document.querySelector("[data-post-reader-nav-pin]");
  const readerNavOverlay = document.querySelector("[data-post-reader-nav-overlay]");
  const readerNavSheet = document.querySelector("[data-post-reader-nav-sheet]");
  const postViewShell = pageRoot instanceof HTMLElement ? pageRoot.closest(".post-view-shell") : null;
  const readerNavStateKey = "kemono-reader-nav-open";
  const readerNavPinnedStateKey = "kemono-reader-nav-pinned";
  const readReaderNavState = () => {
    try {
      return window.sessionStorage.getItem(readerNavStateKey) === "1";
    } catch {
      return false;
    }
  };
  const writeReaderNavState = (open) => {
    try {
      window.sessionStorage.setItem(readerNavStateKey, open ? "1" : "0");
    } catch {
      // Ignore session storage failures.
    }
  };
  const readReaderNavPinnedState = () => {
    try {
      return window.localStorage.getItem(readerNavPinnedStateKey) === "1";
    } catch {
      return false;
    }
  };
  const writeReaderNavPinnedState = (pinned) => {
    try {
      window.localStorage.setItem(readerNavPinnedStateKey, pinned ? "1" : "0");
    } catch {
      // Ignore storage failures.
    }
  };

  if (contentSettingsRoot instanceof HTMLDetailsElement) {
    const closeSettings = () => {
      contentSettingsRoot.open = false;
    };
    document.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (!contentSettingsRoot.contains(target)) {
        closeSettings();
      }
    });
    window.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeSettings();
      }
    });
  }

  function initializeContentSettings() {
    if (!(contentRoot instanceof HTMLElement) || !(contentSettingsRoot instanceof HTMLElement)) {
      return;
    }
    const fontSizeInput = contentSettingsRoot.querySelector("[data-post-content-font-size]");
    const lineHeightInput = contentSettingsRoot.querySelector("[data-post-content-line-height]");
    const fontFamilySelect = contentSettingsRoot.querySelector("[data-post-content-font-family]");
    const textAlignSelect = contentSettingsRoot.querySelector("[data-post-content-text-align]");
    const resetButton = contentSettingsRoot.querySelector("[data-post-content-settings-reset]");
    const fontSizeOutput = contentSettingsRoot.querySelector("[data-post-content-font-size-output]");
    const lineHeightOutput = contentSettingsRoot.querySelector("[data-post-content-line-height-output]");
    if (
      !(fontSizeInput instanceof HTMLInputElement) ||
      !(lineHeightInput instanceof HTMLInputElement) ||
      !(fontFamilySelect instanceof HTMLSelectElement) ||
      !(textAlignSelect instanceof HTMLSelectElement) ||
      !(resetButton instanceof HTMLButtonElement)
    ) {
      return;
    }

    const creatorId = pageRoot instanceof HTMLElement ? pageRoot.dataset.postCreatorId || "" : "";
    const storageKey = creatorId
      ? `kemono-post-content-settings:creator:${creatorId}`
      : "kemono-post-content-settings:global";
    const defaults = {
      fontSize: 1,
      lineHeight: 1.62,
      fontFamily: "default",
      textAlign: "start",
    };
    const fontFamilyMap = {
      default: "var(--font-body)",
      sans: '"Manrope", "Trebuchet MS", sans-serif',
      serif: '"Georgia", "Times New Roman", serif',
      mono: '"Cascadia Mono", "Consolas", "Courier New", monospace',
    };
    const allowedAlignments = new Set(["start", "left", "justify", "center"]);
    const STEP = 0.05;

    const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
    const roundToStep = (value) => Math.round(value / STEP) * STEP;
    const normalizeScale = (value) => clamp(roundToStep(value), 0.85, 1.6);
    const normalizeLineHeight = (value) => clamp(roundToStep(value), 1.2, 2.2);

    const normalizeSettings = (candidate) => {
      const raw = candidate && typeof candidate === "object" ? candidate : {};
      const fontSize =
        typeof raw.fontSize === "number" && Number.isFinite(raw.fontSize) ? normalizeScale(raw.fontSize) : defaults.fontSize;
      const lineHeight =
        typeof raw.lineHeight === "number" && Number.isFinite(raw.lineHeight)
          ? normalizeLineHeight(raw.lineHeight)
          : defaults.lineHeight;
      const fontFamily =
        typeof raw.fontFamily === "string" && raw.fontFamily in fontFamilyMap ? raw.fontFamily : defaults.fontFamily;
      const textAlign =
        typeof raw.textAlign === "string" && allowedAlignments.has(raw.textAlign) ? raw.textAlign : defaults.textAlign;
      return { fontSize, lineHeight, fontFamily, textAlign };
    };

    const readStored = () => {
      try {
        const raw = window.localStorage.getItem(storageKey);
        if (!raw) {
          return defaults;
        }
        return normalizeSettings(JSON.parse(raw));
      } catch {
        return defaults;
      }
    };

    const writeStored = (value) => {
      try {
        window.localStorage.setItem(storageKey, JSON.stringify(value));
      } catch {
        // Ignore storage failures.
      }
    };

    const applySettings = (value) => {
      contentRoot.style.setProperty("--post-content-font-size", `${value.fontSize.toFixed(2)}rem`);
      contentRoot.style.setProperty("--post-content-line-height", value.lineHeight.toFixed(2));
      contentRoot.style.setProperty("--post-content-font-family", fontFamilyMap[value.fontFamily]);
      contentRoot.style.setProperty("--post-content-text-align", value.textAlign);
      fontSizeInput.value = value.fontSize.toFixed(2);
      lineHeightInput.value = value.lineHeight.toFixed(2);
      fontFamilySelect.value = value.fontFamily;
      textAlignSelect.value = value.textAlign;
      if (fontSizeOutput instanceof HTMLOutputElement || fontSizeOutput instanceof HTMLElement) {
        fontSizeOutput.textContent = `${value.fontSize.toFixed(2)}x`;
      }
      if (lineHeightOutput instanceof HTMLOutputElement || lineHeightOutput instanceof HTMLElement) {
        lineHeightOutput.textContent = value.lineHeight.toFixed(2);
      }
    };

    let current = readStored();
    applySettings(current);

    const updateAndStore = (next) => {
      current = normalizeSettings({ ...current, ...next });
      applySettings(current);
      writeStored(current);
    };

    fontSizeInput.addEventListener("input", () => {
      const parsed = Number.parseFloat(fontSizeInput.value);
      if (Number.isFinite(parsed)) {
        updateAndStore({ fontSize: parsed });
      }
    });
    lineHeightInput.addEventListener("input", () => {
      const parsed = Number.parseFloat(lineHeightInput.value);
      if (Number.isFinite(parsed)) {
        updateAndStore({ lineHeight: parsed });
      }
    });
    fontFamilySelect.addEventListener("change", () => {
      updateAndStore({ fontFamily: fontFamilySelect.value });
    });
    textAlignSelect.addEventListener("change", () => {
      updateAndStore({ textAlign: textAlignSelect.value });
    });
    resetButton.addEventListener("click", () => {
      current = defaults;
      applySettings(current);
      writeStored(current);
    });
  }
  initializeContentSettings();

  function initializeGalleryPicker() {
    if (!isGalleryView) {
      return null;
    }
    const drawer = document.querySelector("[data-post-gallery-picker-drawer]");
    const overlay = document.querySelector("[data-post-gallery-picker-overlay]");
    if (!(drawer instanceof HTMLElement) || !(overlay instanceof HTMLElement)) {
      return null;
    }
    const shell = drawer.closest(".post-view-shell.is-gallery");
    const pinToggle = drawer.querySelector("[data-post-gallery-picker-pin-toggle]");
    const viewToggleButton = drawer.querySelector("[data-post-gallery-picker-view-toggle]");
    const viewToggleIcon = drawer.querySelector("[data-post-gallery-picker-view-icon]");
    const scopeTabs = Array.from(drawer.querySelectorAll("[data-post-gallery-picker-scope-tab]")).filter(
      (node) => node instanceof HTMLElement
    );
    const scopePanels = Array.from(drawer.querySelectorAll("[data-post-gallery-picker-scope-panel]")).filter(
      (node) => node instanceof HTMLElement
    );
    const imagesScopePanel = drawer.querySelector("[data-post-gallery-picker-scope-panel='images']");
    const openTriggers = Array.from(document.querySelectorAll("[data-post-gallery-picker-open]")).filter(
      (node) => node instanceof HTMLElement
    );
    const tabPanels = Array.from((imagesScopePanel instanceof HTMLElement ? imagesScopePanel : drawer).querySelectorAll(
      "[data-post-gallery-picker-panel]"
    )).filter((node) => node instanceof HTMLElement);
    const gridPanel = (imagesScopePanel instanceof HTMLElement ? imagesScopePanel : drawer).querySelector(
      "[data-post-gallery-picker-panel='grid']"
    );
    const gridList = gridPanel instanceof HTMLElement ? gridPanel.querySelector(".post-gallery-picker-grid") : null;
    const galleryPickerOpenStateKey = "kemono-gallery-picker-open";
    const galleryPickerPinnedStateKey = "kemono-gallery-picker-pinned";
    const galleryPickerViewStateKey = "kemono-gallery-picker-view";
    const galleryPickerScopeStateKey = "kemono-gallery-picker-scope";
    const readGalleryPickerOpenState = () => {
      try {
        return window.sessionStorage.getItem(galleryPickerOpenStateKey) === "1";
      } catch {
        return false;
      }
    };
    const writeGalleryPickerOpenState = (open) => {
      try {
        window.sessionStorage.setItem(galleryPickerOpenStateKey, open ? "1" : "0");
      } catch {
        // Ignore session storage failures.
      }
    };
    const readGalleryPickerPinnedState = () => {
      try {
        return window.localStorage.getItem(galleryPickerPinnedStateKey) === "1";
      } catch {
        return false;
      }
    };
    const writeGalleryPickerPinnedState = (nextPinned) => {
      try {
        window.localStorage.setItem(galleryPickerPinnedStateKey, nextPinned ? "1" : "0");
      } catch {
        // Ignore storage failures.
      }
    };
    const readGalleryPickerViewState = () => {
      try {
        const stored = window.localStorage.getItem(galleryPickerViewStateKey);
        return stored === "grid" ? "grid" : "list";
      } catch {
        return "list";
      }
    };
    const writeGalleryPickerViewState = (nextTab) => {
      try {
        window.localStorage.setItem(galleryPickerViewStateKey, nextTab === "grid" ? "grid" : "list");
      } catch {
        // Ignore storage failures.
      }
    };
    const readGalleryPickerScopeState = () => {
      try {
        const stored = window.localStorage.getItem(galleryPickerScopeStateKey);
        return stored === "content" ? "content" : "images";
      } catch {
        return "images";
      }
    };
    const writeGalleryPickerScopeState = (nextScope) => {
      try {
        window.localStorage.setItem(galleryPickerScopeStateKey, nextScope === "content" ? "content" : "images");
      } catch {
        // Ignore storage failures.
      }
    };
    let activeScope = readGalleryPickerScopeState();
    let activeTab = readGalleryPickerViewState();
    let lastOpenTrigger = null;
    let pinned = readGalleryPickerPinnedState();
    let gridAspectFrame = null;
    let gridAspectComputed = false;
    let gridAspectListenersBound = false;
    const GRID_RATIO_MIN = 0.66;
    const GRID_RATIO_MAX = 1.8;
    const GRID_RATIO_FALLBACK = 4 / 3;
    const GRID_RATIO_SAMPLE_LIMIT = 120;
    const GRID_VIEW_ICON =
      '<svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">' +
      '<rect x="4.5" y="4.5" width="6" height="6" rx="1.2" fill="none" stroke="currentColor" stroke-width="1.7" />' +
      '<rect x="13.5" y="4.5" width="6" height="6" rx="1.2" fill="none" stroke="currentColor" stroke-width="1.7" />' +
      '<rect x="4.5" y="13.5" width="6" height="6" rx="1.2" fill="none" stroke="currentColor" stroke-width="1.7" />' +
      '<rect x="13.5" y="13.5" width="6" height="6" rx="1.2" fill="none" stroke="currentColor" stroke-width="1.7" />' +
      "</svg>";
    const LIST_VIEW_ICON =
      '<svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">' +
      '<path d="M8 7h11M8 12h11M8 17h11" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" />' +
      '<circle cx="5.2" cy="7" r="1.1" fill="currentColor" />' +
      '<circle cx="5.2" cy="12" r="1.1" fill="currentColor" />' +
      '<circle cx="5.2" cy="17" r="1.1" fill="currentColor" />' +
      "</svg>";

    const isOpen = () => drawer.classList.contains("is-open");
    const isPinned = () => pinned;
    const clampGridRatio = (value) => Math.min(GRID_RATIO_MAX, Math.max(GRID_RATIO_MIN, value));
    const getMedian = (values) => {
      if (!Array.isArray(values) || values.length === 0) {
        return GRID_RATIO_FALLBACK;
      }
      const sorted = values.slice().sort((a, b) => a - b);
      const mid = Math.floor(sorted.length / 2);
      if (sorted.length % 2 === 1) {
        return sorted[mid];
      }
      return (sorted[mid - 1] + sorted[mid]) / 2;
    };

    const syncViewToggle = () => {
      if (!(viewToggleButton instanceof HTMLButtonElement)) {
        return;
      }
      const showViewToggle = activeScope === "images";
      viewToggleButton.hidden = !showViewToggle;
      viewToggleButton.disabled = !showViewToggle;
      if (!showViewToggle) {
        return;
      }
      const nextTab = activeTab === "grid" ? "list" : "grid";
      const label = nextTab === "grid" ? "Switch to grid view" : "Switch to list view";
      viewToggleButton.setAttribute("aria-label", label);
      viewToggleButton.title = label;
      if (viewToggleIcon instanceof HTMLElement) {
        viewToggleIcon.innerHTML = nextTab === "grid" ? GRID_VIEW_ICON : LIST_VIEW_ICON;
      }
    };

    const setActiveScope = (nextScope) => {
      const normalized = nextScope === "content" ? "content" : "images";
      activeScope = normalized;
      writeGalleryPickerScopeState(normalized);
      scopePanels.forEach((panel) => {
        panel.hidden = panel.dataset.postGalleryPickerScopePanel !== normalized;
      });
      scopeTabs.forEach((tab) => {
        const selected = tab.dataset.postGalleryPickerScopeTab === normalized;
        tab.classList.toggle("is-active", selected);
        tab.setAttribute("aria-selected", selected ? "true" : "false");
        tab.tabIndex = selected ? 0 : -1;
      });
      syncViewToggle();
    };

    const setExpandedState = (open) => {
      openTriggers.forEach((trigger) => {
        trigger.setAttribute("aria-expanded", open ? "true" : "false");
      });
    };

    const syncDrawerScrollGate = () => {
      const gated = isOpen() && !isPageAtBottom();
      drawer.classList.toggle("is-scroll-gated", gated);
    };

    const syncDrawerState = () => {
      const open = isOpen();
      const docked = open && pinned;
      drawer.classList.toggle("is-pinned", docked);
      if (shell instanceof HTMLElement) {
        shell.classList.toggle("has-pinned-picker", docked);
      }
      overlay.hidden = !(open && !docked);
      document.body.classList.toggle("is-gallery-picker-open", open && !docked);
      document.body.classList.toggle("is-gallery-picker-pinned", docked);
      setExpandedState(open);
      syncDrawerScrollGate();
      if (pinToggle instanceof HTMLButtonElement) {
        pinToggle.setAttribute("aria-pressed", pinned ? "true" : "false");
        pinToggle.setAttribute("aria-label", pinned ? "Unpin gallery file picker" : "Pin gallery file picker");
        pinToggle.title = pinned ? "Unpin panel" : "Pin panel";
      }
    };

    const collectGridRatios = () => {
      if (!(gridList instanceof HTMLElement)) {
        return [];
      }
      const imageNodes = Array.from(gridList.querySelectorAll("img"));
      if (!imageNodes.length) {
        return [];
      }
      const ratios = [];
      const step = Math.max(1, Math.floor(imageNodes.length / GRID_RATIO_SAMPLE_LIMIT));
      for (let i = 0; i < imageNodes.length; i += step) {
        const imageNode = imageNodes[i];
        if (!(imageNode instanceof HTMLImageElement)) {
          continue;
        }
        if (imageNode.naturalWidth <= 0 || imageNode.naturalHeight <= 0) {
          continue;
        }
        const ratio = imageNode.naturalWidth / imageNode.naturalHeight;
        if (Number.isFinite(ratio) && ratio > 0) {
          ratios.push(ratio);
        }
      }
      return ratios;
    };

    const applyGridAspectRatio = () => {
      if (!(gridList instanceof HTMLElement)) {
        return;
      }
      const ratios = collectGridRatios();
      if (!ratios.length) {
        return;
      }
      const medianRatio = getMedian(ratios);
      const clampedRatio = clampGridRatio(medianRatio);
      gridList.style.setProperty("--post-gallery-grid-aspect-ratio", clampedRatio.toFixed(4));
      gridAspectComputed = true;
    };

    const scheduleGridAspectRatio = () => {
      if (gridAspectFrame !== null) {
        window.cancelAnimationFrame(gridAspectFrame);
      }
      gridAspectFrame = window.requestAnimationFrame(() => {
        gridAspectFrame = null;
        applyGridAspectRatio();
      });
    };

    const bindGridAspectListeners = () => {
      if (gridAspectListenersBound || !(gridList instanceof HTMLElement)) {
        return;
      }
      gridAspectListenersBound = true;
      Array.from(gridList.querySelectorAll("img")).forEach((imageNode) => {
        if (!(imageNode instanceof HTMLImageElement)) {
          return;
        }
        imageNode.addEventListener("load", scheduleGridAspectRatio);
        imageNode.addEventListener("error", scheduleGridAspectRatio);
      });
    };

    const setActiveTab = (nextTab) => {
      const normalized = nextTab === "grid" ? "grid" : "list";
      activeTab = normalized;
      writeGalleryPickerViewState(normalized);
      tabPanels.forEach((panel) => {
        panel.hidden = panel.dataset.postGalleryPickerPanel !== normalized;
      });
      if (normalized === "grid") {
        bindGridAspectListeners();
        if (!gridAspectComputed) {
          scheduleGridAspectRatio();
        }
      }
      syncViewToggle();
    };

    const setOpen = (open, options = {}) => {
      const opts = options && typeof options === "object" ? options : {};
      const restoreFocus = opts.restoreFocus !== false;
      const focusOnOpen = opts.focusOnOpen !== false;
      const trigger = opts.trigger instanceof HTMLElement ? opts.trigger : null;
      if (open) {
        if (trigger instanceof HTMLElement) {
          lastOpenTrigger = trigger;
        }
        drawer.classList.add("is-open");
        drawer.setAttribute("aria-hidden", "false");
        syncDrawerState();
        if (focusOnOpen) {
          const focusTarget = drawer.querySelector(
            "[data-post-gallery-picker-scope-tab].is-active, [data-post-gallery-picker-view-toggle], [data-post-gallery-picker-pin-toggle]"
          );
          if (focusTarget instanceof HTMLElement) {
            window.requestAnimationFrame(() => {
              focusTarget.focus();
            });
          }
        }
      } else {
        drawer.classList.remove("is-open");
        drawer.setAttribute("aria-hidden", "true");
        syncDrawerState();
        if (restoreFocus && lastOpenTrigger instanceof HTMLElement && lastOpenTrigger.isConnected) {
          window.requestAnimationFrame(() => {
            lastOpenTrigger.focus();
          });
        }
      }
      writeGalleryPickerOpenState(open);
    };

    openTriggers.forEach((trigger) => {
      trigger.addEventListener("click", (event) => {
        event.preventDefault();
        if (isOpen()) {
          setOpen(false, { restoreFocus: false });
          return;
        }
        setOpen(true, { trigger });
      });
    });

    scopeTabs.forEach((tab) => {
      tab.addEventListener("click", () => {
        setActiveScope(tab.dataset.postGalleryPickerScopeTab || "images");
      });
    });

    overlay.addEventListener("click", () => {
      setOpen(false);
    });

    if (pinToggle instanceof HTMLButtonElement) {
      pinToggle.addEventListener("click", (event) => {
        event.preventDefault();
        pinned = !pinned;
        writeGalleryPickerPinnedState(pinned);
        syncDrawerState();
      });
    }

    if (viewToggleButton instanceof HTMLButtonElement) {
      viewToggleButton.addEventListener("click", () => {
        setActiveTab(activeTab === "grid" ? "list" : "grid");
      });
    }

    window.addEventListener("scroll", syncDrawerScrollGate, { passive: true });
    window.addEventListener("resize", syncDrawerScrollGate);

    window.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && isOpen()) {
        event.preventDefault();
        setOpen(false);
      }
    });

    setActiveTab(activeTab);
    setActiveScope(activeScope);
    if (readGalleryPickerOpenState()) {
      setOpen(true, { restoreFocus: false, focusOnOpen: false });
    } else {
      syncDrawerState();
    }

    return {
      contains(node) {
        return node instanceof Node && drawer.contains(node);
      },
      isOpen,
      isPinned,
      close(options = {}) {
        setOpen(false, options);
      },
    };
  }
  const galleryPicker = initializeGalleryPicker();

  let readerNavPinned = readReaderNavPinnedState();
  const setReaderNavExpandedState = (open) => {
    readerNavOpenButtons.forEach((trigger) => {
      trigger.setAttribute("aria-expanded", open ? "true" : "false");
    });
  };
  const syncReaderNavState = () => {
    if (!(readerNavSheet instanceof HTMLElement) || !(readerNavOverlay instanceof HTMLElement)) {
      return;
    }
    const open = readerNavSheet.classList.contains("is-open");
    const docked = open && readerNavPinned;
    readerNavSheet.classList.toggle("is-pinned", docked);
    readerNavOverlay.hidden = !(open && !docked);
    document.body.classList.toggle("is-post-nav-open", open);
    document.body.classList.toggle("is-reader-nav-open", open && !docked);
    document.body.classList.toggle("is-post-nav-pinned", docked);
    if (postViewShell instanceof HTMLElement) {
      postViewShell.classList.toggle("has-pinned-nav", docked);
    }
    if (readerNavPinButton instanceof HTMLButtonElement) {
      readerNavPinButton.setAttribute("aria-pressed", readerNavPinned ? "true" : "false");
      readerNavPinButton.setAttribute("aria-label", readerNavPinned ? "Unpin navigation" : "Pin navigation");
      readerNavPinButton.title = readerNavPinned ? "Unpin navigation" : "Pin navigation";
    }
    setReaderNavExpandedState(open);
  };
  const withReaderNavTransitionSuppressed = (callback) => {
    if (!(readerNavSheet instanceof HTMLElement)) {
      callback();
      return;
    }
    readerNavSheet.classList.add("is-restoring");
    callback();
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        readerNavSheet.classList.remove("is-restoring");
      });
    });
  };

  const setReaderNavOpen = (open, options = {}) => {
    const instant = options.instant === true;
    if (!(readerNavSheet instanceof HTMLElement) || !(readerNavOverlay instanceof HTMLElement)) {
      return;
    }
    const applyOpenState = () => {
      if (open) {
        readerNavSheet.classList.add("is-open");
        readerNavSheet.setAttribute("aria-hidden", "false");
      } else {
        readerNavSheet.classList.remove("is-open");
        readerNavSheet.setAttribute("aria-hidden", "true");
      }
    };
    if (instant) {
      withReaderNavTransitionSuppressed(applyOpenState);
    } else {
      applyOpenState();
    }
    syncReaderNavState();
    writeReaderNavState(open);
  };

  if (readReaderNavState()) {
    setReaderNavOpen(true, { instant: true });
  }
  readerNavOpenButtons.forEach((trigger) => {
    trigger.addEventListener("click", (event) => {
      event.preventDefault();
      if (!(readerNavSheet instanceof HTMLElement) || readerNavSheet.classList.contains("is-open")) {
        setReaderNavOpen(false);
        return;
      }
      setReaderNavOpen(true);
    });
  });
  if (readerNavCloseButton instanceof HTMLButtonElement) {
    readerNavCloseButton.addEventListener("click", () => {
      setReaderNavOpen(false);
    });
  }
  if (readerNavPinButton instanceof HTMLButtonElement) {
    readerNavPinButton.addEventListener("click", (event) => {
      event.preventDefault();
      readerNavPinned = !readerNavPinned;
      writeReaderNavPinnedState(readerNavPinned);
      syncReaderNavState();
    });
  }
  if (readerNavOverlay instanceof HTMLElement) {
    readerNavOverlay.addEventListener("click", () => {
      setReaderNavOpen(false);
    });
  }
  if (readerNavSheet instanceof HTMLElement) {
    readerNavSheet.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const anchor = target.closest("a");
      if (!(anchor instanceof HTMLAnchorElement)) {
        return;
      }
      const opensNewContext =
        anchor.target === "_blank" || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey;
      if (!opensNewContext) {
        writeReaderNavState(true);
      }
    });
  }
  window.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setReaderNavOpen(false);
    }
  });
  syncReaderNavState();

  const navigatorEndpoint = pageRoot instanceof HTMLElement ? pageRoot.dataset.postNavigatorUrl || "" : "";

  const getNavigatorElements = () => {
    const navScrollRoot = document.querySelector(".post-nav-list-wrap");
    const navList = document.querySelector(".post-nav-list");
    const activeNav = document.querySelector(".post-nav-link.is-active");
    const currentAboveHint = document.querySelector("[data-post-nav-current-above]");
    const currentBelowHint = document.querySelector("[data-post-nav-current-below]");
    return { navScrollRoot, navList, activeNav, currentAboveHint, currentBelowHint };
  };

  function scrollCurrentIntoView() {
    const { activeNav } = getNavigatorElements();
    if (!(activeNav instanceof HTMLElement)) {
      return;
    }
    activeNav.scrollIntoView({ block: "center", inline: "nearest" });
  }

  function updateCurrentVisibilityHints() {
    const { navScrollRoot, navList, activeNav, currentAboveHint, currentBelowHint } = getNavigatorElements();
    const scrollRoot =
      navScrollRoot instanceof HTMLElement
        ? navScrollRoot
        : navList instanceof HTMLElement
          ? navList
          : null;
    const canTrack = scrollRoot instanceof HTMLElement && activeNav instanceof HTMLElement;
    if (!canTrack) {
      if (currentAboveHint instanceof HTMLElement) {
        currentAboveHint.hidden = true;
      }
      if (currentBelowHint instanceof HTMLElement) {
        currentBelowHint.hidden = true;
      }
      return;
    }

    const hasOverflow = scrollRoot.scrollHeight > scrollRoot.clientHeight + 1;
    if (!hasOverflow) {
      if (currentAboveHint instanceof HTMLElement) {
        currentAboveHint.hidden = true;
      }
      if (currentBelowHint instanceof HTMLElement) {
        currentBelowHint.hidden = true;
      }
      return;
    }

    const listRect = scrollRoot.getBoundingClientRect();
    const activeRect = activeNav.getBoundingClientRect();
    const currentAbove = activeRect.bottom < listRect.top + 2;
    const currentBelow = activeRect.top > listRect.bottom - 2;

    if (currentAboveHint instanceof HTMLElement) {
      currentAboveHint.hidden = !currentAbove;
    }
    if (currentBelowHint instanceof HTMLElement) {
      currentBelowHint.hidden = !currentBelow;
    }
  }

  const escapeHtml = (value) =>
    String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");

  const setNavScopeOnHref = (href, scope) => {
    if (typeof href !== "string" || !href.trim()) {
      return href;
    }
    try {
      const parsed = new URL(href, window.location.href);
      parsed.searchParams.set("nav_scope", scope);
      return parsed.toString();
    } catch {
      return href;
    }
  };

  const renderNavigatorPayload = (payload) => {
    if (!payload || typeof payload !== "object") {
      return;
    }
    const scope = payload.scope === "all" ? "all" : "series";
    const title = typeof payload.title === "string" ? payload.title : "";
    const seriesScopeUrl = typeof payload.series_scope_url === "string" ? payload.series_scope_url : "";
    const allScopeUrl = typeof payload.all_scope_url === "string" ? payload.all_scope_url : "";
    const entries = Array.isArray(payload.entries) ? payload.entries : [];

    const navPanels = Array.from(document.querySelectorAll(".post-nav-panel"));
    navPanels.forEach((panel) => {
      if (!(panel instanceof HTMLElement)) {
        return;
      }
      const subtitle = panel.querySelector(".post-nav-head small");
      if (subtitle instanceof HTMLElement) {
        subtitle.textContent = title;
      }
      Array.from(panel.querySelectorAll("[data-post-nav-scope-chip]")).forEach((chipNode) => {
        if (!(chipNode instanceof HTMLAnchorElement)) {
          return;
        }
        const chipScope = chipNode.dataset.navScope === "all" ? "all" : "series";
        chipNode.classList.toggle("is-active", chipScope === scope);
        chipNode.href = chipScope === "all" ? allScopeUrl : seriesScopeUrl;
      });

      let region = panel.querySelector(".post-nav-list-region");
      const existingEmpty = panel.querySelector(".post-nav-empty");
      if (existingEmpty instanceof HTMLElement && entries.length > 0) {
        existingEmpty.remove();
      }
      if (!(region instanceof HTMLElement) && entries.length > 0) {
        region = document.createElement("div");
        region.className = "post-nav-list-region";
        region.innerHTML = [
          '<button type="button" class="post-nav-current-hint post-nav-current-hint-top" data-post-nav-current-above hidden>↑ Current entry is above</button>',
          '<div class="post-nav-list-wrap"><ul class="post-nav-list"></ul></div>',
          '<button type="button" class="post-nav-current-hint post-nav-current-hint-bottom" data-post-nav-current-below hidden>↓ Current entry is below</button>',
        ].join("");
        panel.appendChild(region);
      }

      if (entries.length === 0) {
        if (region instanceof HTMLElement) {
          region.remove();
        }
        if (!(panel.querySelector(".post-nav-empty") instanceof HTMLElement)) {
          const empty = document.createElement("p");
          empty.className = "post-nav-empty";
          empty.textContent = "No entries to show.";
          panel.appendChild(empty);
        }
        return;
      }

      if (!(region instanceof HTMLElement)) {
        return;
      }
      const list = region.querySelector(".post-nav-list");
      if (!(list instanceof HTMLElement)) {
        return;
      }
      list.innerHTML = entries
        .map((entry) => {
          const href = typeof entry.href === "string" ? entry.href : "#";
          const titleText = typeof entry.title === "string" ? entry.title : "Untitled";
          const publishedAt = typeof entry.published_at_display === "string" ? entry.published_at_display : "";
          const seriesName =
            typeof entry.series_name === "string" && entry.series_name.trim() ? entry.series_name : "Unsorted";
          const isCurrent = Boolean(entry.is_current);
          const publishedMarkup = publishedAt ? `<small>${escapeHtml(publishedAt)}</small>` : "";
          const seriesMarkup =
            scope === "all" ? `<span class="post-nav-link-series">${escapeHtml(seriesName)}</span>` : "";
          const mainMarkup = [
            '<span class="post-nav-link-main">',
            `<strong>${escapeHtml(titleText)}</strong>`,
            publishedMarkup,
            "</span>",
            seriesMarkup,
          ].join("");
          if (isCurrent) {
            return [
              "<li>",
              '<span class="post-nav-link is-active is-current" aria-current="page">',
              mainMarkup,
              "</span>",
              "</li>",
            ].join("");
          }
          return [
            "<li>",
            `<a class="post-nav-link" href="${escapeHtml(href)}">`,
            mainMarkup,
            "</a>",
            "</li>",
          ].join("");
        })
        .join("");
    });

    const modeOptionLinks = Array.from(document.querySelectorAll("[data-post-view-mode-option]"));
    modeOptionLinks.forEach((modeLink) => {
      if (!(modeLink instanceof HTMLAnchorElement)) {
        return;
      }
      modeLink.href = setNavScopeOnHref(modeLink.href, scope);
    });
    const modeSelect = document.querySelector("[data-post-view-mode-select]");
    if (modeSelect instanceof HTMLSelectElement) {
      Array.from(modeSelect.options).forEach((option) => {
        option.value = setNavScopeOnHref(option.value, scope);
      });
    }
    Array.from(document.querySelectorAll(".post-viewer-version-menu-item")).forEach((versionLink) => {
      if (!(versionLink instanceof HTMLAnchorElement)) {
        return;
      }
      versionLink.href = setNavScopeOnHref(versionLink.href, scope);
    });

    requestAnimationFrame(() => {
      scrollCurrentIntoView();
      updateCurrentVisibilityHints();
    });
  };

  const fetchAndRenderNavigatorScope = async (scopeLink) => {
    if (!(scopeLink instanceof HTMLAnchorElement) || !navigatorEndpoint) {
      return false;
    }
    const linkUrl = new URL(scopeLink.href, window.location.href);
    const query = new URLSearchParams();
    query.set("nav_scope", linkUrl.searchParams.get("nav_scope") || scopeLink.dataset.navScope || "series");
    const versionId = linkUrl.searchParams.get("version_id");
    if (versionId) {
      query.set("version_id", versionId);
    }
    const view = linkUrl.searchParams.get("view");
    if (view) {
      query.set("view", view);
    }

    const response = await fetch(`${navigatorEndpoint}?${query.toString()}`, {
      method: "GET",
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    });
    if (!response.ok) {
      throw new Error(`Navigator request failed (${response.status})`);
    }
    const payload = await response.json();
    renderNavigatorPayload(payload);
    window.history.replaceState(null, "", linkUrl.toString());
    return true;
  };

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const scopeChip = target.closest("[data-post-nav-scope-chip]");
    if (scopeChip instanceof HTMLAnchorElement) {
      if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
        return;
      }
      event.preventDefault();
      fetchAndRenderNavigatorScope(scopeChip).catch(() => {
        window.location.assign(scopeChip.href);
      });
      return;
    }

    const hintButton = target.closest("[data-post-nav-current-above], [data-post-nav-current-below]");
    if (hintButton instanceof HTMLButtonElement) {
      scrollCurrentIntoView();
      requestAnimationFrame(updateCurrentVisibilityHints);
    }
  });

  document.addEventListener(
    "scroll",
    (event) => {
      const target = event.target;
      if (
        target instanceof HTMLElement &&
        (target.classList.contains("post-nav-list-wrap") || target.classList.contains("post-nav-list"))
      ) {
        updateCurrentVisibilityHints();
      }
    },
    true
  );
  window.addEventListener("resize", updateCurrentVisibilityHints);
  requestAnimationFrame(() => {
    scrollCurrentIntoView();
    updateCurrentVisibilityHints();
  });

  function initializeReaderView() {
    if (!isImageFocusView || !(contentRoot instanceof HTMLElement)) {
      return null;
    }
    const panel = document.querySelector("[data-post-reader-panel]");
    const canvas = document.querySelector("[data-post-reader-canvas]");
    const stage = panel instanceof HTMLElement ? panel.querySelector(".post-reader-stage") : null;
    const image = document.querySelector("[data-post-reader-image]");
    const emptyState = document.querySelector("[data-post-reader-empty]");
    const caption = document.querySelector("[data-post-reader-caption]");
    const counter = document.querySelector("[data-post-reader-counter]");
    const prevButton = document.querySelector("[data-post-reader-prev]");
    const nextButton = document.querySelector("[data-post-reader-next]");
    const zoomInButton = document.querySelector("[data-post-reader-zoom-in]");
    const zoomOutButton = document.querySelector("[data-post-reader-zoom-out]");
    const zoomFitButton = document.querySelector("[data-post-reader-zoom-fit]");
    const fullscreenButton = document.querySelector("[data-post-reader-fullscreen-toggle]");
    const scrollBar = document.querySelector("[data-post-reader-scrollbar]");
    const scrollBarHorizontal = document.querySelector("[data-post-reader-scrollbar-horizontal]");
    const openTabLink = document.querySelector("[data-post-reader-open-tab]");

    if (
      !(panel instanceof HTMLElement) ||
      !(canvas instanceof HTMLElement) ||
      !(image instanceof HTMLImageElement) ||
      !(emptyState instanceof HTMLElement) ||
      !(caption instanceof HTMLElement) ||
      !(counter instanceof HTMLElement) ||
      !(prevButton instanceof HTMLButtonElement) ||
      !(nextButton instanceof HTMLButtonElement) ||
      !(zoomInButton instanceof HTMLButtonElement) ||
      !(zoomOutButton instanceof HTMLButtonElement) ||
      !(zoomFitButton instanceof HTMLButtonElement)
    ) {
      return null;
    }

    const catalog = [];
    const indexByKey = new Map();
    const imageStateStorageKey =
      pageRoot instanceof HTMLElement ? `kemono-reader-image-index:${pageRoot.dataset.postId || "unknown"}` : "";
    let currentIndex = -1;
    const MIN_ZOOM = 1;
    const DENSITY_MAX_ZOOM_MULTIPLIER = 2;
    const ZOOM_EPSILON = 0.0001;
    const BUTTON_ZOOM_STEP = 0.2;
    const WHEEL_ZOOM_PER_PIXEL = 0.0014;
    const SCROLL_MODE_PAN_PER_PIXEL = 1;
    let zoomLevel = 1;
    let panX = 0;
    let panY = 0;
    let scrollModeActive = false;
    let galleryModeState = "fit";
    let dragPointerId = null;
    let dragStartX = 0;
    let dragStartY = 0;
    let dragPanX = 0;
    let dragPanY = 0;
    let scrollActivityTimer = null;
    const fullscreenSupported =
      typeof canvas.requestFullscreen === "function" &&
      (typeof document.fullscreenEnabled !== "boolean" || document.fullscreenEnabled);

    const readSavedImageIndex = () => {
      if (!imageStateStorageKey) {
        return null;
      }
      try {
        const raw = window.sessionStorage.getItem(imageStateStorageKey);
        if (raw === null) {
          return null;
        }
        const parsed = Number(raw);
        return Number.isInteger(parsed) ? parsed : null;
      } catch {
        return null;
      }
    };

    const saveImageIndex = (index) => {
      if (!imageStateStorageKey) {
        return;
      }
      try {
        window.sessionStorage.setItem(imageStateStorageKey, String(index));
      } catch {
        // Ignore storage failures.
      }
    };

    const normalizeSource = (value) => {
      if (typeof value !== "string" || !value.trim()) {
        return "";
      }
      try {
        const parsed = new URL(value.trim(), window.location.href);
        parsed.hash = "";
        return parsed.toString();
      } catch {
        return value.trim();
      }
    };

    const getFittedSize = () => {
      const canvasWidth = canvas.clientWidth;
      const canvasHeight = canvas.clientHeight;
      const naturalWidth = image.naturalWidth;
      const naturalHeight = image.naturalHeight;
      if (canvasWidth <= 0 || canvasHeight <= 0 || naturalWidth <= 0 || naturalHeight <= 0) {
        return {
          canvasWidth: Math.max(0, canvasWidth),
          canvasHeight: Math.max(0, canvasHeight),
          naturalWidth: Math.max(0, naturalWidth),
          naturalHeight: Math.max(0, naturalHeight),
          fittedWidth: 0,
          fittedHeight: 0,
        };
      }
      const imageAspect = naturalWidth / naturalHeight;
      const canvasAspect = canvasWidth / canvasHeight;
      let fittedWidth = 0;
      let fittedHeight = 0;
      if (imageAspect >= canvasAspect) {
        fittedWidth = canvasWidth;
        fittedHeight = canvasWidth / imageAspect;
      } else {
        fittedHeight = canvasHeight;
        fittedWidth = canvasHeight * imageAspect;
      }
      return {
        canvasWidth,
        canvasHeight,
        naturalWidth,
        naturalHeight,
        fittedWidth,
        fittedHeight,
      };
    };

    const getDynamicMaxZoom = () => {
      const { naturalWidth, naturalHeight, fittedWidth, fittedHeight } = getFittedSize();
      if (naturalWidth <= 0 || naturalHeight <= 0 || fittedWidth <= 0 || fittedHeight <= 0) {
        return MIN_ZOOM;
      }
      const widthLimit = naturalWidth / fittedWidth;
      const heightLimit = naturalHeight / fittedHeight;
      const densityLimitedMax = Math.min(widthLimit, heightLimit);
      if (!Number.isFinite(densityLimitedMax) || densityLimitedMax <= 0) {
        return MIN_ZOOM;
      }
      const boostedDensityMax = densityLimitedMax * DENSITY_MAX_ZOOM_MULTIPLIER;
      return Math.max(MIN_ZOOM, boostedDensityMax);
    };

    const clampZoom = (value) => {
      const maxZoom = getDynamicMaxZoom();
      return Math.max(MIN_ZOOM, Math.min(maxZoom, value));
    };

    const getPanBounds = () => {
      const { canvasWidth, canvasHeight, fittedWidth, fittedHeight } = getFittedSize();
      if (canvasWidth <= 0 || canvasHeight <= 0 || fittedWidth <= 0 || fittedHeight <= 0) {
        return { maxX: 0, maxY: 0 };
      }
      const scaledWidth = fittedWidth * zoomLevel;
      const scaledHeight = fittedHeight * zoomLevel;
      return {
        maxX: Math.max(0, (scaledWidth - canvasWidth) / 2),
        maxY: Math.max(0, (scaledHeight - canvasHeight) / 2),
      };
    };

    const computeCoverZoom = () => {
      const canvasWidth = canvas.clientWidth;
      const canvasHeight = canvas.clientHeight;
      const naturalWidth = image.naturalWidth;
      const naturalHeight = image.naturalHeight;
      if (canvasWidth <= 0 || canvasHeight <= 0 || naturalWidth <= 0 || naturalHeight <= 0) {
        return MIN_ZOOM;
      }
      const imageAspect = naturalWidth / naturalHeight;
      const canvasAspect = canvasWidth / canvasHeight;
      if (!Number.isFinite(imageAspect) || !Number.isFinite(canvasAspect) || imageAspect <= 0 || canvasAspect <= 0) {
        return MIN_ZOOM;
      }
      const coverZoom = imageAspect >= canvasAspect ? imageAspect / canvasAspect : canvasAspect / imageAspect;
      return clampZoom(Math.max(MIN_ZOOM, coverZoom));
    };

    const canPanHorizontallyAtCoverZoom = () => {
      const { canvasWidth, fittedWidth } = getFittedSize();
      if (canvasWidth <= 0 || fittedWidth <= 0) {
        return false;
      }
      const coverZoom = computeCoverZoom();
      const scaledWidth = fittedWidth * coverZoom;
      return scaledWidth - canvasWidth > ZOOM_EPSILON;
    };

    const clampPan = () => {
      const { maxX, maxY } = getPanBounds();
      if (panX > maxX) {
        panX = maxX;
      } else if (panX < -maxX) {
        panX = -maxX;
      }
      if (panY > maxY) {
        panY = maxY;
      } else if (panY < -maxY) {
        panY = -maxY;
      }
      return { maxX, maxY };
    };

    const isDefaultView = () =>
      Math.abs(zoomLevel - MIN_ZOOM) <= ZOOM_EPSILON &&
      Math.abs(panX) <= ZOOM_EPSILON &&
      Math.abs(panY) <= ZOOM_EPSILON;

    const updateScrollBars = () => {
      const hasImages = catalog.length > 0 && currentIndex >= 0;
      const { maxX, maxY } = getPanBounds();
      let verticalScrollVisible = false;

      if (scrollBar instanceof HTMLInputElement) {
        const usableMaxY = Math.max(0, maxY);
        const canvasHeight = Math.max(1, canvas.clientHeight);
        const totalHeight = canvasHeight + 2 * usableMaxY;
        const viewportRatio = Math.max(0, Math.min(1, canvasHeight / totalHeight));
        const railHeight = Math.max(1, scrollBar.clientHeight || canvasHeight);
        const thumbPx = Math.round(Math.max(18, Math.min(railHeight * 0.92, railHeight * viewportRatio)));
        scrollBar.style.setProperty("--post-reader-scroll-thumb-size", `${thumbPx}px`);
        const activeY = hasImages && scrollModeActive && usableMaxY > ZOOM_EPSILON;
        verticalScrollVisible = activeY;
        scrollBar.hidden = !activeY;
        scrollBar.disabled = !activeY;
        if (!activeY) {
          scrollBar.value = "0";
        } else {
          const progressY = (panY + usableMaxY) / (2 * usableMaxY);
          const clampedY = Math.max(0, Math.min(1, progressY));
          scrollBar.value = String(Math.round(clampedY * 100));
        }
      }

      if (stage instanceof HTMLElement) {
        stage.classList.toggle("has-vertical-scrollbar", verticalScrollVisible);
      }

      if (scrollBarHorizontal instanceof HTMLInputElement) {
        const usableMaxX = Math.max(0, maxX);
        const canvasWidth = Math.max(1, canvas.clientWidth);
        const totalWidth = canvasWidth + 2 * usableMaxX;
        const viewportRatioX = Math.max(0, Math.min(1, canvasWidth / totalWidth));
        const railWidth = Math.max(1, scrollBarHorizontal.clientWidth || canvasWidth);
        const thumbPxX = Math.round(Math.max(18, Math.min(railWidth * 0.92, railWidth * viewportRatioX)));
        scrollBarHorizontal.style.setProperty("--post-reader-scroll-thumb-size-x", `${thumbPxX}px`);
        const activeX = hasImages && scrollModeActive && usableMaxX > ZOOM_EPSILON;
        scrollBarHorizontal.hidden = !activeX;
        scrollBarHorizontal.disabled = !activeX;
        if (!activeX) {
          scrollBarHorizontal.value = "0";
        } else {
          const progressX = (usableMaxX - panX) / (2 * usableMaxX);
          const clampedX = Math.max(0, Math.min(1, progressX));
          scrollBarHorizontal.value = String(Math.round(clampedX * 100));
        }
      }
    };

    const markScrollActivity = () => {
      if (!(stage instanceof HTMLElement)) {
        return;
      }
      stage.classList.add("is-scroll-active");
      if (scrollActivityTimer !== null) {
        window.clearTimeout(scrollActivityTimer);
      }
      scrollActivityTimer = window.setTimeout(() => {
        stage.classList.remove("is-scroll-active");
        scrollActivityTimer = null;
      }, 900);
    };

    const setTransforms = () => {
      zoomLevel = clampZoom(zoomLevel);
      const { maxX, maxY } = clampPan();
      image.style.transform = `translate(${panX}px, ${panY}px) scale(${zoomLevel})`;
      if (scrollModeActive) {
        image.style.cursor = maxY > ZOOM_EPSILON || maxX > ZOOM_EPSILON ? "grab" : "default";
      } else {
        image.style.cursor = maxX > ZOOM_EPSILON || maxY > ZOOM_EPSILON ? "grab" : "default";
      }
      updateScrollBars();
    };

    const fitView = () => {
      scrollModeActive = false;
      zoomLevel = 1;
      panX = 0;
      panY = 0;
      if (stage instanceof HTMLElement) {
        stage.classList.remove("is-column-width");
      }
      setTransforms();
      updateButtons();
    };

    const enterColumnMode = () => {
      if (!(stage instanceof HTMLElement)) {
        fitView();
        return;
      }
      stage.classList.add("is-column-width");
      scrollModeActive = true;
      zoomLevel = computeCoverZoom();
      panX = 0;
      panY = 0;
      setTransforms();
      const { maxX, maxY } = getPanBounds();
      panX = maxX;
      panY = maxY;
      setTransforms();
      markScrollActivity();
      updateButtons();
    };

    const enterScrollMode = () => {
      if (stage instanceof HTMLElement) {
        stage.classList.remove("is-column-width");
      }
      scrollModeActive = true;
      zoomLevel = computeCoverZoom();
      panX = 0;
      panY = 0;
      setTransforms();
      const { maxX, maxY } = getPanBounds();
      panX = maxX;
      panY = maxY;
      setTransforms();
      markScrollActivity();
      updateButtons();
    };

    const toggleFitMode = () => {
      if (isGalleryView) {
        if (galleryModeState === "fit") {
          if (!isDefaultView()) {
            fitView();
            return;
          }
          if (canPanHorizontallyAtCoverZoom()) {
            galleryModeState = "pan";
            enterScrollMode();
            return;
          }
          galleryModeState = "column";
          enterColumnMode();
          return;
        }
        if (galleryModeState === "column") {
          galleryModeState = "pan";
          enterScrollMode();
          return;
        }
        galleryModeState = "fit";
        fitView();
        return;
      }
      if (isDefaultView()) {
        enterScrollMode();
        return;
      }
      fitView();
    };

    const getCanvasCenterClient = () => {
      const rect = canvas.getBoundingClientRect();
      return {
        x: rect.left + rect.width / 2,
        y: rect.top + rect.height / 2,
      };
    };

    const setZoomAroundPoint = (nextZoom, clientX, clientY) => {
      const clamped = clampZoom(nextZoom);
      if (Math.abs(clamped - zoomLevel) <= ZOOM_EPSILON) {
        return;
      }
      scrollModeActive = false;
      const beforeRect = image.getBoundingClientRect();
      const canAnchor = beforeRect.width > 0 && beforeRect.height > 0;
      let relX = 0.5;
      let relY = 0.5;
      if (canAnchor) {
        relX = (clientX - beforeRect.left) / beforeRect.width;
        relY = (clientY - beforeRect.top) / beforeRect.height;
      }

      zoomLevel = clamped;
      if (zoomLevel <= MIN_ZOOM + ZOOM_EPSILON) {
        panX = 0;
        panY = 0;
      }
      setTransforms();

      if (canAnchor && zoomLevel > MIN_ZOOM + ZOOM_EPSILON) {
        const afterRect = image.getBoundingClientRect();
        const targetLeft = clientX - relX * afterRect.width;
        const targetTop = clientY - relY * afterRect.height;
        panX += targetLeft - afterRect.left;
        panY += targetTop - afterRect.top;
        setTransforms();
      }
      updateButtons();
    };

    const zoomBy = (step) => {
      const next = clampZoom(zoomLevel + step);
      const center = getCanvasCenterClient();
      setZoomAroundPoint(next, center.x, center.y);
    };

    const normalizedWheelDeltaPixels = (event) => {
      let delta = event.deltaY;
      if (event.deltaMode === 1) {
        // DOM_DELTA_LINE
        delta *= 16;
      } else if (event.deltaMode === 2) {
        // DOM_DELTA_PAGE
        delta *= window.innerHeight || 800;
      }
      return delta;
    };

    const isCanvasFullscreen = () => document.fullscreenElement === canvas;

    const syncFullscreenButton = () => {
      if (!(fullscreenButton instanceof HTMLButtonElement)) {
        return;
      }
      if (!fullscreenSupported) {
        fullscreenButton.hidden = true;
        fullscreenButton.disabled = true;
        return;
      }
      const active = isCanvasFullscreen();
      fullscreenButton.hidden = false;
      fullscreenButton.disabled = false;
      fullscreenButton.classList.toggle("is-active", active);
      fullscreenButton.setAttribute("aria-pressed", active ? "true" : "false");
      fullscreenButton.setAttribute("aria-label", active ? "Exit fullscreen" : "Enter fullscreen");
      fullscreenButton.title = active ? "Exit fullscreen" : "Enter fullscreen";
    };

    const toggleCanvasFullscreen = async () => {
      if (!fullscreenSupported) {
        return;
      }
      try {
        if (isCanvasFullscreen()) {
          if (typeof document.exitFullscreen === "function") {
            await document.exitFullscreen();
          }
          return;
        }
        await canvas.requestFullscreen();
      } catch {
        // Ignore fullscreen errors (for example if denied by browser settings).
      }
    };

    const updateButtons = () => {
      const hasImages = catalog.length > 0 && currentIndex >= 0;
      const showNavigation = hasImages && catalog.length > 1;
      const maxZoom = getDynamicMaxZoom();
      const canZoomIn = hasImages && zoomLevel < maxZoom - ZOOM_EPSILON;
      const canZoomOut = hasImages && zoomLevel > MIN_ZOOM + ZOOM_EPSILON;
      prevButton.hidden = !showNavigation;
      nextButton.hidden = !showNavigation;
      prevButton.disabled = !hasImages || currentIndex <= 0;
      nextButton.disabled = !hasImages || currentIndex >= catalog.length - 1;
      zoomInButton.disabled = !canZoomIn;
      zoomOutButton.disabled = !canZoomOut;
      zoomFitButton.disabled = !hasImages;
      if (isGalleryView) {
        if (!hasImages) {
          zoomFitButton.textContent = "Column";
        } else if (galleryModeState === "fit") {
          if (!isDefaultView()) {
            zoomFitButton.textContent = "Fit";
          } else {
            zoomFitButton.textContent = canPanHorizontallyAtCoverZoom() ? "Scroll" : "Column";
          }
        } else if (galleryModeState === "column") {
          zoomFitButton.textContent = "Pan";
        } else {
          zoomFitButton.textContent = "Fit";
        }
      } else {
        zoomFitButton.textContent = hasImages && isDefaultView() ? "Scroll" : "Fit";
      }
    };

    const captureViewerModeState = () => {
      if (isGalleryView) {
        if (galleryModeState === "column") {
          return "column";
        }
        if (galleryModeState === "pan") {
          return "pan";
        }
        return "fit";
      }
      return scrollModeActive ? "scroll" : "fit";
    };

    const applyViewerModeState = (modeState) => {
      if (isGalleryView) {
        if (modeState === "column") {
          galleryModeState = "column";
          enterColumnMode();
          return;
        }
        if (modeState === "pan") {
          galleryModeState = "pan";
          enterScrollMode();
          return;
        }
        galleryModeState = "fit";
        fitView();
        return;
      }
      if (modeState === "scroll") {
        enterScrollMode();
        return;
      }
      fitView();
    };

    const applyViewerModeWhenImageReady = (modeState, expectedSrc) => {
      const ready = image.complete && image.naturalWidth > 0 && image.naturalHeight > 0;
      if (ready) {
        applyViewerModeState(modeState);
        return;
      }
      const handleReady = () => {
        const currentSrc = normalizeSource(image.currentSrc || image.src || "");
        if (currentSrc !== expectedSrc) {
          return;
        }
        applyViewerModeState(modeState);
      };
      image.addEventListener("load", handleReady, { once: true });
      image.addEventListener("error", handleReady, { once: true });
    };

    const renderActive = (options = {}) => {
      const opts = options && typeof options === "object" ? options : {};
      const modeState = typeof opts.modeState === "string" ? opts.modeState : "fit";
      const hasImages = catalog.length > 0 && currentIndex >= 0;
      if (!hasImages) {
        image.hidden = true;
        image.removeAttribute("src");
        image.alt = "";
        emptyState.hidden = false;
        caption.textContent = "Select an image from the text or launcher.";
        counter.textContent = "0 / 0";
        if (openTabLink instanceof HTMLAnchorElement) {
          openTabLink.hidden = true;
          openTabLink.removeAttribute("href");
        }
        updateButtons();
        return;
      }
      const item = catalog[currentIndex];
      image.src = item.src;
      image.alt = item.title || `Image ${currentIndex + 1}`;
      image.hidden = false;
      emptyState.hidden = true;
      const originLabel = item.origin === "inline" ? "inline" : "saved";
      caption.textContent = `${item.title || `Image ${currentIndex + 1}`} (${originLabel})`;
      counter.textContent = `${currentIndex + 1} / ${catalog.length}`;
      if (openTabLink instanceof HTMLAnchorElement) {
        openTabLink.href = item.src;
        openTabLink.hidden = false;
      }
      const expectedSrc = normalizeSource(item.src);
      applyViewerModeWhenImageReady(modeState, expectedSrc);
    };

    const selectImage = (index, options = {}) => {
      const preserveScroll = Boolean(options && typeof options === "object" && "preserveScroll" in options && options.preserveScroll);
      const preserveMode =
        !(options && typeof options === "object" && "preserveModeState" in options) ||
        Boolean(options && typeof options === "object" && options.preserveModeState);
      if (!Number.isInteger(index) || index < 0 || index >= catalog.length) {
        return;
      }
      const prevScrollX = preserveScroll ? window.scrollX : 0;
      const prevScrollY = preserveScroll ? window.scrollY : 0;
      const nextModeState = preserveMode ? captureViewerModeState() : "fit";
      currentIndex = index;
      renderActive({ modeState: nextModeState });
      saveImageIndex(index);
      if (preserveScroll) {
        window.requestAnimationFrame(() => {
          if (window.scrollX !== prevScrollX || window.scrollY !== prevScrollY) {
            window.scrollTo(prevScrollX, prevScrollY);
          }
        });
      }
    };

    const registerImage = (src, title, origin) => {
      const normalized = normalizeSource(src);
      if (!normalized) {
        return null;
      }
      const existing = indexByKey.get(normalized);
      if (typeof existing === "number") {
        return existing;
      }
      const nextIndex = catalog.length;
      catalog.push({
        src: normalized,
        title: typeof title === "string" ? title.trim() : "",
        origin,
      });
      indexByKey.set(normalized, nextIndex);
      return nextIndex;
    };

    const attachIndexToTrigger = (node, index, src, title) => {
      if (!(node instanceof HTMLElement) || typeof index !== "number") {
        return;
      }
      node.dataset.readerImageIndex = String(index);
      node.dataset.readerImageSrc = src;
      node.dataset.readerImageTitle = title;
    };

    let inlineCounter = 0;
    const inlineImages = Array.from(contentRoot.querySelectorAll("img"));
    inlineImages.forEach((inlineImage) => {
      if (!(inlineImage instanceof HTMLImageElement)) {
        return;
      }
      const src = inlineImage.getAttribute("src") || "";
      if (!src.trim()) {
        return;
      }
      inlineCounter += 1;
      const label = inlineImage.getAttribute("alt") || inlineImage.getAttribute("title") || `Inline image ${inlineCounter}`;
      const index = registerImage(src, label, "inline");
      if (index === null) {
        return;
      }

      const button = document.createElement("button");
      button.type = "button";
      button.className = "post-reader-inline-trigger";
      button.textContent = `Open image: ${label}`;
      button.setAttribute("data-post-reader-inline-trigger", "");
      attachIndexToTrigger(button, index, src, label);

      const parentLink = inlineImage.closest("a.post-image-link");
      if (
        parentLink instanceof HTMLAnchorElement &&
        parentLink.querySelector("img") === inlineImage &&
        !(parentLink.textContent || "").trim()
      ) {
        parentLink.replaceWith(button);
      } else if (parentLink instanceof HTMLAnchorElement && parentLink.children.length === 1) {
        parentLink.replaceWith(button);
      } else {
        inlineImage.replaceWith(button);
      }
    });

    const savedImageSources = Array.from(
      document.querySelectorAll("[data-post-reader-source-image], [data-post-file-image-trigger]")
    );
    savedImageSources.forEach((sourceNode) => {
      if (!(sourceNode instanceof HTMLElement)) {
        return;
      }
      const src = sourceNode.dataset.readerImageSrc || sourceNode.dataset.lightboxSrc || sourceNode.getAttribute("href") || "";
      if (!src.trim()) {
        return;
      }
      const title = sourceNode.dataset.readerImageTitle || sourceNode.dataset.lightboxTitle || sourceNode.getAttribute("title") || "Saved image";
      const index = registerImage(src, title, "saved");
      if (index === null) {
        return;
      }
      attachIndexToTrigger(sourceNode, index, src, title);
    });

    document.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const trigger = target.closest("[data-post-reader-inline-trigger], [data-post-reader-source-image]");
      if (!(trigger instanceof HTMLElement)) {
        return;
      }
      const index = Number(trigger.dataset.readerImageIndex || "");
      if (!Number.isFinite(index)) {
        return;
      }
      event.preventDefault();
      selectImage(index, { preserveScroll: true });
    });

    prevButton.addEventListener("click", () => {
      if (currentIndex > 0) {
        selectImage(currentIndex - 1, { preserveScroll: true });
      }
    });
    nextButton.addEventListener("click", () => {
      if (currentIndex >= 0 && currentIndex < catalog.length - 1) {
        selectImage(currentIndex + 1, { preserveScroll: true });
      }
    });
    zoomInButton.addEventListener("click", () => zoomBy(BUTTON_ZOOM_STEP));
    zoomOutButton.addEventListener("click", () => zoomBy(-BUTTON_ZOOM_STEP));
    zoomFitButton.addEventListener("click", toggleFitMode);
    if (fullscreenButton instanceof HTMLButtonElement) {
      fullscreenButton.addEventListener("click", (event) => {
        event.preventDefault();
        void toggleCanvasFullscreen();
      });
    }
    document.addEventListener("fullscreenchange", () => {
      syncFullscreenButton();
      if (catalog.length === 0 || currentIndex < 0) {
        return;
      }
      setTransforms();
      updateButtons();
    });
    if (scrollBar instanceof HTMLInputElement) {
      scrollBar.addEventListener("input", () => {
        if (!scrollModeActive) {
          return;
        }
        const { maxY } = getPanBounds();
        if (maxY <= ZOOM_EPSILON) {
          return;
        }
        const ratio = Math.max(0, Math.min(1, Number.parseFloat(scrollBar.value) / 100));
        panY = -maxY + ratio * (2 * maxY);
        setTransforms();
        markScrollActivity();
      });
    }
    if (scrollBarHorizontal instanceof HTMLInputElement) {
      scrollBarHorizontal.addEventListener("input", () => {
        if (!scrollModeActive) {
          return;
        }
        const { maxX } = getPanBounds();
        if (maxX <= ZOOM_EPSILON) {
          return;
        }
        const ratio = Math.max(0, Math.min(1, Number.parseFloat(scrollBarHorizontal.value) / 100));
        panX = maxX - ratio * (2 * maxX);
        setTransforms();
        markScrollActivity();
      });
    }
    canvas.addEventListener(
      "wheel",
      (event) => {
        if (catalog.length === 0) {
          return;
        }
        const allowWheelControl = event.ctrlKey;
        if (isGalleryView && !isPageAtBottom() && !allowWheelControl) {
          return;
        }
        const deltaPixels = normalizedWheelDeltaPixels(event);
        if (scrollModeActive) {
          const { maxX, maxY } = getPanBounds();
          if (maxY <= ZOOM_EPSILON && maxX <= ZOOM_EPSILON) {
            return;
          }
          event.preventDefault();
          const horizontalIntent = (event.shiftKey || Math.abs(event.deltaX) > Math.abs(event.deltaY)) && maxX > ZOOM_EPSILON;
          const horizontalOnly = maxY <= ZOOM_EPSILON && maxX > ZOOM_EPSILON;
          if (horizontalIntent || horizontalOnly) {
            const deltaX = Math.abs(event.deltaX) > ZOOM_EPSILON ? event.deltaX : event.deltaY;
            panX -= deltaX * SCROLL_MODE_PAN_PER_PIXEL;
          } else if (maxY > ZOOM_EPSILON) {
            panY -= deltaPixels * SCROLL_MODE_PAN_PER_PIXEL;
          }
          setTransforms();
          markScrollActivity();
          updateButtons();
          return;
        }
        event.preventDefault();
        const factor = Math.exp(-deltaPixels * WHEEL_ZOOM_PER_PIXEL);
        const next = zoomLevel * factor;
        setZoomAroundPoint(next, event.clientX, event.clientY);
      },
      { passive: false }
    );

    canvas.addEventListener("pointerdown", (event) => {
      if (catalog.length === 0) {
        return;
      }
      const { maxX, maxY } = getPanBounds();
      if (maxX <= ZOOM_EPSILON && maxY <= ZOOM_EPSILON) {
        return;
      }
      dragPointerId = event.pointerId;
      dragStartX = event.clientX;
      dragStartY = event.clientY;
      dragPanX = panX;
      dragPanY = panY;
      canvas.classList.add("is-dragging");
      canvas.setPointerCapture(event.pointerId);
    });

    canvas.addEventListener("pointermove", (event) => {
      if (dragPointerId === null || event.pointerId !== dragPointerId) {
        return;
      }
      panX = dragPanX + (event.clientX - dragStartX);
      panY = dragPanY + (event.clientY - dragStartY);
      setTransforms();
      markScrollActivity();
    });

    const endDrag = (event) => {
      if (dragPointerId === null || event.pointerId !== dragPointerId) {
        return;
      }
      dragPointerId = null;
      canvas.classList.remove("is-dragging");
      if (canvas.hasPointerCapture(event.pointerId)) {
        canvas.releasePointerCapture(event.pointerId);
      }
    };
    canvas.addEventListener("pointerup", endDrag);
    canvas.addEventListener("pointercancel", endDrag);
    window.addEventListener("resize", () => {
      if (catalog.length === 0 || currentIndex < 0) {
        return;
      }
      setTransforms();
      updateButtons();
    });

    window.addEventListener("keydown", (event) => {
      const target = event.target;
      if (
        target instanceof HTMLInputElement ||
        target instanceof HTMLTextAreaElement ||
        target instanceof HTMLSelectElement ||
        target instanceof HTMLButtonElement
      ) {
        return;
      }
      if (event.key === "ArrowLeft") {
        if (currentIndex > 0) {
          event.preventDefault();
          selectImage(currentIndex - 1, { preserveScroll: true });
        }
      } else if (event.key === "ArrowRight") {
        if (currentIndex >= 0 && currentIndex < catalog.length - 1) {
          event.preventDefault();
          selectImage(currentIndex + 1, { preserveScroll: true });
        }
      } else if (event.key === "+" || event.key === "=") {
        event.preventDefault();
        zoomBy(BUTTON_ZOOM_STEP);
      } else if (event.key === "-") {
        event.preventDefault();
        zoomBy(-BUTTON_ZOOM_STEP);
      } else if (event.key === "0") {
        event.preventDefault();
        fitView();
      }
    });

    if (catalog.length > 0) {
      const savedIndex = readSavedImageIndex();
      if (savedIndex !== null && savedIndex >= 0 && savedIndex < catalog.length) {
        selectImage(savedIndex, { preserveModeState: false });
      } else {
        selectImage(0, { preserveModeState: false });
      }
    } else {
      renderActive();
    }
    syncFullscreenButton();

    return {
      openByTrigger(trigger) {
        if (!(trigger instanceof HTMLElement)) {
          return false;
        }
        const index = Number(trigger.dataset.readerImageIndex || "");
        if (!Number.isFinite(index)) {
          return false;
        }
        selectImage(index, { preserveScroll: true });
        return true;
      },
    };
  }

  const readerView = initializeReaderView();
  const middleEllipsisTargets = Array.from(document.querySelectorAll("[data-middle-ellipsis]"));
  const ellipsisMeasureCanvas = document.createElement("canvas");
  const ellipsisMeasureContext = ellipsisMeasureCanvas.getContext("2d");

  function measureTextWidth(element, value) {
    if (!(element instanceof HTMLElement) || !ellipsisMeasureContext) {
      return value.length * 8;
    }
    const style = window.getComputedStyle(element);
    const font = style.font && style.font !== "normal normal normal normal 16px / normal sans-serif"
      ? style.font
      : `${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
    ellipsisMeasureContext.font = font;
    return ellipsisMeasureContext.measureText(value).width;
  }

  function isInsideClosedDetails(element) {
    return element.closest("details:not([open])") instanceof HTMLDetailsElement;
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
    if (isInsideClosedDetails(element)) {
      return;
    }
    const fullText = (element.dataset.fullText || element.textContent || "").trim();
    if (!fullText) {
      return;
    }
    element.dataset.fullText = fullText;
    element.setAttribute("title", fullText);
    element.textContent = middleEllipsizeToFit(element, fullText);
  }

  function applyMiddleEllipsisAll() {
    if (!middleEllipsisTargets.length) {
      return;
    }
    middleEllipsisTargets.forEach((target) => {
      applyMiddleEllipsis(target);
    });
  }

  applyMiddleEllipsisAll();
  let middleEllipsisResizeTimer = null;
  window.addEventListener("resize", () => {
    if (middleEllipsisResizeTimer !== null) {
      window.clearTimeout(middleEllipsisResizeTimer);
    }
    middleEllipsisResizeTimer = window.setTimeout(() => {
      applyMiddleEllipsisAll();
      middleEllipsisResizeTimer = null;
    }, 80);
  });
  document.addEventListener(
    "toggle",
    (event) => {
      if (!(event.target instanceof HTMLDetailsElement)) {
        return;
      }
      window.requestAnimationFrame(() => {
        applyMiddleEllipsisAll();
      });
    },
    true
  );

  const lightbox = document.querySelector("[data-post-lightbox]");
  if (!(lightbox instanceof HTMLElement)) {
    return;
  }

  const dialog = lightbox.querySelector(".post-lightbox-dialog");
  const imageTarget = lightbox.querySelector("[data-post-lightbox-image]");
  const captionTarget = lightbox.querySelector("[data-post-lightbox-caption]");
  const openTabLink = lightbox.querySelector("[data-post-lightbox-open-tab]");
  const closeButtons = Array.from(lightbox.querySelectorAll("[data-post-lightbox-close]"));
  if (!(imageTarget instanceof HTMLImageElement)) {
    return;
  }

  function syncLightboxCaptionWidth() {
    if (!(captionTarget instanceof HTMLElement)) {
      return;
    }
    const imageWidth = Math.floor(imageTarget.getBoundingClientRect().width);
    if (imageWidth > 0) {
      captionTarget.style.width = `${imageWidth}px`;
      captionTarget.style.maxWidth = `${imageWidth}px`;
      return;
    }
    captionTarget.style.removeProperty("width");
    captionTarget.style.removeProperty("max-width");
  }

  function openLightbox(src, caption) {
    if (typeof src !== "string" || !src.trim()) {
      return;
    }
    const cleanSrc = src.trim();
    const cleanCaption = typeof caption === "string" ? caption.trim() : "";

    imageTarget.src = cleanSrc;
    imageTarget.alt = cleanCaption || "Image preview";
    if (captionTarget instanceof HTMLElement) {
      captionTarget.textContent = cleanCaption;
      captionTarget.dataset.fullText = cleanCaption;
    }
    if (openTabLink instanceof HTMLAnchorElement) {
      openTabLink.href = cleanSrc;
    }

    lightbox.hidden = false;
    document.body.classList.add("is-lightbox-open");
    if (captionTarget instanceof HTMLElement) {
      window.requestAnimationFrame(() => {
        syncLightboxCaptionWidth();
        applyMiddleEllipsis(captionTarget);
      });
    }
  }

  function closeLightbox() {
    lightbox.hidden = true;
    imageTarget.removeAttribute("src");
    imageTarget.alt = "";
    if (captionTarget instanceof HTMLElement) {
      captionTarget.textContent = "";
      delete captionTarget.dataset.fullText;
      captionTarget.removeAttribute("title");
      captionTarget.style.removeProperty("width");
      captionTarget.style.removeProperty("max-width");
    }
    if (openTabLink instanceof HTMLAnchorElement) {
      openTabLink.removeAttribute("href");
    }
    document.body.classList.remove("is-lightbox-open");
  }

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const trigger = target.closest("[data-post-file-image-trigger]");
    if (!(trigger instanceof HTMLElement)) {
      return;
    }
    if (isImageFocusView && readerView && readerView.openByTrigger(trigger)) {
      event.preventDefault();
      if (
        isGalleryView &&
        galleryPicker &&
        galleryPicker.isOpen() &&
        !galleryPicker.isPinned() &&
        galleryPicker.contains(trigger)
      ) {
        galleryPicker.close({ restoreFocus: false });
      }
      return;
    }
    event.preventDefault();
    openLightbox(trigger.dataset.lightboxSrc || "", trigger.dataset.lightboxTitle || "");
  });

  if (contentRoot instanceof HTMLElement && !isImageFocusView) {
    contentRoot.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      const anchor = target.closest("a.post-image-link");
      if (anchor instanceof HTMLAnchorElement) {
        const nestedImage = anchor.querySelector("img");
        const src = anchor.getAttribute("href") || nestedImage?.getAttribute("src") || "";
        const caption = nestedImage?.getAttribute("alt") || "";
        if (!src) {
          return;
        }
        event.preventDefault();
        openLightbox(src, caption);
        return;
      }

      const image = target.closest("img");
      if (!(image instanceof HTMLImageElement) || !contentRoot.contains(image)) {
        return;
      }
      const src = image.getAttribute("src") || "";
      if (!src) {
        return;
      }
      event.preventDefault();
      openLightbox(src, image.getAttribute("alt") || "");
    });
  }

  closeButtons.forEach((button) => {
    button.addEventListener("click", closeLightbox);
  });

  lightbox.addEventListener("click", (event) => {
    if (lightbox.hidden) {
      return;
    }
    const target = event.target;
    if (!(target instanceof Node)) {
      return;
    }
    if (dialog instanceof HTMLElement && !dialog.contains(target)) {
      closeLightbox();
    }
  });

  window.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !lightbox.hidden) {
      closeLightbox();
    }
  });

  imageTarget.addEventListener("load", () => {
    if (lightbox.hidden) {
      return;
    }
    syncLightboxCaptionWidth();
    if (captionTarget instanceof HTMLElement) {
      applyMiddleEllipsis(captionTarget);
    }
  });

  window.addEventListener("resize", () => {
    if (lightbox.hidden) {
      return;
    }
    syncLightboxCaptionWidth();
    if (captionTarget instanceof HTMLElement) {
      applyMiddleEllipsis(captionTarget);
    }
  });
})();
