(() => {
  const syncSiteHeaderHeight = () => {
    const header = document.querySelector(".site-header");
    if (!(header instanceof HTMLElement)) {
      return;
    }
    const height = Math.max(0, Math.round(header.getBoundingClientRect().height));
    document.documentElement.style.setProperty("--site-header-height", `${height}px`);
  };
  syncSiteHeaderHeight();
  window.addEventListener("resize", syncSiteHeaderHeight);
  window.requestAnimationFrame(syncSiteHeaderHeight);

  const modeSwitcher = document.querySelector("[data-post-view-mode-switcher]");
  if (modeSwitcher instanceof HTMLElement) {
    const creatorId = modeSwitcher.dataset.creatorId || "";
    const currentMode = modeSwitcher.dataset.currentMode || "classic";
    const viewParamPresent = modeSwitcher.dataset.viewParamPresent === "1";
    const storageKey = creatorId ? `kemono-post-view-mode:creator:${creatorId}` : "";

    const isKnownMode = (value) => value === "classic" || value === "reader";
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
  const isReaderView = pageRoot instanceof HTMLElement && pageRoot.dataset.postViewMode === "reader";
  const contentRoot = document.querySelector("[data-post-content]");
  const readerNavOpenButton = document.querySelector("[data-post-reader-nav-open]");
  const readerNavCloseButton = document.querySelector("[data-post-reader-nav-close]");
  const readerNavOverlay = document.querySelector("[data-post-reader-nav-overlay]");
  const readerNavSheet = document.querySelector("[data-post-reader-nav-sheet]");
  const readerNavStateKey = "kemono-reader-nav-open";
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

  const setReaderNavOpen = (open) => {
    if (!(readerNavSheet instanceof HTMLElement) || !(readerNavOverlay instanceof HTMLElement)) {
      return;
    }
    if (open) {
      readerNavSheet.classList.add("is-open");
      readerNavSheet.setAttribute("aria-hidden", "false");
      readerNavOverlay.hidden = false;
      document.body.classList.add("is-reader-nav-open");
    } else {
      readerNavSheet.classList.remove("is-open");
      readerNavSheet.setAttribute("aria-hidden", "true");
      readerNavOverlay.hidden = true;
      document.body.classList.remove("is-reader-nav-open");
    }
    writeReaderNavState(open);
  };

  if (isReaderView) {
    if (readReaderNavState()) {
      setReaderNavOpen(true);
    }
    if (readerNavOpenButton instanceof HTMLButtonElement) {
      readerNavOpenButton.addEventListener("click", () => {
        setReaderNavOpen(true);
      });
    }
    if (readerNavCloseButton instanceof HTMLButtonElement) {
      readerNavCloseButton.addEventListener("click", () => {
        setReaderNavOpen(false);
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
  }

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
          return [
            "<li>",
            `<a class="post-nav-link${isCurrent ? " is-active" : ""}" href="${escapeHtml(href)}">`,
            '<span class="post-nav-link-main">',
            `<strong>${escapeHtml(titleText)}</strong>`,
            publishedMarkup,
            "</span>",
            seriesMarkup,
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
    if (!isReaderView || !(contentRoot instanceof HTMLElement)) {
      return null;
    }
    const panel = document.querySelector("[data-post-reader-panel]");
    const canvas = document.querySelector("[data-post-reader-canvas]");
    const image = document.querySelector("[data-post-reader-image]");
    const emptyState = document.querySelector("[data-post-reader-empty]");
    const caption = document.querySelector("[data-post-reader-caption]");
    const counter = document.querySelector("[data-post-reader-counter]");
    const prevButton = document.querySelector("[data-post-reader-prev]");
    const nextButton = document.querySelector("[data-post-reader-next]");
    const zoomInButton = document.querySelector("[data-post-reader-zoom-in]");
    const zoomOutButton = document.querySelector("[data-post-reader-zoom-out]");
    const zoomFitButton = document.querySelector("[data-post-reader-zoom-fit]");
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
    const MAX_ZOOM = 8;
    const ZOOM_EPSILON = 0.0001;
    const BUTTON_ZOOM_STEP = 0.2;
    const WHEEL_ZOOM_PER_PIXEL = 0.0014;
    let zoomLevel = 1;
    let panX = 0;
    let panY = 0;
    let dragPointerId = null;
    let dragStartX = 0;
    let dragStartY = 0;
    let dragPanX = 0;
    let dragPanY = 0;

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

    const getPanBounds = () => {
      const canvasWidth = canvas.clientWidth;
      const canvasHeight = canvas.clientHeight;
      const naturalWidth = image.naturalWidth;
      const naturalHeight = image.naturalHeight;
      if (canvasWidth <= 0 || canvasHeight <= 0 || naturalWidth <= 0 || naturalHeight <= 0) {
        return { maxX: 0, maxY: 0 };
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
      const scaledWidth = fittedWidth * zoomLevel;
      const scaledHeight = fittedHeight * zoomLevel;
      return {
        maxX: Math.max(0, (scaledWidth - canvasWidth) / 2),
        maxY: Math.max(0, (scaledHeight - canvasHeight) / 2),
      };
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

    const setTransforms = () => {
      const { maxX, maxY } = clampPan();
      image.style.transform = `translate(${panX}px, ${panY}px) scale(${zoomLevel})`;
      image.style.cursor = maxX > ZOOM_EPSILON || maxY > ZOOM_EPSILON ? "grab" : "default";
    };

    const fitView = () => {
      zoomLevel = 1;
      panX = 0;
      panY = 0;
      setTransforms();
      updateButtons();
    };

    const clampZoom = (value) => Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, value));

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

    const updateButtons = () => {
      const hasImages = catalog.length > 0 && currentIndex >= 0;
      const canZoomIn = hasImages && zoomLevel < MAX_ZOOM - ZOOM_EPSILON;
      const canZoomOut = hasImages && zoomLevel > MIN_ZOOM + ZOOM_EPSILON;
      prevButton.disabled = !hasImages || currentIndex <= 0;
      nextButton.disabled = !hasImages || currentIndex >= catalog.length - 1;
      zoomInButton.disabled = !canZoomIn;
      zoomOutButton.disabled = !canZoomOut;
      zoomFitButton.disabled = !hasImages;
    };

    const renderActive = () => {
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
      fitView();
      updateButtons();
    };

    const selectImage = (index, options = {}) => {
      const preserveScroll = Boolean(options && typeof options === "object" && "preserveScroll" in options && options.preserveScroll);
      if (!Number.isInteger(index) || index < 0 || index >= catalog.length) {
        return;
      }
      const prevScrollX = preserveScroll ? window.scrollX : 0;
      const prevScrollY = preserveScroll ? window.scrollY : 0;
      currentIndex = index;
      renderActive();
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
    zoomFitButton.addEventListener("click", fitView);

    canvas.addEventListener(
      "wheel",
      (event) => {
        if (catalog.length === 0) {
          return;
        }
        event.preventDefault();
        const deltaPixels = normalizedWheelDeltaPixels(event);
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
        selectImage(savedIndex);
      } else {
        selectImage(0);
      }
    } else {
      renderActive();
    }

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
    }
    if (openTabLink instanceof HTMLAnchorElement) {
      openTabLink.href = cleanSrc;
    }

    lightbox.hidden = false;
    document.body.classList.add("is-lightbox-open");
  }

  function closeLightbox() {
    lightbox.hidden = true;
    imageTarget.removeAttribute("src");
    imageTarget.alt = "";
    if (captionTarget instanceof HTMLElement) {
      captionTarget.textContent = "";
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
    if (isReaderView && readerView && readerView.openByTrigger(trigger)) {
      event.preventDefault();
      return;
    }
    event.preventDefault();
    openLightbox(trigger.dataset.lightboxSrc || "", trigger.dataset.lightboxTitle || "");
  });

  if (contentRoot instanceof HTMLElement && !isReaderView) {
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
})();
