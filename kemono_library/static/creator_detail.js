(() => {
  const creatorPageSelector = "[data-creator-detail-page]";
  const creatorFilterFormSelector = "[data-creator-filter-form]";
  const creatorFilterSearchSelector = "[data-creator-filter-search]";
  const seriesQuickAddRootSelector = "[data-series-quick-add-root]";
  const seriesQuickAddToggleSelector = "[data-series-quick-add-toggle]";
  const seriesQuickAddFormSelector = "[data-series-quick-add-form]";
  const seriesQuickAddSearchSelector = "[data-series-quick-add-search]";
  const seriesQuickAddSearchClearSelector = "[data-series-quick-add-search-clear]";
  const seriesQuickAddIncludeAssignedSelector = "[data-series-quick-add-include-assigned]";
  const seriesQuickAddTagRowSelector = "[data-series-quick-add-tag-row]";
  const seriesQuickAddGridSelector = "[data-series-quick-add-grid]";
  const seriesQuickAddEmptySelector = "[data-series-quick-add-empty]";
  const seriesQuickAddStatusSelector = "[data-series-quick-add-status]";
  const seriesQuickAddSubmitSelector = "[data-series-quick-add-submit]";
  const seriesQuickAddSelectAllSelector = "[data-series-quick-add-select-all]";
  const seriesQuickAddSelectNoneSelector = "[data-series-quick-add-select-none]";
  const seriesQuickAddCardCheckboxSelector = "input[data-series-quick-add-post]";

  const dynamicCreatorLinkSelector = [
    ".creator-sort-bar a",
    ".creator-explorer-switch a",
    ".creator-tag-explorer-grid a",
    ".folder-explorer-grid a.folder-tile",
    ".creator-post-search-clear",
  ].join(", ");

  const shouldPushHistoryForLink = (link) =>
    link instanceof HTMLAnchorElement && link.matches(".folder-explorer-grid a.folder-tile");

  const getCreatorPageRoot = () => document.querySelector(creatorPageSelector);
  if (!(getCreatorPageRoot() instanceof HTMLElement)) {
    return;
  }

  const getCreatorSortPopover = () => {
    const root = getCreatorPageRoot();
    if (!(root instanceof HTMLElement)) {
      return null;
    }
    const node = root.querySelector(".creator-sort-popover");
    return node instanceof HTMLDetailsElement ? node : null;
  };

  const getCreatorTagSortCollapse = () => {
    const root = getCreatorPageRoot();
    if (!(root instanceof HTMLElement)) {
      return null;
    }
    const node = root.querySelector(".creator-tag-sort-collapse");
    return node instanceof HTMLDetailsElement ? node : null;
  };

  const getSeriesQuickAddRoot = () => {
    const root = getCreatorPageRoot();
    if (!(root instanceof HTMLElement)) {
      return null;
    }
    const node = root.querySelector(seriesQuickAddRootSelector);
    return node instanceof HTMLElement ? node : null;
  };

  const listTagPopovers = () =>
    Array.from(document.querySelectorAll(".creator-post-tag-details")).filter(
      (node) => node instanceof HTMLDetailsElement
    );

  const syncCardLayerClass = (popover) => {
    if (!(popover instanceof HTMLDetailsElement)) {
      return;
    }
    const card = popover.closest(".creator-post-card");
    if (!(card instanceof HTMLElement)) {
      return;
    }
    card.classList.toggle("is-tag-popover-open", popover.open);
  };

  const syncAllCardLayers = () => {
    listTagPopovers().forEach((popover) => {
      syncCardLayerClass(popover);
    });
  };

  const closeAllTagPopoversExcept = (keepOpen) => {
    listTagPopovers().forEach((popover) => {
      if (!(popover instanceof HTMLDetailsElement) || popover === keepOpen) {
        return;
      }
      popover.classList.remove("is-tag-list-align-right");
      popover.open = false;
      syncCardLayerClass(popover);
    });
  };

  const ensureTagListInViewport = (popover) => {
    if (!(popover instanceof HTMLDetailsElement) || !popover.open) {
      return;
    }
    const list = popover.querySelector(".creator-post-tag-list");
    if (!(list instanceof HTMLElement)) {
      return;
    }

    const viewportPadding = 12;
    popover.classList.remove("is-tag-list-align-right");

    let rect = list.getBoundingClientRect();
    if (rect.right > window.innerWidth - viewportPadding) {
      popover.classList.add("is-tag-list-align-right");
      rect = list.getBoundingClientRect();
    }

    if (rect.left < viewportPadding && popover.classList.contains("is-tag-list-align-right")) {
      popover.classList.remove("is-tag-list-align-right");
      rect = list.getBoundingClientRect();
    }

    let deltaY = 0;
    if (rect.bottom > window.innerHeight - viewportPadding) {
      deltaY = rect.bottom - (window.innerHeight - viewportPadding);
    } else if (rect.top < viewportPadding) {
      deltaY = rect.top - viewportPadding;
    }

    if (Math.abs(deltaY) > 1) {
      window.scrollBy({ top: deltaY, behavior: "smooth" });
    }
  };

  const isCreatorDetailUrl = (href) => {
    try {
      const targetUrl = new URL(href, window.location.href);
      return targetUrl.origin === window.location.origin && /^\/creators\/\d+\/?$/.test(targetUrl.pathname);
    } catch (_error) {
      return false;
    }
  };

  const getCreatorDetailDefaults = (root = null) => {
    const sourceRoot = root instanceof HTMLElement ? root : getCreatorPageRoot();
    const defaultSortRaw =
      sourceRoot instanceof HTMLElement ? (sourceRoot.getAttribute("data-default-sort") || "").trim().toLowerCase() : "";
    const defaultDirectionRaw =
      sourceRoot instanceof HTMLElement ? (sourceRoot.getAttribute("data-default-direction") || "").trim().toLowerCase() : "";
    const defaultExplorerRaw =
      sourceRoot instanceof HTMLElement ? (sourceRoot.getAttribute("data-default-explorer") || "").trim().toLowerCase() : "";

    return {
      defaultSort: defaultSortRaw === "title" ? "title" : "published",
      defaultDirection: defaultDirectionRaw === "asc" ? "asc" : "desc",
      defaultExplorer: defaultExplorerRaw === "tags" ? "tags" : "series",
    };
  };

  const canonicalizeCreatorDetailUrl = (href, { root = null } = {}) => {
    try {
      const targetUrl = new URL(href, window.location.href);
      if (!isCreatorDetailUrl(targetUrl.toString())) {
        return targetUrl.toString();
      }

      const { defaultSort, defaultDirection, defaultExplorer } = getCreatorDetailDefaults(root);
      const params = targetUrl.searchParams;

      const sortValue = (params.get("sort") || "").trim().toLowerCase();
      if (!sortValue || sortValue === defaultSort) {
        params.delete("sort");
      }

      const directionValue = (params.get("direction") || "").trim().toLowerCase();
      if (!directionValue || directionValue === defaultDirection) {
        params.delete("direction");
      }

      const explorerValue = (params.get("explorer") || "").trim().toLowerCase();
      if (!explorerValue || explorerValue === defaultExplorer) {
        params.delete("explorer");
      }

      const queryText = (params.get("q") || "").trim();
      if (!queryText) {
        params.delete("q");
      } else {
        params.set("q", queryText);
      }

      const normalizedTags = params
        .getAll("tag")
        .map((tag) => tag.trim())
        .filter((tag) => tag.length > 0);
      params.delete("tag");
      normalizedTags.forEach((tag) => {
        params.append("tag", tag);
      });

      const rawSeriesId = (params.get("series_id") || "").trim();
      const seriesId = Number(rawSeriesId);
      const hasValidSeriesId = Number.isInteger(seriesId) && seriesId > 0;
      if (!hasValidSeriesId) {
        params.delete("series_id");
      }

      if (hasValidSeriesId) {
        params.delete("folder");
      } else {
        const folderValue = (params.get("folder") || "").trim().toLowerCase();
        if (folderValue !== "unsorted") {
          params.delete("folder");
        } else {
          params.set("folder", "unsorted");
        }
      }

      return targetUrl.toString();
    } catch (_error) {
      return href;
    }
  };

  const buildUrlFromForm = (form) => {
    const action = form.getAttribute("action") || window.location.pathname;
    const targetUrl = new URL(action, window.location.href);
    const params = new URLSearchParams(new FormData(form));
    const query = params.toString();
    targetUrl.search = query;
    return canonicalizeCreatorDetailUrl(targetUrl.toString());
  };

  let pendingSearchTimer = null;
  let activeFilterRequest = null;
  let filterRequestToken = 0;
  let searchInputToken = 0;

  let quickAddPendingSearchTimer = null;
  let quickAddActiveRequest = null;
  let quickAddRequestToken = 0;
  let quickAddSearchToken = 0;
  let quickAddSelectedTags = [];
  let quickAddSelectedTagKeys = new Set();
  let quickAddSelectedPostIds = new Set();
  let quickAddIncludeAssigned = false;

  const clearPendingSearchTimer = () => {
    if (pendingSearchTimer !== null) {
      window.clearTimeout(pendingSearchTimer);
      pendingSearchTimer = null;
    }
  };

  const clearQuickAddPendingSearchTimer = () => {
    if (quickAddPendingSearchTimer !== null) {
      window.clearTimeout(quickAddPendingSearchTimer);
      quickAddPendingSearchTimer = null;
    }
  };

  const abortQuickAddRequest = () => {
    if (quickAddActiveRequest instanceof AbortController) {
      quickAddActiveRequest.abort();
      quickAddActiveRequest = null;
    }
  };

  const resetQuickAddState = () => {
    clearQuickAddPendingSearchTimer();
    abortQuickAddRequest();
    quickAddSelectedTags = [];
    quickAddSelectedTagKeys = new Set();
    quickAddSelectedPostIds = new Set();
    quickAddIncludeAssigned = false;
  };

  const getQuickAddElements = () => {
    const root = getSeriesQuickAddRoot();
    if (!(root instanceof HTMLElement)) {
      return null;
    }
    const toggle = root.querySelector(seriesQuickAddToggleSelector);
    const form = root.querySelector(seriesQuickAddFormSelector);
    const searchInput = root.querySelector(seriesQuickAddSearchSelector);
    const searchClear = root.querySelector(seriesQuickAddSearchClearSelector);
    const includeAssignedToggle = root.querySelector(seriesQuickAddIncludeAssignedSelector);
    const tagRow = root.querySelector(seriesQuickAddTagRowSelector);
    const grid = root.querySelector(seriesQuickAddGridSelector);
    const empty = root.querySelector(seriesQuickAddEmptySelector);
    const status = root.querySelector(seriesQuickAddStatusSelector);
    const submit = root.querySelector(seriesQuickAddSubmitSelector);
    const selectAll = root.querySelector(seriesQuickAddSelectAllSelector);
    const selectNone = root.querySelector(seriesQuickAddSelectNoneSelector);

    if (
      !(toggle instanceof HTMLButtonElement) ||
      !(form instanceof HTMLFormElement) ||
      !(searchInput instanceof HTMLInputElement) ||
      !(searchClear instanceof HTMLElement) ||
      !(includeAssignedToggle instanceof HTMLButtonElement) ||
      !(tagRow instanceof HTMLElement) ||
      !(grid instanceof HTMLElement) ||
      !(empty instanceof HTMLElement) ||
      !(status instanceof HTMLElement) ||
      !(submit instanceof HTMLButtonElement) ||
      !(selectAll instanceof HTMLButtonElement) ||
      !(selectNone instanceof HTMLButtonElement)
    ) {
      return null;
    }

    return {
      root,
      toggle,
      form,
      searchInput,
      searchClear,
      includeAssignedToggle,
      tagRow,
      grid,
      empty,
      status,
      submit,
      selectAll,
      selectNone,
    };
  };

  const isQuickAddOpen = () => {
    const elements = getQuickAddElements();
    return Boolean(elements && elements.root.classList.contains("is-open") && !elements.form.hidden);
  };

  const setQuickAddOpen = (open, { refresh = true } = {}) => {
    const elements = getQuickAddElements();
    if (!elements) {
      return;
    }
    elements.root.classList.toggle("is-open", open);
    elements.toggle.setAttribute("aria-expanded", open ? "true" : "false");
    elements.form.hidden = !open;
    if (open) {
      if (refresh) {
        resetQuickAddState();
        elements.searchInput.value = "";
        updateQuickAddSearchClearVisibility();
        updateQuickAddIncludeAssignedToggle();
        updateQuickAddSubmitState();
        fetchAndRenderQuickAddCandidates();
      }
      return;
    }
    resetQuickAddState();
    updateQuickAddIncludeAssignedToggle();
  };

  const updateQuickAddSearchClearVisibility = () => {
    const elements = getQuickAddElements();
    if (!elements) {
      return;
    }
    elements.searchClear.hidden = !elements.searchInput.value.trim();
  };

  const updateQuickAddIncludeAssignedToggle = () => {
    const elements = getQuickAddElements();
    if (!elements) {
      return;
    }
    elements.includeAssignedToggle.classList.toggle("is-active", quickAddIncludeAssigned);
    elements.includeAssignedToggle.setAttribute("aria-pressed", quickAddIncludeAssigned ? "true" : "false");
    elements.includeAssignedToggle.textContent = quickAddIncludeAssigned ? "Hide assigned" : "Show assigned";
  };

  const updateQuickAddSubmitState = () => {
    const elements = getQuickAddElements();
    if (!elements) {
      return;
    }
    const selectedCount = quickAddSelectedPostIds.size;
    elements.submit.disabled = selectedCount <= 0;
    elements.submit.textContent = selectedCount > 0 ? `Add selected (${selectedCount})` : "Add selected";
  };

  const renderQuickAddTagFacets = (tagFacets) => {
    const elements = getQuickAddElements();
    if (!elements) {
      return;
    }
    elements.tagRow.innerHTML = "";
    if (!Array.isArray(tagFacets) || tagFacets.length <= 0) {
      return;
    }

    for (const facet of tagFacets) {
      const rawTag = typeof facet.tag === "string" ? facet.tag : "";
      const tag = rawTag.trim();
      if (!tag) {
        continue;
      }
      const normalizedTag = typeof facet.normalized_tag === "string" ? facet.normalized_tag : tag.toLowerCase();
      const postCount = Number.isFinite(Number(facet.post_count)) ? Number(facet.post_count) : 0;
      const isSelected = quickAddSelectedTagKeys.has(normalizedTag);

      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = `chip chip--control creator-tag-option-chip${isSelected ? " is-active" : ""}`;
      chip.setAttribute("data-series-quick-add-tag", normalizedTag);
      chip.setAttribute("data-series-quick-add-tag-value", tag);

      const label = document.createElement("span");
      label.textContent = tag;
      const count = document.createElement("small");
      count.textContent = String(postCount);
      chip.append(label, count);
      elements.tagRow.append(chip);
    }
  };

  const getQuickAddThumbnailUrl = (post) => {
    const localPath = typeof post.thumbnail_local_path === "string" ? post.thumbnail_local_path.trim() : "";
    if (localPath) {
      const normalizedPath = localPath.replace(/\\/g, "/").replace(/^\/+/, "");
      const encodedPath = normalizedPath
        .split("/")
        .filter((segment) => segment.length > 0)
        .map((segment) => encodeURIComponent(segment))
        .join("/");
      if (encodedPath) {
        return `/files/${encodedPath}`;
      }
    }

    const remoteUrl = typeof post.thumbnail_remote_url === "string" ? post.thumbnail_remote_url.trim() : "";
    return remoteUrl || "";
  };

  const clampQuickAddThumbnailFocus = (rawValue, fallback = 50) => {
    const numericValue = Number(rawValue);
    if (!Number.isFinite(numericValue)) {
      return fallback;
    }
    return Math.max(0, Math.min(100, numericValue));
  };

  const syncQuickAddCardSelection = (checkbox) => {
    if (!(checkbox instanceof HTMLInputElement)) {
      return;
    }
    const card = checkbox.closest(".series-quick-add-card");
    if (!(card instanceof HTMLElement)) {
      return;
    }
    card.classList.toggle("is-selected", checkbox.checked);
  };

  const renderQuickAddPosts = (posts) => {
    const elements = getQuickAddElements();
    if (!elements) {
      return;
    }

    elements.grid.innerHTML = "";

    if (!Array.isArray(posts) || posts.length <= 0) {
      elements.empty.hidden = false;
      return;
    }
    elements.empty.hidden = true;

    for (const post of posts) {
      const postId = Number(post.id);
      if (!Number.isInteger(postId) || postId <= 0) {
        continue;
      }

      const title = typeof post.title === "string" && post.title.trim() ? post.title.trim() : `Post ${postId}`;
      const seriesName = typeof post.series_name === "string" && post.series_name.trim() ? post.series_name.trim() : "Unsorted";
      const seriesId = Number(post.series_id);
      const hasSourceSeries = Number.isInteger(seriesId) && seriesId > 0;
      const publishedAt = typeof post.published_at === "string" && post.published_at.trim() ? post.published_at.trim() : "";
      const thumbnailUrl = getQuickAddThumbnailUrl(post);
      const focusX = clampQuickAddThumbnailFocus(post.thumbnail_focus_x, 50);
      const focusY = clampQuickAddThumbnailFocus(post.thumbnail_focus_y, 50);
      const tags = Array.isArray(post.default_tags)
        ? post.default_tags.map((value) => String(value || "").trim()).filter((value) => value.length > 0)
        : [];

      const card = document.createElement("article");
      card.className = "series-quick-add-card creator-post-card";

      const head = document.createElement("div");
      head.className = "series-quick-add-card-head";

      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.value = String(postId);
      checkbox.setAttribute("data-series-quick-add-post", "1");
      checkbox.checked = quickAddSelectedPostIds.has(postId);
      syncQuickAddCardSelection(checkbox);

      const titleWrap = document.createElement("h3");
      titleWrap.className = "series-quick-add-card-title";
      titleWrap.textContent = title;

      head.append(checkbox, titleWrap);
      card.append(head);

      const thumbnail = document.createElement("div");
      thumbnail.className = "creator-post-thumb series-quick-add-card-thumb";
      if (publishedAt) {
        const publishedBadge = document.createElement("time");
        publishedBadge.className = "creator-post-published";
        publishedBadge.setAttribute("datetime", publishedAt);
        publishedBadge.textContent = publishedAt;
        thumbnail.append(publishedBadge);
      }
      if (thumbnailUrl) {
        const image = document.createElement("img");
        image.src = thumbnailUrl;
        image.alt = title;
        image.loading = "lazy";
        image.style.objectFit = "cover";
        image.style.objectPosition = `${focusX}% ${focusY}%`;
        if (/^https?:\/\//i.test(thumbnailUrl)) {
          image.referrerPolicy = "no-referrer";
        }
        thumbnail.append(image);
      } else {
        const fallback = document.createElement("span");
        fallback.className = "creator-post-thumb-fallback";
        fallback.textContent = "No preview";
        fallback.setAttribute("aria-hidden", "true");
        thumbnail.append(fallback);
      }
      card.append(thumbnail);

      const body = document.createElement("div");
      body.className = "creator-post-body";

      const meta = document.createElement("div");
      meta.className = "creator-post-meta series-quick-add-card-meta";

      if (hasSourceSeries) {
        const sourceChip = document.createElement("span");
        sourceChip.className = "chip chip--accent creator-post-badge";
        sourceChip.textContent = seriesName;
        meta.append(sourceChip);
      }

      const inlineTagCount = 4;
      let inlineTags = tags.slice(0, inlineTagCount);
      let overflowTags = tags.slice(inlineTagCount);
      if (overflowTags.length === 1) {
        inlineTags = tags.slice();
        overflowTags = [];
      }

      for (const tag of inlineTags) {
        const tagChip = document.createElement("span");
        tagChip.className = "chip creator-post-tag-chip";
        tagChip.textContent = tag;
        meta.append(tagChip);
      }

      if (overflowTags.length > 0) {
        const overflowDetails = document.createElement("details");
        overflowDetails.className = "creator-post-tag-details";

        const overflowSummary = document.createElement("summary");
        overflowSummary.className = "chip chip--control creator-post-tag-summary";
        const overflowCount = document.createElement("span");
        overflowCount.setAttribute("aria-hidden", "true");
        overflowCount.textContent = `+${overflowTags.length}`;
        const overflowLabel = document.createElement("span");
        overflowLabel.className = "sr-only";
        overflowLabel.textContent = `Show ${overflowTags.length} more tags`;
        overflowSummary.append(overflowCount, overflowLabel);

        const overflowList = document.createElement("div");
        overflowList.className = "creator-post-tag-list layout-row layout-row--wrap layout-row--sm";
        for (const tag of overflowTags) {
          const tagChip = document.createElement("span");
          tagChip.className = "chip creator-post-tag-chip";
          tagChip.textContent = tag;
          overflowList.append(tagChip);
        }

        overflowDetails.append(overflowSummary, overflowList);
        meta.append(overflowDetails);
      }

      body.append(meta);
      card.append(body);
      elements.grid.append(card);
    }
  };

  const updateQuickAddStatus = (message) => {
    const elements = getQuickAddElements();
    if (!elements) {
      return;
    }
    elements.status.textContent = message;
  };

  const setQuickAddTags = (tags) => {
    const nextTags = [];
    const nextKeys = new Set();
    for (const rawTag of tags) {
      const tag = String(rawTag || "").trim();
      if (!tag) {
        continue;
      }
      const key = tag.toLowerCase();
      if (nextKeys.has(key)) {
        continue;
      }
      nextKeys.add(key);
      nextTags.push(tag);
    }
    quickAddSelectedTags = nextTags;
    quickAddSelectedTagKeys = nextKeys;
  };

  const buildQuickAddCandidatesUrl = () => {
    const root = getSeriesQuickAddRoot();
    if (!(root instanceof HTMLElement)) {
      return null;
    }
    const base = root.getAttribute("data-series-quick-add-candidates-url");
    if (!base) {
      return null;
    }
    const url = new URL(base, window.location.origin);
    const elements = getQuickAddElements();
    if (!elements) {
      return url.toString();
    }

    const searchText = elements.searchInput.value.trim();
    if (searchText) {
      url.searchParams.set("q", searchText);
    }
    if (quickAddIncludeAssigned) {
      url.searchParams.set("include_assigned", "1");
    }
    for (const tag of quickAddSelectedTags) {
      url.searchParams.append("tag", tag);
    }
    return url.toString();
  };

  const fetchAndRenderQuickAddCandidates = async ({ expectedSearchToken = null } = {}) => {
    if (!isQuickAddOpen()) {
      return;
    }

    const targetUrl = buildQuickAddCandidatesUrl();
    if (!targetUrl) {
      return;
    }

    quickAddRequestToken += 1;
    const requestToken = quickAddRequestToken;
    abortQuickAddRequest();
    const controller = new AbortController();
    quickAddActiveRequest = controller;

    updateQuickAddStatus("Loading posts...");

    try {
      const response = await fetch(targetUrl, {
        method: "GET",
        credentials: "same-origin",
        headers: {
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest",
        },
        cache: "no-store",
        signal: controller.signal,
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(typeof payload.error === "string" ? payload.error : `Failed (${response.status})`);
      }
      if (requestToken !== quickAddRequestToken) {
        return;
      }
      if (expectedSearchToken !== null && expectedSearchToken !== quickAddSearchToken) {
        return;
      }

      const selectedTags = Array.isArray(payload.selected_tags) ? payload.selected_tags : [];
      setQuickAddTags(selectedTags);
      if (typeof payload.include_assigned === "boolean") {
        quickAddIncludeAssigned = payload.include_assigned;
      }
      updateQuickAddIncludeAssignedToggle();
      renderQuickAddTagFacets(Array.isArray(payload.tag_facets) ? payload.tag_facets : []);
      renderQuickAddPosts(Array.isArray(payload.posts) ? payload.posts : []);
      updateQuickAddSearchClearVisibility();
      updateQuickAddSubmitState();

      const visibleCount = Number.isFinite(Number(payload.count)) ? Number(payload.count) : 0;
      const selectedCount = quickAddSelectedPostIds.size;
      if (visibleCount > 0) {
        updateQuickAddStatus(
          selectedCount > 0
            ? `${visibleCount} matching posts | ${selectedCount} selected`
            : `${visibleCount} matching posts`
        );
      } else {
        updateQuickAddStatus(
          selectedCount > 0
            ? `No matches | ${selectedCount} selected`
            : "No matching posts"
        );
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        return;
      }
      updateQuickAddStatus(error instanceof Error ? error.message : "Failed to load quick-add posts.");
      const elements = getQuickAddElements();
      if (elements) {
        elements.grid.innerHTML = "";
        elements.empty.hidden = false;
      }
    } finally {
      if (quickAddActiveRequest === controller) {
        quickAddActiveRequest = null;
      }
    }
  };

  const queueQuickAddSearchRefresh = () => {
    quickAddSearchToken += 1;
    const expectedSearchToken = quickAddSearchToken;
    clearQuickAddPendingSearchTimer();
    quickAddPendingSearchTimer = window.setTimeout(() => {
      quickAddPendingSearchTimer = null;
      fetchAndRenderQuickAddCandidates({ expectedSearchToken });
    }, 220);
  };

  const replaceHeaderBrandContext = (parsedDocument) => {
    if (!(parsedDocument instanceof Document)) {
      return;
    }
    const nextHeader = parsedDocument.querySelector(".site-header");
    const currentHeader = document.querySelector(".site-header");
    if (!(nextHeader instanceof HTMLElement) || !(currentHeader instanceof HTMLElement)) {
      return;
    }
    const nextBrand = nextHeader.querySelector(".brand");
    const currentBrand = currentHeader.querySelector(".brand");
    if (!(nextBrand instanceof Element) || !(currentBrand instanceof Element)) {
      return;
    }
    currentBrand.replaceWith(nextBrand);
  };

  const replaceCreatorLayout = (htmlText, destinationUrl, { historyMode = "replace" } = {}) => {
    const parser = new DOMParser();
    const parsedDocument = parser.parseFromString(htmlText, "text/html");
    const nextCreatorPage = parsedDocument.querySelector(creatorPageSelector);
    const currentCreatorPage = getCreatorPageRoot();
    if (!(nextCreatorPage instanceof HTMLElement) || !(currentCreatorPage instanceof HTMLElement)) {
      return false;
    }

    replaceHeaderBrandContext(parsedDocument);
    currentCreatorPage.replaceWith(nextCreatorPage);
    const nextTitle = parsedDocument.querySelector("title");
    if (nextTitle instanceof HTMLTitleElement) {
      const parsedTitle = (nextTitle.textContent || "").trim();
      if (parsedTitle) {
        document.title = parsedTitle;
      }
    }
    const canonicalDestinationUrl = canonicalizeCreatorDetailUrl(destinationUrl, { root: nextCreatorPage });
    if (historyMode === "push") {
      window.history.pushState(null, "", canonicalDestinationUrl);
    } else if (historyMode === "replace") {
      window.history.replaceState(null, "", canonicalDestinationUrl);
    }
    resetQuickAddState();
    syncAllCardLayers();
    return true;
  };

  const fetchAndRenderCreatorLayout = async (
    targetUrl,
    {
      restoreSearchCaret = false,
      expectedSearchToken = null,
      preserveSortPopoverState = true,
      preserveTagSortCollapseState = true,
      historyMode = "replace",
    } = {}
  ) => {
    if (!isCreatorDetailUrl(targetUrl)) {
      window.location.assign(targetUrl);
      return;
    }
    const rootBeforeFetch = getCreatorPageRoot();
    if (!(rootBeforeFetch instanceof HTMLElement)) {
      window.location.assign(targetUrl);
      return;
    }

    clearPendingSearchTimer();
    filterRequestToken += 1;
    const requestToken = filterRequestToken;
    if (activeFilterRequest instanceof AbortController) {
      activeFilterRequest.abort();
    }
    const controller = new AbortController();
    activeFilterRequest = controller;

    let caretStart = null;
    let caretEnd = null;
    const currentSortPopover = getCreatorSortPopover();
    const reopenSortPopover =
      preserveSortPopoverState &&
      currentSortPopover instanceof HTMLDetailsElement &&
      currentSortPopover.open;
    const currentTagSortCollapse = getCreatorTagSortCollapse();
    const reopenTagSortCollapse =
      preserveTagSortCollapseState &&
      currentTagSortCollapse instanceof HTMLDetailsElement &&
      currentTagSortCollapse.open;
    if (restoreSearchCaret) {
      const activeElement = document.activeElement;
      if (activeElement instanceof HTMLInputElement && activeElement.matches(creatorFilterSearchSelector)) {
        caretStart = activeElement.selectionStart;
        caretEnd = activeElement.selectionEnd;
      }
    }

    rootBeforeFetch.setAttribute("aria-busy", "true");
    try {
      const response = await fetch(targetUrl, {
        method: "GET",
        credentials: "same-origin",
        headers: {
          Accept: "text/html,application/xhtml+xml",
          "X-Requested-With": "XMLHttpRequest",
        },
        cache: "no-store",
        signal: controller.signal,
      });
      if (!response.ok) {
        throw new Error(`Creator filter request failed (${response.status})`);
      }

      const responseHtml = await response.text();
      if (requestToken !== filterRequestToken) {
        return;
      }
      if (expectedSearchToken !== null && expectedSearchToken !== searchInputToken) {
        return;
      }

      const destinationUrl = response.url || targetUrl;
      if (!replaceCreatorLayout(responseHtml, destinationUrl, { historyMode })) {
        window.location.assign(targetUrl);
        return;
      }
      if (reopenSortPopover) {
        const nextSortPopover = getCreatorSortPopover();
        if (nextSortPopover instanceof HTMLDetailsElement) {
          nextSortPopover.open = true;
        }
      }
      if (reopenTagSortCollapse) {
        const nextTagSortCollapse = getCreatorTagSortCollapse();
        if (nextTagSortCollapse instanceof HTMLDetailsElement) {
          nextTagSortCollapse.open = true;
        }
      }

      if (restoreSearchCaret) {
        const nextSearchInput = document.querySelector(creatorFilterSearchSelector);
        if (nextSearchInput instanceof HTMLInputElement) {
          nextSearchInput.focus();
          if (caretStart !== null && caretEnd !== null) {
            const valueLength = nextSearchInput.value.length;
            const nextStart = Math.max(0, Math.min(caretStart, valueLength));
            const nextEnd = Math.max(0, Math.min(caretEnd, valueLength));
            nextSearchInput.setSelectionRange(nextStart, nextEnd);
          }
        }
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        return;
      }
      window.location.assign(targetUrl);
    } finally {
      const currentRoot = getCreatorPageRoot();
      if (currentRoot instanceof HTMLElement) {
        currentRoot.removeAttribute("aria-busy");
      }
      if (activeFilterRequest === controller) {
        activeFilterRequest = null;
      }
    }
  };

  const queueSearchRefresh = (form) => {
    searchInputToken += 1;
    const expectedSearchToken = searchInputToken;
    if (activeFilterRequest instanceof AbortController) {
      activeFilterRequest.abort();
    }
    clearPendingSearchTimer();
    pendingSearchTimer = window.setTimeout(() => {
      pendingSearchTimer = null;
      fetchAndRenderCreatorLayout(buildUrlFromForm(form), {
        restoreSearchCaret: true,
        expectedSearchToken,
      });
    }, 220);
  };

  document.addEventListener(
    "toggle",
    (event) => {
      const target = event.target;
      if (!(target instanceof HTMLDetailsElement)) {
        return;
      }

      if (target.classList.contains("creator-post-tag-details")) {
        syncCardLayerClass(target);
        if (target.open) {
          closeAllTagPopoversExcept(target);
          window.requestAnimationFrame(() => {
            ensureTagListInViewport(target);
          });
        }
      }
    },
    true
  );

  document.addEventListener(
    "pointerdown",
    (event) => {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      const clickedInsidePopover = listTagPopovers().some((popover) => popover.contains(target));
      if (!clickedInsidePopover) {
        closeAllTagPopoversExcept(null);
      }
    },
    true
  );

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }
    closeAllTagPopoversExcept(null);
    if (isQuickAddOpen()) {
      setQuickAddOpen(false);
    }
  });

  document.addEventListener("submit", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLFormElement)) {
      return;
    }

    if (target.matches(creatorFilterFormSelector)) {
      if (!(target.closest(creatorPageSelector) instanceof HTMLElement)) {
        return;
      }
      event.preventDefault();
      fetchAndRenderCreatorLayout(buildUrlFromForm(target));
      return;
    }

    if (!target.matches(seriesQuickAddFormSelector)) {
      return;
    }
    if (!(target.closest(creatorPageSelector) instanceof HTMLElement)) {
      return;
    }
    event.preventDefault();

    const elements = getQuickAddElements();
    if (!elements) {
      return;
    }

    if (quickAddSelectedPostIds.size <= 0) {
      updateQuickAddStatus("Select at least one post.");
      updateQuickAddSubmitState();
      return;
    }

    const submitUrl = elements.root.getAttribute("data-series-quick-add-submit-url");
    if (!submitUrl) {
      updateQuickAddStatus("Quick-add submit URL is missing.");
      return;
    }

    const formData = new FormData();
    for (const postId of quickAddSelectedPostIds) {
      formData.append("post_id", String(postId));
    }

    elements.submit.disabled = true;
    updateQuickAddStatus("Adding selected posts...");

    try {
      const response = await fetch(submitUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          Accept: "application/json",
          "X-Requested-With": "XMLHttpRequest",
        },
        body: formData,
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(typeof payload.error === "string" ? payload.error : `Failed (${response.status})`);
      }

      setQuickAddOpen(false);
      await fetchAndRenderCreatorLayout(window.location.href);
    } catch (error) {
      updateQuickAddStatus(error instanceof Error ? error.message : "Failed to add selected posts.");
      updateQuickAddSubmitState();
    }
  });

  document.addEventListener("input", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) {
      return;
    }

    if (target.matches(creatorFilterSearchSelector)) {
      const form = target.closest(creatorFilterFormSelector);
      if (!(form instanceof HTMLFormElement)) {
        return;
      }
      if (!(form.closest(creatorPageSelector) instanceof HTMLElement)) {
        return;
      }
      queueSearchRefresh(form);
      return;
    }

    if (!target.matches(seriesQuickAddSearchSelector)) {
      return;
    }
    if (!(target.closest(seriesQuickAddFormSelector) instanceof HTMLFormElement)) {
      return;
    }
    updateQuickAddSearchClearVisibility();
    queueQuickAddSearchRefresh();
  });

  document.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) {
      return;
    }
    if (!target.matches(seriesQuickAddCardCheckboxSelector)) {
      return;
    }
    const postId = Number(target.value);
    if (!Number.isInteger(postId) || postId <= 0) {
      return;
    }
    if (target.checked) {
      quickAddSelectedPostIds.add(postId);
    } else {
      quickAddSelectedPostIds.delete(postId);
    }
    syncQuickAddCardSelection(target);
    updateQuickAddSubmitState();

    const elements = getQuickAddElements();
    if (elements) {
      const visibleCount = elements.grid.querySelectorAll(seriesQuickAddCardCheckboxSelector).length;
      const selectedCount = quickAddSelectedPostIds.size;
      if (visibleCount > 0) {
        updateQuickAddStatus(
          selectedCount > 0
            ? `${visibleCount} matching posts | ${selectedCount} selected`
            : `${visibleCount} matching posts`
        );
      }
    }
  });

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }

    const quickAddToggle = target.closest(seriesQuickAddToggleSelector);
    if (quickAddToggle instanceof HTMLElement) {
      const shouldOpen = !isQuickAddOpen();
      setQuickAddOpen(shouldOpen);
      return;
    }

    const quickAddSearchClear = target.closest(seriesQuickAddSearchClearSelector);
    if (quickAddSearchClear instanceof HTMLElement) {
      const elements = getQuickAddElements();
      if (elements) {
        elements.searchInput.value = "";
        updateQuickAddSearchClearVisibility();
        queueQuickAddSearchRefresh();
      }
      return;
    }

    const includeAssignedToggle = target.closest(seriesQuickAddIncludeAssignedSelector);
    if (includeAssignedToggle instanceof HTMLElement) {
      quickAddIncludeAssigned = !quickAddIncludeAssigned;
      updateQuickAddIncludeAssignedToggle();
      queueQuickAddSearchRefresh();
      return;
    }

    const quickAddCard = target.closest(".series-quick-add-card");
    if (quickAddCard instanceof HTMLElement && quickAddCard.closest(seriesQuickAddGridSelector)) {
      const interactiveChild = target.closest("a, button, input, select, textarea, details, summary");
      if (!(interactiveChild instanceof HTMLElement)) {
        const checkbox = quickAddCard.querySelector(seriesQuickAddCardCheckboxSelector);
        if (checkbox instanceof HTMLInputElement) {
          checkbox.checked = !checkbox.checked;
          checkbox.dispatchEvent(new Event("change", { bubbles: true }));
        }
      }
      return;
    }

    const quickAddTagChip = target.closest("[data-series-quick-add-tag]");
    if (quickAddTagChip instanceof HTMLElement) {
      const key = (quickAddTagChip.getAttribute("data-series-quick-add-tag") || "").trim().toLowerCase();
      const value = (quickAddTagChip.getAttribute("data-series-quick-add-tag-value") || "").trim();
      if (!key || !value) {
        return;
      }

      if (quickAddSelectedTagKeys.has(key)) {
        setQuickAddTags(quickAddSelectedTags.filter((tag) => tag.toLowerCase() !== key));
      } else {
        setQuickAddTags([...quickAddSelectedTags, value]);
      }
      queueQuickAddSearchRefresh();
      return;
    }

    const selectAllButton = target.closest(seriesQuickAddSelectAllSelector);
    if (selectAllButton instanceof HTMLElement) {
      const elements = getQuickAddElements();
      if (!elements) {
        return;
      }
      elements.grid.querySelectorAll(seriesQuickAddCardCheckboxSelector).forEach((node) => {
        if (!(node instanceof HTMLInputElement)) {
          return;
        }
        const postId = Number(node.value);
        if (!Number.isInteger(postId) || postId <= 0) {
          return;
        }
        node.checked = true;
        quickAddSelectedPostIds.add(postId);
        syncQuickAddCardSelection(node);
      });
      updateQuickAddSubmitState();
      const visibleCount = elements.grid.querySelectorAll(seriesQuickAddCardCheckboxSelector).length;
      updateQuickAddStatus(`${visibleCount} matching posts | ${quickAddSelectedPostIds.size} selected`);
      return;
    }

    const selectNoneButton = target.closest(seriesQuickAddSelectNoneSelector);
    if (selectNoneButton instanceof HTMLElement) {
      const elements = getQuickAddElements();
      if (!elements) {
        return;
      }
      elements.grid.querySelectorAll(seriesQuickAddCardCheckboxSelector).forEach((node) => {
        if (!(node instanceof HTMLInputElement)) {
          return;
        }
        const postId = Number(node.value);
        if (!Number.isInteger(postId) || postId <= 0) {
          return;
        }
        node.checked = false;
        quickAddSelectedPostIds.delete(postId);
        syncQuickAddCardSelection(node);
      });
      updateQuickAddSubmitState();
      const visibleCount = elements.grid.querySelectorAll(seriesQuickAddCardCheckboxSelector).length;
      updateQuickAddStatus(`${visibleCount} matching posts`);
      return;
    }

    const link = target.closest(dynamicCreatorLinkSelector);
    if (!(link instanceof HTMLAnchorElement)) {
      return;
    }
    if (!(link.closest(creatorPageSelector) instanceof HTMLElement)) {
      return;
    }
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return;
    }
    if (link.target && link.target !== "_self") {
      return;
    }
    const href = link.href;
    if (!href || !isCreatorDetailUrl(href)) {
      return;
    }
    event.preventDefault();
    fetchAndRenderCreatorLayout(href, {
      historyMode: shouldPushHistoryForLink(link) ? "push" : "replace",
    });
  });

  window.addEventListener("popstate", () => {
    const targetUrl = window.location.href;
    if (!isCreatorDetailUrl(targetUrl)) {
      return;
    }
    fetchAndRenderCreatorLayout(targetUrl, {
      historyMode: "preserve",
      preserveSortPopoverState: false,
      preserveTagSortCollapseState: false,
    });
  });

  const canonicalCurrentUrl = canonicalizeCreatorDetailUrl(window.location.href);
  if (canonicalCurrentUrl !== window.location.href) {
    window.history.replaceState(null, "", canonicalCurrentUrl);
  }

  syncAllCardLayers();
})();
