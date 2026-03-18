(() => {
  const creatorPageSelector = "[data-creator-detail-page]";
  const creatorFilterFormSelector = "[data-creator-filter-form]";
  const creatorFilterSearchSelector = "[data-creator-filter-search]";
  const dynamicCreatorLinkSelector = [
    ".creator-sort-bar a",
    ".creator-explorer-switch a",
    ".creator-tag-explorer-grid a",
    ".folder-explorer-grid a.folder-tile",
    ".creator-post-search-clear",
  ].join(", ");

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

  const buildUrlFromForm = (form) => {
    const action = form.getAttribute("action") || window.location.pathname;
    const targetUrl = new URL(action, window.location.href);
    const params = new URLSearchParams(new FormData(form));
    const query = params.toString();
    targetUrl.search = query;
    return targetUrl.toString();
  };

  let pendingSearchTimer = null;
  let activeFilterRequest = null;
  let filterRequestToken = 0;
  let searchInputToken = 0;

  const clearPendingSearchTimer = () => {
    if (pendingSearchTimer !== null) {
      window.clearTimeout(pendingSearchTimer);
      pendingSearchTimer = null;
    }
  };

  const replaceCreatorLayout = (htmlText, destinationUrl) => {
    const parser = new DOMParser();
    const parsedDocument = parser.parseFromString(htmlText, "text/html");
    const nextCreatorPage = parsedDocument.querySelector(creatorPageSelector);
    const currentCreatorPage = getCreatorPageRoot();
    if (!(nextCreatorPage instanceof HTMLElement) || !(currentCreatorPage instanceof HTMLElement)) {
      return false;
    }

    currentCreatorPage.replaceWith(nextCreatorPage);
    const nextTitle = parsedDocument.querySelector("title");
    if (nextTitle instanceof HTMLTitleElement) {
      const parsedTitle = (nextTitle.textContent || "").trim();
      if (parsedTitle) {
        document.title = parsedTitle;
      }
    }
    window.history.replaceState(null, "", destinationUrl);
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
      if (!replaceCreatorLayout(responseHtml, destinationUrl)) {
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
      if (!(target instanceof HTMLDetailsElement) || !target.classList.contains("creator-post-tag-details")) {
        return;
      }
      syncCardLayerClass(target);
      if (target.open) {
        closeAllTagPopoversExcept(target);
        window.requestAnimationFrame(() => {
          ensureTagListInViewport(target);
        });
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
      if (clickedInsidePopover) {
        return;
      }
      closeAllTagPopoversExcept(null);
    },
    true
  );

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }
    closeAllTagPopoversExcept(null);
  });

  document.addEventListener("submit", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLFormElement) || !target.matches(creatorFilterFormSelector)) {
      return;
    }
    if (!(target.closest(creatorPageSelector) instanceof HTMLElement)) {
      return;
    }
    event.preventDefault();
    fetchAndRenderCreatorLayout(buildUrlFromForm(target));
  });

  document.addEventListener("input", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement) || !target.matches(creatorFilterSearchSelector)) {
      return;
    }
    const form = target.closest(creatorFilterFormSelector);
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    if (!(form.closest(creatorPageSelector) instanceof HTMLElement)) {
      return;
    }
    queueSearchRefresh(form);
  });

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
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
    fetchAndRenderCreatorLayout(href);
  });

  syncAllCardLayers();
})();
