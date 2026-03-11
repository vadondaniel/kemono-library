(() => {
  const key = "kemono-theme";
  const root = document.documentElement;
  const radios = Array.from(document.querySelectorAll("[data-theme-toggle-radio]")).filter(
    (node) => node instanceof HTMLInputElement
  );
  const select = document.getElementById("theme-toggle-select");
  const toggle = document.getElementById("theme-toggle");
  const hasRadios = radios.length > 0;
  const hasSelect = select instanceof HTMLSelectElement;
  const hasToggleButton = toggle instanceof HTMLButtonElement;
  if (!hasRadios && !hasSelect && !hasToggleButton) {
    return;
  }

  const cycle = ["auto", "light", "dark"];
  const isKnown = (value) => value === "auto" || value === "light" || value === "dark";

  function currentSetting() {
    const attr = root.getAttribute("data-theme-setting");
    if (isKnown(attr)) {
      return attr;
    }
    return "auto";
  }

  function apply(setting, persist) {
    const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    const resolved = setting === "auto" ? (prefersDark ? "dark" : "light") : setting;
    const label = `${setting[0].toUpperCase()}${setting.slice(1)}`;

    root.setAttribute("data-theme-setting", setting);
    root.setAttribute("data-theme", resolved);
    if (hasRadios) {
      radios.forEach((radio) => {
        radio.checked = radio.value === setting;
      });
    }
    if (hasSelect) {
      select.value = setting;
      select.setAttribute("aria-label", `Theme mode (current: ${label})`);
    }
    if (hasToggleButton) {
      toggle.textContent = label;
      toggle.setAttribute("aria-label", `Switch color theme (current: ${label})`);
    }
    if (persist) {
      localStorage.setItem(key, setting);
    }
  }

  if (hasRadios) {
    radios.forEach((radio) => {
      radio.addEventListener("change", () => {
        if (!radio.checked) {
          return;
        }
        const value = radio.value;
        if (!isKnown(value)) {
          return;
        }
        apply(value, true);
      });
    });
  }
  if (hasSelect) {
    select.addEventListener("change", () => {
      const value = select.value;
      if (!isKnown(value)) {
        return;
      }
      apply(value, true);
    });
  }
  if (hasToggleButton) {
    toggle.addEventListener("click", () => {
      const idx = cycle.indexOf(currentSetting());
      const next = cycle[(idx + 1) % cycle.length];
      apply(next, true);
    });
  }

  const media = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;
  if (media) {
    media.addEventListener("change", () => {
      if (currentSetting() === "auto") {
        apply("auto", false);
      }
    });
  }

  apply(currentSetting(), false);
})();

(() => {
  const selectors = [
    "details.series-create-disclosure",
    "details.series-edit-disclosure",
    "details.action-menu-disclosure",
    "details.post-content-settings-popover",
  ];
  const all = () => Array.from(document.querySelectorAll(selectors.join(",")));

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Node)) {
      return;
    }
    for (const details of all()) {
      if (details.hasAttribute("open") && !details.contains(target)) {
        details.removeAttribute("open");
      }
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }
    for (const details of all()) {
      if (details.hasAttribute("open")) {
        details.removeAttribute("open");
      }
    }
  });
})();

(() => {
  const isKnownMode = (value) => value === "classic" || value === "reader" || value === "gallery";
  const postLinkSelector = "a[data-post-detail-link]";

  const readPreferredPostMode = (creatorId) => {
    const normalizedCreatorId = typeof creatorId === "string" ? creatorId.trim() : "";
    if (!normalizedCreatorId) {
      return null;
    }
    const key = `kemono-post-view-mode:creator:${normalizedCreatorId}`;
    try {
      const value = window.localStorage.getItem(key);
      return isKnownMode(value) ? value : null;
    } catch {
      return null;
    }
  };

  const withPreferredView = (href, creatorId) => {
    if (typeof href !== "string" || !href.trim()) {
      return href;
    }
    const preferredMode = readPreferredPostMode(creatorId);
    if (!preferredMode || preferredMode === "classic") {
      return href;
    }
    try {
      const url = new URL(href, window.location.href);
      if (!/^\/posts\/\d+/.test(url.pathname)) {
        return href;
      }
      if (url.searchParams.has("view")) {
        return href;
      }
      url.searchParams.set("view", preferredMode);
      return url.origin === window.location.origin ? `${url.pathname}${url.search}${url.hash}` : url.toString();
    } catch {
      return href;
    }
  };

  const rewritePostLinks = () => {
    const links = Array.from(document.querySelectorAll(postLinkSelector));
    links.forEach((node) => {
      if (!(node instanceof HTMLAnchorElement)) {
        return;
      }
      const creatorId = node.dataset.postCreatorId || "";
      const rewritten = withPreferredView(node.getAttribute("href") || "", creatorId);
      if (rewritten && rewritten !== node.getAttribute("href")) {
        node.setAttribute("href", rewritten);
      }
    });
  };

  rewritePostLinks();

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const link = target.closest(postLinkSelector);
    if (!(link instanceof HTMLAnchorElement)) {
      return;
    }
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return;
    }
    const creatorId = link.dataset.postCreatorId || "";
    const rewritten = withPreferredView(link.getAttribute("href") || "", creatorId);
    if (rewritten && rewritten !== link.getAttribute("href")) {
      link.setAttribute("href", rewritten);
    }
  });
})();
