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
