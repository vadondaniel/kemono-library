(() => {
  const key = "kemono-theme";
  const root = document.documentElement;
  const toggle = document.getElementById("theme-toggle");
  if (!toggle) {
    return;
  }

  const cycle = ["auto", "light", "dark"];

  function currentSetting() {
    const attr = root.getAttribute("data-theme-setting");
    if (attr === "light" || attr === "dark" || attr === "auto") {
      return attr;
    }
    return "auto";
  }

  function apply(setting, persist) {
    const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    const resolved = setting === "auto" ? (prefersDark ? "dark" : "light") : setting;

    root.setAttribute("data-theme-setting", setting);
    root.setAttribute("data-theme", resolved);
    toggle.textContent = `Theme: ${setting[0].toUpperCase()}${setting.slice(1)}`;
    if (persist) {
      localStorage.setItem(key, setting);
    }
  }

  toggle.addEventListener("click", () => {
    const idx = cycle.indexOf(currentSetting());
    const next = cycle[(idx + 1) % cycle.length];
    apply(next, true);
  });

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
