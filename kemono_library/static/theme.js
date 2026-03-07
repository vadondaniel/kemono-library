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
