(() => {
  const form = document.getElementById("file-selection-form");
  if (!form) {
    return;
  }

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
