(() => {
  const tagPopovers = Array.from(document.querySelectorAll(".creator-post-tag-details")).filter(
    (node) => node instanceof HTMLDetailsElement
  );
  if (!tagPopovers.length) {
    return;
  }

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

  const closeAllExcept = (keepOpen) => {
    tagPopovers.forEach((popover) => {
      if (!(popover instanceof HTMLDetailsElement) || popover === keepOpen) {
        return;
      }
      popover.open = false;
      syncCardLayerClass(popover);
    });
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
        closeAllExcept(target);
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
      const clickedInside = tagPopovers.some((popover) => popover.contains(target));
      if (clickedInside) {
        return;
      }
      closeAllExcept(null);
    },
    true
  );

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }
    closeAllExcept(null);
  });

  tagPopovers.forEach((popover) => {
    syncCardLayerClass(popover);
  });
})();
