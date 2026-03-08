(() => {
  const sourceSelect = document.getElementById("thumbnail_attachment_id");
  const stage = document.querySelector("[data-thumb-stage]");
  const previewImage = document.querySelector("[data-thumb-image]");
  const emptyState = document.querySelector("[data-thumb-empty]");
  const readout = document.querySelector("[data-thumb-readout]");
  const xInput = document.querySelector("[data-thumb-focus-x-input]");
  const yInput = document.querySelector("[data-thumb-focus-y-input]");

  if (
    !(sourceSelect instanceof HTMLSelectElement) ||
    !(stage instanceof HTMLElement) ||
    !(previewImage instanceof HTMLImageElement) ||
    !(emptyState instanceof HTMLElement) ||
    !(xInput instanceof HTMLInputElement) ||
    !(yInput instanceof HTMLInputElement)
  ) {
    return;
  }

  function clamp(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return 50;
    }
    return Math.max(0, Math.min(100, numeric));
  }

  /** @type {"x" | "y" | "none"} */
  let activeAxis = "none";
  let focusX = clamp(xInput.value);
  let focusY = clamp(yInput.value);

  /** @type {{ startX: number; startY: number; startFocusX: number; startFocusY: number } | null} */
  let dragState = null;

  function setAxisMode(nextAxis) {
    activeAxis = nextAxis;
    stage.classList.toggle("axis-x", nextAxis === "x");
    stage.classList.toggle("axis-y", nextAxis === "y");
    stage.classList.toggle("axis-none", nextAxis === "none");
  }

  function normalizeToAxis(x, y) {
    const nextX = clamp(x);
    const nextY = clamp(y);
    if (activeAxis === "x") {
      return { x: nextX, y: 50 };
    }
    if (activeAxis === "y") {
      return { x: 50, y: nextY };
    }
    return { x: 50, y: 50 };
  }

  function updateReadout() {
    if (!(readout instanceof HTMLElement)) {
      return;
    }
    if (previewImage.hidden) {
      readout.textContent = "No thumbnail selected.";
      return;
    }
    if (activeAxis === "x") {
      readout.textContent = `Drag to pan horizontally (${focusX.toFixed(1)}%).`;
      return;
    }
    if (activeAxis === "y") {
      readout.textContent = `Drag to pan vertically (${focusY.toFixed(1)}%).`;
      return;
    }
    readout.textContent = "This image already matches the preview ratio.";
  }

  function renderFocus() {
    previewImage.style.objectPosition = `${focusX}% ${focusY}%`;
    xInput.value = focusX.toFixed(2);
    yInput.value = focusY.toFixed(2);
    updateReadout();
  }

  function setFocus(x, y) {
    const normalized = normalizeToAxis(x, y);
    focusX = normalized.x;
    focusY = normalized.y;
    renderFocus();
  }

  function setPreviewUrl(url) {
    const cleaned = typeof url === "string" ? url.trim() : "";
    if (!cleaned) {
      previewImage.hidden = true;
      emptyState.hidden = false;
      stage.classList.add("is-empty");
      setAxisMode("none");
      setFocus(50, 50);
      return;
    }

    if (previewImage.getAttribute("src") !== cleaned) {
      previewImage.setAttribute("src", cleaned);
    }
    previewImage.hidden = false;
    emptyState.hidden = true;
    stage.classList.remove("is-empty");
  }

  function updateAxisFromImage() {
    if (previewImage.hidden) {
      setAxisMode("none");
      setFocus(50, 50);
      return;
    }

    const rect = stage.getBoundingClientRect();
    const stageWidth = rect.width;
    const stageHeight = rect.height;
    const imageWidth = previewImage.naturalWidth;
    const imageHeight = previewImage.naturalHeight;
    if (stageWidth <= 0 || stageHeight <= 0 || imageWidth <= 0 || imageHeight <= 0) {
      setAxisMode("none");
      setFocus(50, 50);
      return;
    }

    const targetRatio = stageWidth / stageHeight;
    const imageRatio = imageWidth / imageHeight;
    const tolerance = 0.01;
    if (imageRatio > targetRatio + tolerance) {
      setAxisMode("x");
    } else if (imageRatio < targetRatio - tolerance) {
      setAxisMode("y");
    } else {
      setAxisMode("none");
    }
    setFocus(focusX, focusY);
  }

  function refreshFromSelect() {
    const selected = sourceSelect.options[sourceSelect.selectedIndex];
    const previewUrl = selected ? selected.dataset.previewUrl || "" : "";
    setPreviewUrl(previewUrl);
    if (previewImage.complete) {
      updateAxisFromImage();
    }
  }

  function beginDrag(startClientX, startClientY) {
    if (previewImage.hidden || activeAxis === "none") {
      return false;
    }
    dragState = {
      startX: startClientX,
      startY: startClientY,
      startFocusX: focusX,
      startFocusY: focusY,
    };
    stage.classList.add("is-dragging");
    return true;
  }

  function updateDrag(clientX, clientY) {
    if (!dragState || activeAxis === "none") {
      return;
    }
    const rect = stage.getBoundingClientRect();
    if (rect.width <= 0 || rect.height <= 0) {
      return;
    }

    const deltaX = ((clientX - dragState.startX) / rect.width) * 100;
    const deltaY = ((clientY - dragState.startY) / rect.height) * 100;

    if (activeAxis === "x") {
      setFocus(dragState.startFocusX - deltaX, dragState.startFocusY);
      return;
    }
    setFocus(dragState.startFocusX, dragState.startFocusY - deltaY);
  }

  function endDrag() {
    dragState = null;
    stage.classList.remove("is-dragging");
  }

  sourceSelect.addEventListener("change", refreshFromSelect);
  previewImage.addEventListener("load", updateAxisFromImage);
  window.addEventListener("resize", updateAxisFromImage);

  if ("PointerEvent" in window) {
    let activePointerId = null;
    stage.addEventListener("pointerdown", (event) => {
      if (!beginDrag(event.clientX, event.clientY)) {
        return;
      }
      activePointerId = event.pointerId;
      event.preventDefault();
    });
    window.addEventListener("pointermove", (event) => {
      if (activePointerId !== event.pointerId) {
        return;
      }
      updateDrag(event.clientX, event.clientY);
    });
    const clearPointer = (event) => {
      if (activePointerId === event.pointerId) {
        activePointerId = null;
        endDrag();
      }
    };
    window.addEventListener("pointerup", clearPointer);
    window.addEventListener("pointercancel", clearPointer);
  } else {
    let mouseDragging = false;
    stage.addEventListener("mousedown", (event) => {
      mouseDragging = beginDrag(event.clientX, event.clientY);
      if (mouseDragging) {
        event.preventDefault();
      }
    });
    window.addEventListener("mousemove", (event) => {
      if (!mouseDragging) {
        return;
      }
      updateDrag(event.clientX, event.clientY);
    });
    window.addEventListener("mouseup", () => {
      mouseDragging = false;
      endDrag();
    });

    let touchDragging = false;
    stage.addEventListener(
      "touchstart",
      (event) => {
        if (!event.touches.length) {
          return;
        }
        const touch = event.touches[0];
        touchDragging = beginDrag(touch.clientX, touch.clientY);
        if (touchDragging) {
          event.preventDefault();
        }
      },
      { passive: false }
    );
    window.addEventListener(
      "touchmove",
      (event) => {
        if (!touchDragging || !event.touches.length) {
          return;
        }
        const touch = event.touches[0];
        updateDrag(touch.clientX, touch.clientY);
        event.preventDefault();
      },
      { passive: false }
    );
    window.addEventListener("touchend", () => {
      touchDragging = false;
      endDrag();
    });
    window.addEventListener("touchcancel", () => {
      touchDragging = false;
      endDrag();
    });
  }

  refreshFromSelect();
  if (previewImage.complete) {
    updateAxisFromImage();
  } else {
    setFocus(focusX, focusY);
  }
})();

(() => {
  const lightbox = document.querySelector("[data-post-edit-lightbox]");
  if (!(lightbox instanceof HTMLElement)) {
    return;
  }

  const triggers = Array.from(document.querySelectorAll("[data-lightbox-image]"));
  if (!triggers.length) {
    return;
  }

  const imageTarget = lightbox.querySelector("[data-lightbox-image-target]");
  const captionTarget = lightbox.querySelector("[data-lightbox-caption]");
  const dialog = lightbox.querySelector(".post-edit-lightbox-dialog");
  const closeButtons = Array.from(lightbox.querySelectorAll("[data-lightbox-close]"));
  if (!(imageTarget instanceof HTMLImageElement)) {
    return;
  }

  function openLightbox(src, alt) {
    imageTarget.src = src;
    imageTarget.alt = alt || "";
    if (captionTarget instanceof HTMLElement) {
      captionTarget.textContent = alt || "";
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
    document.body.classList.remove("is-lightbox-open");
  }

  triggers.forEach((trigger) => {
    trigger.addEventListener("click", () => {
      if (!(trigger instanceof HTMLElement)) {
        return;
      }
      const src = trigger.dataset.lightboxSrc || "";
      const alt = trigger.dataset.lightboxAlt || "";
      if (!src) {
        return;
      }
      openLightbox(src, alt);
    });
  });

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
    if (event.key !== "Escape" || lightbox.hidden) {
      return;
    }
    closeLightbox();
  });
})();
