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
    return { x: nextX, y: nextY };
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
    const nextX = focusX.toFixed(2);
    const nextY = focusY.toFixed(2);
    const xChanged = xInput.value !== nextX;
    const yChanged = yInput.value !== nextY;
    xInput.value = nextX;
    yInput.value = nextY;
    if (xChanged) {
      xInput.dispatchEvent(new Event("input", { bubbles: true }));
    }
    if (yChanged) {
      yInput.dispatchEvent(new Event("input", { bubbles: true }));
    }
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
      updateReadout();
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
      updateReadout();
      return;
    }

    const rect = stage.getBoundingClientRect();
    const stageWidth = rect.width;
    const stageHeight = rect.height;
    const imageWidth = previewImage.naturalWidth;
    const imageHeight = previewImage.naturalHeight;
    if (stageWidth <= 0 || stageHeight <= 0 || imageWidth <= 0 || imageHeight <= 0) {
      setAxisMode("none");
      updateReadout();
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
    renderFocus();
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
  const openTabLink = lightbox.querySelector("[data-lightbox-open-tab]");
  const dialog = lightbox.querySelector(".post-edit-lightbox-dialog");
  const closeButtons = Array.from(lightbox.querySelectorAll("[data-lightbox-close]"));
  if (!(imageTarget instanceof HTMLImageElement)) {
    return;
  }

  function measureTextWidth(element, value) {
    if (!(element instanceof HTMLElement)) {
      return value.length * 8;
    }
    const style = window.getComputedStyle(element);
    const font = style.font && style.font !== "normal normal normal normal 16px / normal sans-serif"
      ? style.font
      : `${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
    const canvas = document.createElement("canvas");
    const context = canvas.getContext("2d");
    if (!context) {
      return value.length * 8;
    }
    context.font = font;
    return context.measureText(value).width;
  }

  function middleEllipsizeToFit(element, value) {
    const available = Math.max(0, Math.floor(element.clientWidth || element.getBoundingClientRect().width));
    if (available <= 0 || !value) {
      return value;
    }
    if (measureTextWidth(element, value) <= available) {
      return value;
    }

    const dotIndex = value.lastIndexOf(".");
    const likelyExtension =
      dotIndex > 0 && dotIndex < value.length - 1 && value.length - dotIndex <= 9
        ? value.slice(dotIndex)
        : "";
    const minRight = likelyExtension ? Math.max(8, likelyExtension.length + 5) : 8;

    let low = 6;
    let high = Math.max(6, value.length - 4);
    let best = "";
    while (low <= high) {
      const keep = Math.floor((low + high) / 2);
      let right = Math.max(minRight, Math.round(keep * 0.4));
      right = Math.min(right, Math.max(4, value.length - 5));
      let left = keep - right;
      if (left < 3) {
        left = 3;
      }
      if (left + right + 3 > value.length) {
        right = Math.max(4, value.length - left - 3);
      }
      const candidate = `${value.slice(0, left)}...${value.slice(value.length - right)}`;
      if (measureTextWidth(element, candidate) <= available) {
        best = candidate;
        low = keep + 1;
      } else {
        high = keep - 1;
      }
    }
    if (best) {
      return best;
    }
    return `${value.slice(0, 3)}...${value.slice(-Math.max(4, minRight))}`;
  }

  function applyMiddleEllipsis(element) {
    if (!(element instanceof HTMLElement)) {
      return;
    }
    const fullText = (element.dataset.fullText || element.textContent || "").trim();
    if (!fullText) {
      return;
    }
    element.dataset.fullText = fullText;
    element.setAttribute("title", fullText);
    element.textContent = middleEllipsizeToFit(element, fullText);
  }

  function syncLightboxCaptionWidth() {
    if (!(captionTarget instanceof HTMLElement)) {
      return;
    }
    const imageWidth = Math.floor(imageTarget.getBoundingClientRect().width);
    if (imageWidth > 0) {
      captionTarget.style.width = `${imageWidth}px`;
      captionTarget.style.maxWidth = `${imageWidth}px`;
      return;
    }
    captionTarget.style.removeProperty("width");
    captionTarget.style.removeProperty("max-width");
  }

  function openLightbox(src, alt) {
    const cleanSrc = typeof src === "string" ? src.trim() : "";
    const cleanAlt = typeof alt === "string" ? alt : "";
    if (!cleanSrc) {
      return;
    }

    imageTarget.src = cleanSrc;
    imageTarget.alt = cleanAlt || "";
    if (captionTarget instanceof HTMLElement) {
      captionTarget.textContent = cleanAlt || "";
      captionTarget.dataset.fullText = cleanAlt || "";
    }
    if (openTabLink instanceof HTMLAnchorElement) {
      openTabLink.href = cleanSrc;
    }
    lightbox.hidden = false;
    document.body.classList.add("is-lightbox-open");
    if (captionTarget instanceof HTMLElement) {
      window.requestAnimationFrame(() => {
        syncLightboxCaptionWidth();
        applyMiddleEllipsis(captionTarget);
      });
    }
  }

  function closeLightbox() {
    lightbox.hidden = true;
    imageTarget.removeAttribute("src");
    imageTarget.alt = "";
    if (captionTarget instanceof HTMLElement) {
      captionTarget.textContent = "";
      delete captionTarget.dataset.fullText;
      captionTarget.removeAttribute("title");
      captionTarget.style.removeProperty("width");
      captionTarget.style.removeProperty("max-width");
    }
    if (openTabLink instanceof HTMLAnchorElement) {
      openTabLink.removeAttribute("href");
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

  imageTarget.addEventListener("load", () => {
    if (lightbox.hidden) {
      return;
    }
    syncLightboxCaptionWidth();
    if (captionTarget instanceof HTMLElement) {
      applyMiddleEllipsis(captionTarget);
    }
  });

  window.addEventListener("resize", () => {
    if (lightbox.hidden || !(captionTarget instanceof HTMLElement)) {
      return;
    }
    syncLightboxCaptionWidth();
    applyMiddleEllipsis(captionTarget);
  });
})();

(() => {
  const page = document.querySelector("[data-post-edit-page]");
  const form = document.getElementById("post-edit-main-form");
  if (!(page instanceof HTMLElement) || !(form instanceof HTMLFormElement)) {
    return;
  }

  const statusNodes = Array.from(document.querySelectorAll("[data-post-edit-save-state]")).filter(
    (node) => node instanceof HTMLElement
  );
  const primarySaveButton = document.querySelector("[data-post-edit-primary-save]");
  const contentField = document.getElementById("content");
  const previewToggle = document.querySelector("[data-post-edit-preview-toggle]");
  const previewRoot = document.querySelector("[data-post-edit-preview]");
  const previewFrame = document.querySelector("[data-post-edit-preview-frame]");
  const sectionJumpLinks = Array.from(document.querySelectorAll("[data-post-edit-jump]")).filter(
    (node) => node instanceof HTMLAnchorElement
  );

  const formControlSet = new Set();
  Array.from(form.querySelectorAll("input, textarea, select")).forEach((node) => {
    if (node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement || node instanceof HTMLSelectElement) {
      formControlSet.add(node);
    }
  });
  const externalControls = Array.from(
    document.querySelectorAll("input[form='post-edit-main-form'], textarea[form='post-edit-main-form'], select[form='post-edit-main-form']")
  );
  externalControls.forEach((node) => {
    if (node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement || node instanceof HTMLSelectElement) {
      formControlSet.add(node);
    }
  });
  const trackedControls = Array.from(formControlSet);
  if (!trackedControls.length) {
    return;
  }

  const baseValues = new Map();
  const confirmLeaveMessage = "You have unsaved changes. Leave without saving?";
  let dirty = false;
  let saving = false;
  let navigationBypass = false;
  let bypassTimeout = null;
  let previewVisible = false;
  let previewDebounce = null;

  const setSaveState = (state) => {
    const normalized = state === "saving" || state === "dirty" ? state : "saved";
    const text = normalized === "saving" ? "Saving..." : normalized === "dirty" ? "Unsaved changes" : "Saved";
    statusNodes.forEach((node) => {
      if (!(node instanceof HTMLElement)) {
        return;
      }
      node.dataset.saveState = normalized;
      node.textContent = text;
    });
  };

  const clearNavigationBypassSoon = () => {
    if (bypassTimeout !== null) {
      window.clearTimeout(bypassTimeout);
    }
    bypassTimeout = window.setTimeout(() => {
      bypassTimeout = null;
      navigationBypass = false;
      if (saving) {
        saving = false;
        setSaveState(dirty ? "dirty" : "saved");
      }
    }, 5000);
  };

  const markNavigationBypass = () => {
    navigationBypass = true;
    clearNavigationBypassSoon();
  };

  const getControlValue = (control) => {
    if (control instanceof HTMLInputElement) {
      const type = control.type.toLowerCase();
      if (type === "checkbox" || type === "radio") {
        return control.checked ? "1" : "0";
      }
      if (control.name === "thumbnail_focus_x" || control.name === "thumbnail_focus_y") {
        const numeric = Number(control.value);
        if (Number.isFinite(numeric)) {
          return `num:${numeric.toFixed(2)}`;
        }
      }
      return control.value;
    }
    if (control instanceof HTMLTextAreaElement) {
      return control.value;
    }
    if (control instanceof HTMLSelectElement) {
      if (control.multiple) {
        return Array.from(control.selectedOptions)
          .map((option) => option.value)
          .join("\u001f");
      }
      return control.value;
    }
    return "";
  };

  trackedControls.forEach((control) => {
    baseValues.set(control, getControlValue(control));
  });

  const recomputeDirty = () => {
    dirty = trackedControls.some((control) => baseValues.get(control) !== getControlValue(control));
    if (!saving) {
      setSaveState(dirty ? "dirty" : "saved");
    }
  };

  const attachDirtyListeners = () => {
    const onControlChange = () => {
      if (!saving && navigationBypass) {
        navigationBypass = false;
      }
      recomputeDirty();
    };
    trackedControls.forEach((control) => {
      control.addEventListener("input", onControlChange);
      control.addEventListener("change", onControlChange);
    });
  };

  const maybeWarnBeforeLeaving = () => {
    if (!dirty || navigationBypass || saving) {
      return true;
    }
    return window.confirm(confirmLeaveMessage);
  };

  form.addEventListener("submit", () => {
    saving = true;
    setSaveState("saving");
    markNavigationBypass();
  });

  document.addEventListener(
    "submit",
    (event) => {
      const target = event.target;
      if (!(target instanceof HTMLFormElement) || target === form) {
        return;
      }
      if (maybeWarnBeforeLeaving()) {
        markNavigationBypass();
        return;
      }
      event.preventDefault();
      event.stopImmediatePropagation();
    },
    true
  );

  document.addEventListener(
    "click",
    (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const link = target.closest("a[href]");
      if (!(link instanceof HTMLAnchorElement)) {
        return;
      }
      if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
        return;
      }
      if (link.target && link.target !== "_self") {
        return;
      }
      const rawHref = link.getAttribute("href") || "";
      if (!rawHref || rawHref.startsWith("#") || link.hasAttribute("download")) {
        return;
      }
      if (!maybeWarnBeforeLeaving()) {
        event.preventDefault();
        event.stopImmediatePropagation();
        return;
      }
      markNavigationBypass();
    },
    true
  );

  window.addEventListener("beforeunload", (event) => {
    if (!dirty || navigationBypass || saving) {
      return;
    }
    event.preventDefault();
    event.returnValue = "";
  });

  window.addEventListener("keydown", (event) => {
    const key = typeof event.key === "string" ? event.key.toLowerCase() : "";
    if ((event.ctrlKey || event.metaKey) && key === "s") {
      event.preventDefault();
      if (typeof form.requestSubmit === "function") {
        if (primarySaveButton instanceof HTMLButtonElement) {
          form.requestSubmit(primarySaveButton);
          return;
        }
        form.requestSubmit();
        return;
      }
      form.submit();
    }
  });

  const escapeHtml = (value) =>
    value
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");

  const renderPreview = () => {
    if (!(contentField instanceof HTMLTextAreaElement) || !(previewFrame instanceof HTMLIFrameElement) || !previewVisible) {
      return;
    }
    const raw = contentField.value || "";
    const looksLikeHtml = /<[a-z!/?][\s\S]*>/i.test(raw);
    const bodyMarkup = looksLikeHtml ? raw : `<pre>${escapeHtml(raw)}</pre>`;
    const doc = [
      "<!doctype html>",
      "<html lang='en'>",
      "<head>",
      "<meta charset='utf-8'>",
      "<meta name='viewport' content='width=device-width, initial-scale=1'>",
      "<style>",
      "body{margin:0;padding:1rem 1.1rem;font-family:Georgia,'Times New Roman',serif;line-height:1.62;color:#1f1b17;background:#f4f2ef;}",
      "img,video,iframe{max-width:100%;height:auto;}",
      "pre{white-space:pre-wrap;word-break:break-word;font-family:'Cascadia Mono','Consolas',monospace;background:#ece6de;padding:0.78rem;border-radius:0.55rem;}",
      "a{color:#5f3f1f;}",
      "</style>",
      "</head>",
      "<body>",
      bodyMarkup,
      "</body>",
      "</html>",
    ].join("");
    previewFrame.srcdoc = doc;
  };

  const schedulePreviewRender = () => {
    if (previewDebounce !== null) {
      window.clearTimeout(previewDebounce);
    }
    previewDebounce = window.setTimeout(() => {
      previewDebounce = null;
      renderPreview();
    }, 170);
  };

  const syncPreviewToggle = () => {
    if (!(previewToggle instanceof HTMLButtonElement) || !(previewRoot instanceof HTMLElement)) {
      return;
    }
    previewRoot.hidden = !previewVisible;
    previewToggle.setAttribute("aria-expanded", previewVisible ? "true" : "false");
    previewToggle.textContent = previewVisible ? "Hide preview" : "Show preview";
  };

  if (previewToggle instanceof HTMLButtonElement && previewRoot instanceof HTMLElement) {
    previewToggle.addEventListener("click", () => {
      previewVisible = !previewVisible;
      syncPreviewToggle();
      if (previewVisible) {
        renderPreview();
      }
    });
    if (contentField instanceof HTMLTextAreaElement) {
      contentField.addEventListener("input", schedulePreviewRender);
    }
    syncPreviewToggle();
  }

  sectionJumpLinks.forEach((jumpLink) => {
    jumpLink.addEventListener("click", (event) => {
      const targetId = jumpLink.getAttribute("href") || "";
      if (!targetId.startsWith("#")) {
        return;
      }
      const section = document.querySelector(targetId);
      if (!(section instanceof HTMLElement)) {
        return;
      }
      event.preventDefault();
      section.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });

  const attachmentGroups = Array.from(document.querySelectorAll("[data-post-edit-attachment-group]")).filter(
    (node) => node instanceof HTMLElement
  );
  attachmentGroups.forEach((groupNode) => {
    if (!(groupNode instanceof HTMLElement)) {
      return;
    }
    const items = Array.from(groupNode.querySelectorAll("[data-post-edit-attachment-item]")).filter(
      (node) => node instanceof HTMLElement
    );
    if (!items.length) {
      return;
    }
    const filterInput = groupNode.querySelector("[data-post-edit-attachment-filter]");
    const bulkButtons = Array.from(groupNode.querySelectorAll("[data-post-edit-attachment-bulk]")).filter(
      (node) => node instanceof HTMLButtonElement
    );
    const emptyNote = groupNode.querySelector("[data-post-edit-filter-empty]");

    const applyFilter = () => {
      const rawQuery =
        filterInput instanceof HTMLInputElement || filterInput instanceof HTMLTextAreaElement ? filterInput.value : "";
      const query = rawQuery.trim().toLowerCase();
      let visibleCount = 0;
      items.forEach((itemNode) => {
        if (!(itemNode instanceof HTMLElement)) {
          return;
        }
        const haystack = (itemNode.dataset.attachmentSearch || itemNode.textContent || "").toLowerCase();
        const visible = !query || haystack.includes(query);
        itemNode.hidden = !visible;
        if (visible) {
          visibleCount += 1;
        }
      });
      if (emptyNote instanceof HTMLElement) {
        emptyNote.hidden = visibleCount !== 0;
      }
    };

    if (filterInput instanceof HTMLInputElement || filterInput instanceof HTMLTextAreaElement) {
      filterInput.addEventListener("input", applyFilter);
    }

    bulkButtons.forEach((buttonNode) => {
      buttonNode.addEventListener("click", () => {
        const mode = buttonNode.dataset.postEditAttachmentBulk;
        const nextChecked = mode === "all";
        items.forEach((itemNode) => {
          if (!(itemNode instanceof HTMLElement) || itemNode.hidden) {
            return;
          }
          const toggle = itemNode.querySelector("[data-post-edit-attachment-toggle]");
          if (!(toggle instanceof HTMLInputElement) || toggle.type.toLowerCase() !== "checkbox") {
            return;
          }
          if (toggle.checked === nextChecked) {
            return;
          }
          toggle.checked = nextChecked;
          toggle.dispatchEvent(new Event("change", { bubbles: true }));
        });
      });
    });

    applyFilter();
  });

  attachDirtyListeners();
  recomputeDirty();
})();
