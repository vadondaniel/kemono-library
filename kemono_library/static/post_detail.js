(() => {
  const lightbox = document.querySelector("[data-post-lightbox]");
  if (!(lightbox instanceof HTMLElement)) {
    return;
  }

  const dialog = lightbox.querySelector(".post-lightbox-dialog");
  const imageTarget = lightbox.querySelector("[data-post-lightbox-image]");
  const captionTarget = lightbox.querySelector("[data-post-lightbox-caption]");
  const openTabLink = lightbox.querySelector("[data-post-lightbox-open-tab]");
  const closeButtons = Array.from(lightbox.querySelectorAll("[data-post-lightbox-close]"));
  const contentRoot = document.querySelector(".post-content");
  const fileTriggers = Array.from(document.querySelectorAll("[data-post-file-image-trigger]"));

  if (!(imageTarget instanceof HTMLImageElement)) {
    return;
  }

  function openLightbox(src, caption) {
    if (typeof src !== "string" || !src.trim()) {
      return;
    }
    const cleanSrc = src.trim();
    const cleanCaption = typeof caption === "string" ? caption.trim() : "";

    imageTarget.src = cleanSrc;
    imageTarget.alt = cleanCaption || "Image preview";
    if (captionTarget instanceof HTMLElement) {
      captionTarget.textContent = cleanCaption;
    }
    if (openTabLink instanceof HTMLAnchorElement) {
      openTabLink.href = cleanSrc;
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
    if (openTabLink instanceof HTMLAnchorElement) {
      openTabLink.removeAttribute("href");
    }
    document.body.classList.remove("is-lightbox-open");
  }

  fileTriggers.forEach((trigger) => {
    trigger.addEventListener("click", (event) => {
      event.preventDefault();
      if (!(trigger instanceof HTMLElement)) {
        return;
      }
      openLightbox(trigger.dataset.lightboxSrc || "", trigger.dataset.lightboxTitle || "");
    });
  });

  if (contentRoot instanceof HTMLElement) {
    contentRoot.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      const anchor = target.closest("a.post-image-link");
      if (anchor instanceof HTMLAnchorElement) {
        const nestedImage = anchor.querySelector("img");
        const src = anchor.getAttribute("href") || nestedImage?.getAttribute("src") || "";
        const caption = nestedImage?.getAttribute("alt") || "";
        if (!src) {
          return;
        }
        event.preventDefault();
        openLightbox(src, caption);
        return;
      }

      const image = target.closest("img");
      if (!(image instanceof HTMLImageElement) || !contentRoot.contains(image)) {
        return;
      }
      const src = image.getAttribute("src") || "";
      if (!src) {
        return;
      }
      event.preventDefault();
      openLightbox(src, image.getAttribute("alt") || "");
    });
  }

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
    if (event.key === "Escape" && !lightbox.hidden) {
      closeLightbox();
    }
  });
})();
