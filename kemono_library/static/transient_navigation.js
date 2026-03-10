(() => {
  function buildFormData(form, submitter) {
    if (submitter && submitter instanceof HTMLElement) {
      try {
        return new FormData(form, submitter);
      } catch (_error) {
        // Older browsers may not support FormData(form, submitter).
      }
    }
    return new FormData(form);
  }

  async function submitWithReplace(form, submitter) {
    const method = (form.getAttribute("method") || "GET").trim().toUpperCase();
    const action = form.getAttribute("action") || window.location.href;
    const targetUrl = new URL(action, window.location.href);
    const formData = buildFormData(form, submitter);
    let fetchUrl = targetUrl.toString();
    let body;

    if (method === "GET") {
      const params = new URLSearchParams(formData);
      const query = params.toString();
      fetchUrl = query ? `${targetUrl.pathname}?${query}` : targetUrl.pathname;
      body = undefined;
    } else {
      body = formData;
    }

    const response = await fetch(fetchUrl, {
      method,
      body,
      credentials: "same-origin",
      redirect: "follow",
      headers: {
        Accept: "text/html,application/xhtml+xml",
        "X-Requested-With": "XMLHttpRequest",
      },
      cache: "no-store",
    });

    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      const payload = await response.json().catch(() => ({}));
      const redirectUrl = payload && typeof payload.redirect_url === "string" ? payload.redirect_url : "";
      if (redirectUrl) {
        window.location.replace(redirectUrl);
        return;
      }
    }

    const destination = response.url || fetchUrl;
    window.location.replace(destination);
  }

  document.addEventListener("submit", (event) => {
    if (event.defaultPrevented) {
      return;
    }
    const target = event.target;
    if (!(target instanceof HTMLFormElement)) {
      return;
    }
    if (!target.hasAttribute("data-nav-replace-redirect")) {
      return;
    }
    if (target.dataset.navSubmitting === "1") {
      event.preventDefault();
      return;
    }

    const submitter = event.submitter;
    target.dataset.navSubmitting = "1";
    if (submitter instanceof HTMLButtonElement || submitter instanceof HTMLInputElement) {
      submitter.disabled = true;
    }
    event.preventDefault();

    submitWithReplace(target, submitter)
      .catch(() => {
        target.removeAttribute("data-nav-submitting");
        if (submitter instanceof HTMLButtonElement || submitter instanceof HTMLInputElement) {
          submitter.disabled = false;
        }
        target.submit();
      });
  });

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const link = target.closest("a[data-nav-replace-href]");
    if (!(link instanceof HTMLAnchorElement)) {
      return;
    }
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return;
    }
    if (link.target && link.target !== "_self") {
      return;
    }
    if (!link.href || link.hasAttribute("download")) {
      return;
    }
    event.preventDefault();
    window.location.replace(link.href);
  });
})();
