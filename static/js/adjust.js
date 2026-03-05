(() => {
  const DEBOUNCE_MS = 250;

  const debounce = (fn, wait = DEBOUNCE_MS) => {
    let timerId;
    return (...args) => {
      clearTimeout(timerId);
      timerId = setTimeout(() => fn(...args), wait);
    };
  };

  const setStatus = (el, message, isError = false) => {
    if (!el) return;
    el.textContent = message;
    el.style.color = isError ? "#991b1b" : "var(--text-muted)";
  };

  const updateTable = (headerRow, body, columns, rows) => {
    headerRow.innerHTML = "";
    columns.forEach((col) => {
      const th = document.createElement("th");
      th.scope = "col";
      th.textContent = col;
      headerRow.appendChild(th);
    });

    body.innerHTML = "";
    if (!rows.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = columns.length || 1;
      td.textContent = "No rows available with current selections.";
      tr.appendChild(td);
      body.appendChild(tr);
      return;
    }

    rows.forEach((row) => {
      const tr = document.createElement("tr");
      columns.forEach((column) => {
        const td = document.createElement("td");
        td.textContent =
          row[column] === undefined || row[column] === null ? "" : row[column];
        tr.appendChild(td);
      });
      body.appendChild(tr);
    });
  };

  const collectOverrides = (form) => {
    const dtype = {};
    const mapping = {};

    form.querySelectorAll("[data-dtype]").forEach((select) => {
      dtype[select.dataset.dtype] = select.value;
    });

    form.querySelectorAll("[data-mapping]").forEach((select) => {
      if (select.value) {
        mapping[select.dataset.mapping] = select.value;
      }
    });

    const limit = Number.parseInt(
      form.dataset.previewLimit || form.closest("[data-preview-limit]")?.dataset.previewLimit || "20",
      10,
    );

    return {
      dtype_overrides: dtype,
      name_mapping: mapping,
      limit: Number.isNaN(limit) ? 20 : limit,
    };
  };

  const initAdjustPreview = () => {
    const container = document.querySelector("[data-adjust-container]");
    if (!container) return;

    const form = container.querySelector("[data-preview-form]");
    if (!form) return;

    const previewUrl = container.dataset.previewUrl;
    if (!previewUrl) return;

    const headerRow = container.querySelector("#preview-headers");
    const body = container.querySelector("#preview-body");
    const statusEl = container.querySelector("[data-preview-status]");

    if (!headerRow || !body) return;

    let lastColumns = Array.from(headerRow.querySelectorAll("th")).map((th) =>
      th.textContent.trim(),
    );

    const parseAllowedNames = () => {
      try {
        return JSON.parse(container.dataset.allowedNames || "[]");
      } catch (error) {
        console.warn("Failed to parse allowed names dataset", error);
        return [];
      }
    };

    const locationField = container.dataset.locationField || "location";
    const allowedNames = parseAllowedNames();
    const requiredTargets = allowedNames.filter((name) => name && name !== locationField);
    const nullLocationInput = form.querySelector("[data-null-location-flag]");

    form.addEventListener("submit", (event) => {
      const payload = collectOverrides(form);
      const mappings = payload.name_mapping || {};
      const mappedTargets = new Set(Object.values(mappings));
      const missing = requiredTargets.filter((target) => !mappedTargets.has(target));
      if (missing.length) {
        alert(`Please map a column to: ${missing.join(", ")}.`);
        event.preventDefault();
        return;
      }

      if (!mappedTargets.has(locationField)) {
        const confirmed = window.confirm(
          "No column is mapped to location. Add a location column with null values?",
        );
        if (!confirmed) {
          event.preventDefault();
          if (nullLocationInput) nullLocationInput.value = "";
          return;
        }
        if (nullLocationInput) nullLocationInput.value = "1";
      } else if (nullLocationInput) {
        nullLocationInput.value = "";
      }
    });

    const requestPreview = async () => {
      try {
        setStatus(statusEl, "Refreshing preview…");
        const payload = collectOverrides(form);
        const response = await fetch(previewUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.error || "Preview request failed.");
        }
        if (!Array.isArray(data.rows)) {
          throw new Error("Unexpected response from preview endpoint.");
        }
        let columns = Array.isArray(data.columns) ? data.columns : [];
        if (!columns.length) {
          columns = data.rows.length ? Object.keys(data.rows[0]) : lastColumns;
        }
        if (!columns.length) {
          columns = lastColumns;
        }
        lastColumns = columns;
        updateTable(headerRow, body, columns, data.rows);
        setStatus(statusEl, "Preview updated.");
      } catch (error) {
        console.error(error);
        setStatus(statusEl, error.message || "Failed to refresh preview.", true);
      }
    };

    const debouncedRequest = debounce(requestPreview);

    form.addEventListener("change", () => {
      debouncedRequest();
    });

    // Trigger initial refresh so preview reflects any defaults.
    debouncedRequest();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAdjustPreview);
  } else {
    initAdjustPreview();
  }
})();
