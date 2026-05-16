function normalizeGithubUrl(raw) {
  try {
    const url = new URL(raw);
    const host = url.hostname.toLowerCase();
    if (host !== "github.com") return { url: raw, note: "" };
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts.length >= 5 && parts[2] === "blob") {
      const owner = parts[0];
      const repo = parts[1];
      const branch = parts[3];
      const rest = parts.slice(4).join("/");
      return {
        url: `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/${rest}`,
        note: "لینک گیتهاب به raw تبدیل شد."
      };
    }
    if (parts.length >= 4 && parts[2] === "releases" && parts[3] === "tag") {
      return {
        url: raw,
        note: "برای دانلود مستقیم از Release، لینک asset را از بخش Assets بردارید."
      };
    }
    return { url: raw, note: "" };
  } catch {
    return { url: raw, note: "" };
  }
}

function buildDownloadUrl(target, filename) {
  const u = new URL("/api/download", window.location.origin);
  u.searchParams.set("url", target);
  if (filename) u.searchParams.set("filename", filename);
  return u.toString();
}

function copyToClipboard(text) {
  if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
    return navigator.clipboard.writeText(text);
  }
  const ta = document.createElement("textarea");
  ta.value = text;
  ta.style.position = "fixed";
  ta.style.top = "-1000px";
  document.body.appendChild(ta);
  ta.focus();
  ta.select();
  document.execCommand("copy");
  document.body.removeChild(ta);
  return Promise.resolve();
}

document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("downloadForm");
  const inputUrl = document.getElementById("url");
  const inputFilename = document.getElementById("filename");
  const btnGenerate = document.getElementById("btnGenerate");
  const btnCopy = document.getElementById("btnCopy");
  const btnOpen = document.getElementById("btnOpen");
  const btnDownload = document.getElementById("btnDownload");
  const resultBox = document.getElementById("resultBox");
  const resultLink = document.getElementById("resultLink");
  const hint = document.getElementById("urlHint");
  
  const progressBox = document.getElementById("progressBox");
  const progressText = document.getElementById("progressText");
  const progressPercent = document.getElementById("progressPercent");
  const progressBarFill = document.getElementById("progressBarFill");

  function updatePreview() {
    const raw = String(inputUrl.value || "").trim();
    const filename = String(inputFilename.value || "").trim();
    const mapped = normalizeGithubUrl(raw);
    if (mapped.note) hint.textContent = mapped.note;
    else hint.textContent = "";
    if (!raw) {
      resultBox.hidden = true;
      return;
    }
    const dl = buildDownloadUrl(mapped.url, filename);
    resultLink.textContent = dl;
    resultLink.href = dl;
    btnOpen.href = dl;
    resultBox.hidden = false;
  }

  inputUrl.addEventListener("input", updatePreview);
  inputFilename.addEventListener("input", updatePreview);

  btnGenerate.addEventListener("click", () => {
    updatePreview();
    if (!resultBox.hidden) {
      resultLink.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  });

  btnCopy.addEventListener("click", async () => {
    updatePreview();
    const text = resultLink.href;
    if (!text || text === "#") return;
    await copyToClipboard(text);
    btnCopy.textContent = "کپی شد";
    setTimeout(() => {
      btnCopy.textContent = "کپی";
    }, 900);
  });

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const raw = String(inputUrl.value || "").trim();
    if (!raw) return;

    const mapped = normalizeGithubUrl(raw);
    if (mapped.url !== raw) {
      inputUrl.value = mapped.url;
    }

    const filename = String(inputFilename.value || "").trim();
    const proxyUrl = new URL("/api/download", window.location.origin);
    proxyUrl.searchParams.set("url", mapped.url);
    if (filename) proxyUrl.searchParams.set("filename", filename);

    // Disable UI
    btnDownload.disabled = true;
    btnGenerate.disabled = true;
    progressBox.hidden = false;
    progressBarFill.style.width = "0%";
    progressPercent.textContent = "0%";
    progressText.textContent = "در حال استعلام حجم فایل...";

    try {
      const headRes = await fetch(proxyUrl.toString(), {
        headers: { "Range": "bytes=0-0" }
      });

      const isChunked = headRes.status === 206;
      const contentRange = headRes.headers.get("content-range");
      let totalSize = 0;

      if (isChunked && contentRange) {
        const match = contentRange.match(/\/(\d+)$/);
        if (match) totalSize = parseInt(match[1], 10);
      }

      if (!isChunked || totalSize === 0) {
        progressText.textContent = "دانلود مستقیم (بدون پشتیبانی از تکه‌بندی)...";
        window.location.href = proxyUrl.toString();
        setTimeout(resetUI, 3000);
        return;
      }

      const chunkSize = 2 * 1024 * 1024; // 2MB
      let downloadedBytes = 0;
      const chunks = [];

      for (let start = 0; start < totalSize; start += chunkSize) {
        const end = Math.min(start + chunkSize - 1, totalSize - 1);
        progressText.textContent = `در حال دریافت بخش ${Math.floor(start/chunkSize) + 1} از ${Math.ceil(totalSize/chunkSize)}...`;
        
        const chunkRes = await fetch(proxyUrl.toString(), {
          headers: { "Range": `bytes=${start}-${end}` }
        });
        
        if (!chunkRes.ok) throw new Error("خطا در دریافت بخشی از فایل");
        
        const blob = await chunkRes.blob();
        chunks.push(blob);
        downloadedBytes += blob.size;
        
        const percent = Math.round((downloadedBytes / totalSize) * 100);
        progressBarFill.style.width = `${percent}%`;
        progressPercent.textContent = `${percent}%`;
      }

      progressText.textContent = "در حال آماده‌سازی برای ذخیره...";
      
      const finalBlob = new Blob(chunks, { type: headRes.headers.get("content-type") || "application/octet-stream" });
      const finalUrl = URL.createObjectURL(finalBlob);
      
      let finalFilename = filename;
      if (!finalFilename) {
        const cd = headRes.headers.get("content-disposition");
        if (cd && cd.includes("filename=")) {
          finalFilename = cd.split("filename=")[1].replace(/['"]/g, "");
        } else {
          finalFilename = mapped.url.split("/").pop() || "downloaded_file";
        }
      }

      const a = document.createElement("a");
      a.href = finalUrl;
      a.download = finalFilename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      
      setTimeout(() => URL.revokeObjectURL(finalUrl), 10000);
      
      progressText.textContent = "دانلود با موفقیت انجام شد ✅";
      setTimeout(resetUI, 3000);

    } catch (error) {
      alert("خطا در دانلود ابری: " + error.message);
      resetUI();
    }
  });

  function resetUI() {
    btnDownload.disabled = false;
    btnGenerate.disabled = false;
    progressBox.hidden = true;
  }

  updatePreview();
});
