/**
 * One-shot intro: onyx overlay with the chrono-tree wordmark. It holds
 * center briefly, then drifts to the exact topbar spot — the overlay
 * background dissolves around it, the mark itself never fades. At the
 * end it is swapped for the real brand (same geometry), so there is no
 * vanish-and-reappear. Skipped under prefers-reduced-motion.
 */
export function playIntro(): void {
  if (matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  const overlay = document.createElement("div");
  overlay.className = "intro";
  overlay.innerHTML = `<span class="intro-mark">chrono-tree<span class="brand-dot">▸</span></span>`;
  document.body.classList.add("introing");
  document.body.appendChild(overlay);
  // 1.5s: drift animation ends exactly here — swap the landed mark for
  // the real brand and drop the (already transparent) overlay.
  setTimeout(() => {
    document.body.classList.remove("introing");
    overlay.remove();
  }, 1500);
}
