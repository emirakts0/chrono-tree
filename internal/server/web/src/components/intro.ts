/**
 * One-shot intro: onyx overlay with the chrono wordmark, which drifts to
 * its topbar home while the page blooms in beneath (~1.4s). Skipped
 * entirely under prefers-reduced-motion.
 */
export function playIntro(): void {
  if (matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  const overlay = document.createElement("div");
  overlay.className = "intro";
  overlay.innerHTML = `<span class="intro-mark">chrono<span class="brand-dot">▸</span></span>`;
  document.body.appendChild(overlay);
  // The wordmark fades/scales down toward the top-left while the overlay
  // dissolves; CSS keyframes own the motion. Cleanup after the longest run.
  setTimeout(() => overlay.remove(), 1800);
}
