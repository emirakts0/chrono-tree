import { fetchAlerts, type AlertsPage } from "../api";
import { state } from "../store";
import type { Hello } from "../store";

const PAGE = 25;
const STATES = ["active", "triggered", "cancelled"];

// Set by mountInquiry; helloArrived fills the venue/tier selects once the
// SSE hello frame lands (the vocabulary is static for the process lifetime).
let fillVocab: ((hello: Hello) => void) | null = null;

/** Called from app.ts when the hello frame arrives; no-op after the first. */
export function helloArrived(hello: Hello): void {
  fillVocab?.(hello);
}

export function mountInquiry(root: HTMLElement): void {
  let offset = 0;
  let page: AlertsPage | null = null;

  root.innerHTML = `
    <h2>alert inquiry</h2>
    <form class="filters" id="f">
      <input name="symbol" placeholder="symbol (e.g. BTCUSDT)">
      <select name="state"><option value="">state: any</option>
        ${STATES.map((s) => `<option>${s}</option>`).join("")}</select>
      <select name="venue"><option value="">venue: any</option></select>
      <select name="tier"><option value="">tier: any</option></select>
      <select name="direction"><option value="">direction: any</option>
        <option>ABOVE</option><option>BELOW</option></select>
      <button type="submit">query</button>
    </form>
    <table class="tbl"><thead><tr>
      <th>symbol</th><th>dims</th><th>type</th><th>dir</th><th>target</th>
      <th>state</th><th>created</th><th></th>
    </tr></thead><tbody id="rows"></tbody></table>
    <div class="pager"><button id="prev">← prev</button>
      <span id="pg" class="mono"></span>
      <button id="next">next →</button></div>`;

  const form = root.querySelector("form") as HTMLFormElement;
  const rows = root.querySelector("#rows") as HTMLElement;
  const pg = root.querySelector("#pg") as HTMLElement;
  const prev = root.querySelector("#prev") as HTMLButtonElement;
  const next = root.querySelector("#next") as HTMLButtonElement;
  const venueSel = root.querySelector('select[name="venue"]') as HTMLSelectElement;
  const tierSel = root.querySelector('select[name="tier"]') as HTMLSelectElement;

  const setOptions = (sel: HTMLSelectElement, values: string[]): void => {
    const current = sel.value;
    sel.innerHTML =
      `<option value="">${sel.name}: any</option>` +
      values.map((v) => `<option>${v}</option>`).join("");
    sel.value = current;
  };

  fillVocab = (hello: Hello): void => {
    setOptions(venueSel, hello.venues);
    setOptions(tierSel, hello.tiers);
    fillVocab = null;
  };
  // The hello frame may have arrived before this mount.
  if (state.hello) fillVocab(state.hello);

  const setPager = (): void => {
    prev.disabled = offset === 0;
    next.disabled = !(page && offset + page.items.length < page.total);
  };

  async function load(): Promise<void> {
    rows.innerHTML = `<tr><td colspan="8" class="empty">querying…</td></tr>`;
    const q: Record<string, string> = { limit: String(PAGE), offset: String(offset) };
    new FormData(form).forEach((v, k) => (q[k] = String(v)));
    try {
      page = await fetchAlerts(q);
    } catch (err) {
      page = null;
      rows.innerHTML = `<tr><td colspan="8" class="empty err">query failed — ${
        err instanceof Error ? err.message : String(err)
      }</td></tr>`;
      pg.textContent = "";
      setPager();
      return;
    }
    rows.innerHTML =
      page.items.length === 0
        ? `<tr><td colspan="8" class="empty">no alerts match</td></tr>`
        : page.items
            .map(
              (a) => `<tr>
              <td>${a.symbol}</td><td class="meta">${a.venue}/${a.tier}</td>
              <td class="meta">${a.price_type}</td>
              <td><span class="badge ${a.direction === "ABOVE" ? "up" : "down"}">${a.direction}</span></td>
              <td class="mono">${a.target_price}</td>
              <td><span class="chip state-${a.state}">${a.state}</span></td>
              <td class="mono time">${new Date(a.created_at_unix_nanos / 1e6).toLocaleTimeString()}</td>
              <td class="mono meta">${a.id.slice(0, 8)}</td>
            </tr>`
            )
            .join("");
    pg.textContent =
      page.items.length === 0
        ? `0 of ${page.total}`
        : `${offset + 1}–${offset + page.items.length} of ${page.total}`;
    setPager();
  }

  form.addEventListener("submit", (e) => {
    e.preventDefault();
    offset = 0;
    void load();
  });
  prev.addEventListener("click", () => {
    if (offset > 0) {
      offset = Math.max(0, offset - PAGE);
      void load();
    }
  });
  next.addEventListener("click", () => {
    if (page && offset + page.items.length < page.total) {
      offset += PAGE;
      void load();
    }
  });
  setPager();
  void load();
}
