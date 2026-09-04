import { fetchAlerts, type AlertsPage } from "../api";
import { state } from "../store";
import type { Hello } from "../store";
import { mountDropdown, setDropdownOptions, type DDOption } from "./dropdown";

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
      <span class="ddhost" data-dd="state"></span>
      <span class="ddhost" data-dd="venue"></span>
      <span class="ddhost" data-dd="tier"></span>
      <span class="ddhost" data-dd="direction"></span>
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

  const dd = (name: string): HTMLElement =>
    root.querySelector(`[data-dd="${name}"]`) as HTMLElement;
  const staticOpts = (vals: string[], label: string): DDOption[] =>
    [{ value: "", label }, ...vals.map((v) => ({ value: v, label: v }))];
  mountDropdown(dd("state"), "state", staticOpts(STATES, "state: any"));
  mountDropdown(dd("venue"), "venue", staticOpts([], "venue: any"));
  mountDropdown(dd("tier"), "tier", staticOpts([], "tier: any"));
  mountDropdown(
    dd("direction"), "direction",
    staticOpts(["ABOVE", "BELOW"], "direction: any"),
  );

  fillVocab = (hello: Hello): void => {
    setDropdownOptions(dd("venue"), staticOpts(hello.venues, "venue: any"));
    setDropdownOptions(dd("tier"), staticOpts(hello.tiers, "tier: any"));
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
