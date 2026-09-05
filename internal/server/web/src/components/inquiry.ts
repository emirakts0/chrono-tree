import { fetchAlerts, type AlertsPage } from "../api";
import { state } from "../store";
import type { Hello } from "../store";
import { mountDropdown, setDropdownOptions, type DDOption } from "./dropdown";

const PAGE_SIZES = [25, 50, 100, 250, 500];
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
  let pageSize = PAGE_SIZES[0];
  let page: AlertsPage | null = null;

  root.innerHTML = `
    <h2>alert inquiry</h2>
    <form class="filters" id="f">
      <input name="symbol" placeholder="symbol (e.g. BTCUSDT)">
      <span class="ddhost" data-dd="state"></span>
      <span class="ddhost" data-dd="venue"></span>
      <span class="ddhost" data-dd="tier"></span>
      <span class="ddhost" data-dd="direction"></span>
      <button type="submit"><svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.5" fill="none" stroke="currentColor" stroke-width="1.8"/><line x1="10.6" y1="10.6" x2="14" y2="14" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>query</button>
    </form>
    <div class="tblwrap"><table class="tbl"><thead><tr>
      <th>symbol</th><th>dims</th><th>type</th><th>dir</th><th>target</th>
      <th>state</th><th>created</th><th></th>
    </tr></thead><tbody id="rows"></tbody></table></div>
    <div class="pager">
      <label class="pgsize">rows/page
        <select id="pgsize">${PAGE_SIZES.map((n) => `<option value="${n}"${n === pageSize ? " selected" : ""}>${n}</option>`).join("")}</select>
      </label>
      <button id="first" title="first page">«</button>
      <button id="prev">← prev</button>
      <span id="pg" class="mono"></span>
      <button id="next">next →</button>
      <button id="last" title="last page">»</button>
    </div>`;

  const form = root.querySelector("form") as HTMLFormElement;
  const rows = root.querySelector("#rows") as HTMLElement;
  const wrap = root.querySelector(".tblwrap") as HTMLElement;
  const pg = root.querySelector("#pg") as HTMLElement;
  const first = root.querySelector("#first") as HTMLButtonElement;
  const prev = root.querySelector("#prev") as HTMLButtonElement;
  const next = root.querySelector("#next") as HTMLButtonElement;
  const last = root.querySelector("#last") as HTMLButtonElement;
  const pgsize = root.querySelector("#pgsize") as HTMLSelectElement;

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

  const lastOffset = (): number =>
    page ? Math.max(0, Math.floor((page.total - 1) / pageSize) * pageSize) : 0;

  const setPager = (): void => {
    const atStart = offset === 0;
    const atEnd = !(page && offset + page.items.length < page.total);
    first.disabled = prev.disabled = atStart;
    next.disabled = last.disabled = atEnd;
  };

  // A new page of rows starts at the top of the scroll area; the page itself
  // never moves (the card is fixed-height, so nothing outside it reflows).
  const turn = (to: number): void => {
    offset = to;
    wrap.scrollTop = 0;
    void load();
  };

  async function load(): Promise<void> {
    rows.innerHTML = `<tr><td colspan="8" class="empty">querying…</td></tr>`;
    const q: Record<string, string> = { limit: String(pageSize), offset: String(offset) };
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
    turn(0);
  });
  first.addEventListener("click", () => {
    if (offset > 0) turn(0);
  });
  prev.addEventListener("click", () => {
    if (offset > 0) turn(Math.max(0, offset - pageSize));
  });
  next.addEventListener("click", () => {
    if (page && offset + page.items.length < page.total) turn(offset + pageSize);
  });
  last.addEventListener("click", () => {
    if (page && offset + page.items.length < page.total) turn(lastOffset());
  });
  pgsize.addEventListener("change", () => {
    pageSize = Number(pgsize.value);
    turn(0);
  });
  setPager();
  void load();
}
