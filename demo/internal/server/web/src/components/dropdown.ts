export interface DDOption {
  value: string; // "" = "any"
  label: string;
}

/**
 * Custom dropdown (native <select> popups cannot be styled). Renders a
 * button + popover into `host`; keeps `input` (a hidden form input) in
 * sync so the existing FormData-based query build is untouched.
 */
export function mountDropdown(
  host: HTMLElement,
  name: string,
  options: DDOption[],
): HTMLInputElement {
  const input = document.createElement("input");
  input.type = "hidden";
  input.name = name;

  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "dd";
  btn.setAttribute("aria-haspopup", "listbox");
  btn.setAttribute("aria-expanded", "false");

  const list = document.createElement("ul");
  list.className = "dd-list";
  list.role = "listbox";
  list.hidden = true;

  let selected = "";
  const render = (): void => {
    btn.innerHTML = `<span>${options.find((o) => o.value === selected)?.label ?? ""}</span><i class="chev">▾</i>`;
    for (const li of list.children) {
      (li as HTMLElement).classList.toggle("sel", (li as HTMLElement).dataset.v === selected);
    }
  };

  options.forEach((o) => {
    const li = document.createElement("li");
    li.role = "option";
    li.dataset.v = o.value;
    li.tabIndex = -1; // focusable so the listbox key handler below works
    li.textContent = o.label;
    li.addEventListener("click", () => {
      selected = o.value;
      input.value = selected;
      render();
      close();
    });
    list.appendChild(li);
  });

  const open = (): void => {
    list.hidden = false;
    btn.setAttribute("aria-expanded", "true");
    (list.querySelector(`[data-v="${selected}"]`) as HTMLElement | null)?.focus();
  };
  const close = (): void => {
    list.hidden = true;
    btn.setAttribute("aria-expanded", "false");
  };
  const toggle = (): void => (list.hidden ? open() : close());

  btn.addEventListener("click", toggle);
  list.addEventListener("keydown", (e: KeyboardEvent) => {
    const items = [...list.children] as HTMLElement[];
    const i = items.indexOf(document.activeElement as HTMLElement);
    if (e.key === "Escape") { close(); btn.focus(); }
    else if (e.key === "ArrowDown" && i < items.length - 1) items[i + 1].focus();
    else if (e.key === "ArrowUp" && i > 0) items[i - 1].focus();
    else if (e.key === "Enter" && i >= 0) { items[i].click(); e.preventDefault(); }
  });
  document.addEventListener("click", (e) => {
    if (!host.contains(e.target as Node)) close();
  });

  host.append(input, btn, list);
  render();
  return input;
}

/** Replace a dropdown's option list in place (hello-frame vocabulary). */
export function setDropdownOptions(host: HTMLElement, options: DDOption[]): void {
  // Simplest correct approach: unmount and rebuild preserving selection.
  const input = host.querySelector("input") as HTMLInputElement;
  const sel = input.value;
  host.innerHTML = "";
  mountDropdown(host, input.name, options);
  const rebuilt = host.querySelector("input") as HTMLInputElement;
  rebuilt.value = sel;
}
