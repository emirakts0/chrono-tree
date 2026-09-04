import "./style.css";
import { state, tickSeries, pushTrigger } from "./store";
import type { Hello, Snapshot, Trigger } from "./store";
import { mount, update } from "./components/layout";
import { mountInquiry, helloArrived } from "./components/inquiry";

function main(): void {
  mountInquiry(mount(document.getElementById("app")!).inquiry);
  const es = new EventSource("/api/stream"); // EventSource reconnects on its own
  es.onmessage = (ev: MessageEvent<string>) => {
    const frame = JSON.parse(ev.data) as { type: string } & Record<string, unknown>;
    switch (frame.type) {
      case "hello":
        state.hello = frame as unknown as Hello;
        helloArrived(state.hello);
        break;
      case "snapshot":
        state.snapshot = frame.snapshot as Snapshot;
        tickSeries.push(state.snapshot.ticks_per_sec);
        if (tickSeries.length > 120) tickSeries.shift();
        break;
      case "trigger":
        pushTrigger(frame.trigger as Trigger);
        break;
    }
    update();
  };
}

main();
