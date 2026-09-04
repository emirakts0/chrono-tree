import "./style.css";
import { state, seedHistory, pushSample, pushTriggers } from "./store";
import type { Hello, Snapshot, Trigger } from "./store";
import { mount, update } from "./components/layout";
import { mountInquiry, helloArrived } from "./components/inquiry";
import { playIntro } from "./components/intro";

function main(): void {
  playIntro();
  mountInquiry(mount(document.getElementById("app")!).inquiry);
  const es = new EventSource("/api/stream"); // EventSource reconnects on its own
  es.onmessage = (ev: MessageEvent<string>) => {
    const frame = JSON.parse(ev.data) as { type: string } & Record<string, unknown>;
    switch (frame.type) {
      case "hello":
        state.hello = frame as unknown as Hello;
        seedHistory(state.hello.history);
        helloArrived(state.hello);
        break;
      case "snapshot":
        state.snapshot = frame.snapshot as Snapshot;
        pushSample(state.snapshot);
        break;
      case "triggers":
        pushTriggers(frame.triggers as Trigger[]);
        break;
    }
    update();
  };
}

main();
