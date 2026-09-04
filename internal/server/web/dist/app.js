"use strict";(()=>{var i={hello:null,snapshot:null,triggers:[],series:{ticks:[],fires:[],live:[],venues:{}}},F=120;function y(e,t){e.push(t),e.length>F&&e.shift()}function M(e){for(let t in h)delete h[t];i.series.ticks=[...e.t],i.series.fires=[...e.f],i.series.live=[...e.l],i.series.venues=Object.fromEntries(Object.entries(e.v).map(([t,n])=>[t,[...n]]))}var h={};function P(e,t){let n=i.series.venues[e]??=[],l=h[e]===void 0?0:t-h[e];h[e]=t,y(n,l)}function q(e){y(i.series.ticks,e.ticks_per_sec),y(i.series.fires,e.triggers_per_sec),y(i.series.live,e.engine.live);for(let[t,n]of Object.entries(e.venue_ticks))P(t,n)}function S(e){for(let t of e)i.triggers.unshift(t);i.triggers.length>10&&(i.triggers.length=10)}var f=e=>e>=1e9?`${(e/1e9).toFixed(1)}B`:e>=1e6?`${(e/1e6).toFixed(1)}M`:e>=1e4?`${(e/1e3).toFixed(1)}k`:e.toLocaleString();function z(e){let t=Math.floor(e/3600),n=Math.floor(e%3600/60),l=Math.floor(e%60);return t>0?`${t}h ${n}m`:n>0?`${n}m ${l}s`:`${l}s`}function D(e){return`<span class="dot ${e?"on":"off"}"></span>`}function B(e){if(e.length<2)return"";let t=100,n=28,l=Math.max(...e,1),o=e.map((d,g)=>`${g/(e.length-1)*t},${n-d/l*n}`);return`<svg viewBox="0 0 ${t} ${n}" preserveAspectRatio="none" role="img" aria-label="ticks per second, last two minutes">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" points="${o.join(" ")}"/>
  </svg>`}function k(e,t=100,n=16){if(e.length<2)return"";let l=Math.max(...e,1),o=Math.min(...e,0),d=l-o||1,g=e.map((v,m)=>{let c=m/(e.length-1)*t,p=n-(v-o)/d*n;return`${c.toFixed(1)},${p.toFixed(1)}`});return`<svg viewBox="0 0 ${t} ${n}" preserveAspectRatio="none" class="trend">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" points="${g.join(" ")}"/>
  </svg>`}function E(e){if(e.length<2)return"\u2014";let t=e[e.length-1]-e[e.length-2];return t>0?"\u25B2":t<0?"\u25BC":"\u2014"}function $(e){if(e.length<2)return"flat";let t=e[e.length-1]-e[e.length-2];return t>0?"up":t<0?"down":"flat"}var L={active:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="3.5" fill="none" stroke="currentColor" stroke-width="2"/><circle class="pulse" cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" stroke-width="1"/></svg>',triggered:'<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M9.5 1 3 9.2h3.6L6 15l6.3-8.2H8.7L9.5 1z" fill="currentColor"/></svg>',cancelled:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="5.5" fill="none" stroke="currentColor" stroke-width="1.5"/><line x1="3.4" y1="12.6" x2="12.6" y2="3.4" stroke="currentColor" stroke-width="1.5"/></svg>'};function A(e){return e.innerHTML=`
  <header class="topbar">
    <span class="brand">chrono<span class="brand-dot">\u25B8</span></span>
    <span class="status mono">
      <span id="dot-feed"></span> feed
      <span id="dot-nats"></span> nats
      <span class="sep"></span>
      <span class="mono" id="published">\u2014</span> pub
      \xB7 <span class="mono" id="pubdropped">\u2014</span> drop
    </span>
  </header>
  <main class="grid">
    <section class="card area-pulse"><h2>pulse</h2>
      <div class="big mono" id="tickrate">\u2014</div>
      <div class="sub">ticks/s</div>
      <div id="spark"></div>
      <div class="duo"><span class="mono" id="ticks">\u2014</span> ticks
        \xB7 <span class="mono" id="ticksdropped">\u2014</span> dropped</div>
    </section>
    <section class="card area-fires fires"><h2>fires</h2>
      <div class="big mono" id="fired">\u2014</div>
      <div class="sub">triggers fired \xB7 <span id="firesarrow">\u2014</span></div>
      <div id="firesspark"></div>
    </section>
    <section class="card area-book"><h2>alert book</h2>
      <div class="trio">
        <div class="tile t-active"><span class="glyph">${L.active}</span><div class="big mono" id="st-active">\u2014</div><div class="sub">active</div></div>
        <div class="tile t-triggered"><span class="glyph">${L.triggered}</span><div class="big mono" id="st-triggered">\u2014</div><div class="sub">triggered</div></div>
        <div class="tile t-cancelled"><span class="glyph">${L.cancelled}</span><div class="big mono" id="st-cancelled">\u2014</div><div class="sub">cancelled</div></div>
      </div>
      <div class="livetrend"><span class="lbl">live alerts</span>
        <span class="mono" id="live">\u2014</span><span id="livetrendline"></span></div>
    </section>
    <section class="card area-venues"><h2>venues</h2><div id="venues" class="rows"></div></section>
    <section class="card area-engine"><h2>engine</h2><div id="engine" class="rows"></div></section>
    <section class="card area-stream rail"><h2>trigger stream</h2><div class="stream" id="stream">
      <div class="empty">waiting for the first trigger\u2026</div></div></section>
    <section class="card area-inquiry" id="inquiry"></section>
  </main>`,{inquiry:document.getElementById("inquiry")}}function I(){let e=i.snapshot,t=(p,a)=>{let r=document.getElementById(p);r&&(r.textContent=a)},n=(p,a)=>{let r=document.getElementById(p);r&&(r.innerHTML=D(a))};if(n("dot-feed",e?e.feed_ever_connected&&e.feed_last_seen_ms_ago<1e4:!1),n("dot-nats",!!e?.nats_connected),!e)return;t("published",f(e.triggers_published)),t("pubdropped",f(e.triggers_publish_dropped)),t("tickrate",f(e.ticks_per_sec)),t("ticks",f(e.ticks)),t("ticksdropped",f(e.ticks_dropped));let l=document.getElementById("spark");l&&(l.innerHTML=B(i.series.ticks)),t("fired",f(e.triggers_fired));let o=document.getElementById("firesarrow");o&&(o.textContent=E(i.series.fires),o.className=`trendmark ${$(i.series.fires)}`);let d=document.getElementById("firesspark");d&&(d.innerHTML=k(i.series.fires)),t("st-active",String(e.alerts_by_state.active??0)),t("st-triggered",String(e.alerts_by_state.triggered??0)),t("st-cancelled",String(e.alerts_by_state.cancelled??0)),t("live",f(e.engine.live));let g=document.getElementById("livetrendline");g&&(g.innerHTML=k(i.series.live));let v=document.getElementById("venues");if(v){let p=Object.entries(e.venue_ticks).sort((u,x)=>x[1]-u[1]),a=p[0]?.[1]||1,r=u=>(i.series.venues[u]??[]).slice(-1)[0]??0,s=Math.max(1,...p.map(([u])=>r(u)));v.innerHTML=p.map(([u,x])=>{let _=i.series.venues[u]??[];return`<div class="row">
          <span class="vname">${u} <span class="trendmark ${$(_)}">${E(_)}</span></span>
          <span class="vmeta">${k(_.slice(-40))}</span>
          <span class="bar"><i style="width:${100*x/a}%"></i><i class="mark" style="left:${100*r(u)/s}%"></i></span>
          <span class="mono">${f(x)}</span>
        </div>`}).join("")||'<div class="empty">no ticks yet</div>'}let m=document.getElementById("engine");m&&(m.innerHTML=`
      <div class="row"><span>ring drops</span><span class="mono">${f(e.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${i.hello?f(i.hello.symbol_count):"\u2014"}</span></div>
      <div class="row"><span>uptime</span><span class="mono">${z(e.uptime_sec)}</span></div>`);let c=document.getElementById("stream");if(c){let p=i.triggers[0],a=p?`${i.triggers.length}:${p.alert_id}:${p.fired_at_unix_nanos}`:"";a!==c.dataset.key&&(c.dataset.key=a,c.innerHTML=i.triggers.length===0?'<div class="empty">waiting for the first trigger\u2026</div>':i.triggers.map(r=>`<div class="trg">
                <span class="badge ${r.direction==="ABOVE"?"up":"down"}">${r.direction}</span>
                <span class="sym">${r.symbol}</span>
                <span class="mono">${r.fired_price}</span>
                <span class="meta">${r.venue}/${r.tier}</span>
                <span class="mono time">${new Date(r.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span>
              </div>`).join(""))}}async function O(e){let t=new URLSearchParams(Object.entries(e).filter(([,l])=>l!=="")),n=await fetch(`/api/alerts?${t.toString()}`);if(!n.ok)throw new Error(`alerts: HTTP ${n.status}`);return await n.json()}function b(e,t,n){let l=document.createElement("input");l.type="hidden",l.name=t;let o=document.createElement("button");o.type="button",o.className="dd",o.setAttribute("aria-haspopup","listbox"),o.setAttribute("aria-expanded","false");let d=document.createElement("ul");d.className="dd-list",d.role="listbox",d.hidden=!0;let g="",v=()=>{o.innerHTML=`<span>${n.find(a=>a.value===g)?.label??""}</span><i class="chev">\u25BE</i>`;for(let a of d.children)a.classList.toggle("sel",a.dataset.v===g)};n.forEach(a=>{let r=document.createElement("li");r.role="option",r.dataset.v=a.value,r.tabIndex=-1,r.textContent=a.label,r.addEventListener("click",()=>{g=a.value,l.value=g,v(),c()}),d.appendChild(r)});let m=()=>{d.hidden=!1,o.setAttribute("aria-expanded","true"),d.querySelector(`[data-v="${g}"]`)?.focus()},c=()=>{d.hidden=!0,o.setAttribute("aria-expanded","false")},p=()=>d.hidden?m():c();return o.addEventListener("click",p),d.addEventListener("keydown",a=>{let r=[...d.children],s=r.indexOf(document.activeElement);a.key==="Escape"?(c(),o.focus()):a.key==="ArrowDown"&&s<r.length-1?r[s+1].focus():a.key==="ArrowUp"&&s>0?r[s-1].focus():a.key==="Enter"&&s>=0&&(r[s].click(),a.preventDefault())}),document.addEventListener("click",a=>{e.contains(a.target)||c()}),e.append(l,o,d),v(),l}function T(e,t){let n=e.querySelector("input"),l=n.value;e.innerHTML="",b(e,n.name,t);let o=e.querySelector("input");o.value=l}var H=25,V=["active","triggered","cancelled"],w=null;function C(e){w?.(e)}function R(e){let t=0,n=null;e.innerHTML=`
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
    <div class="pager"><button id="prev">\u2190 prev</button>
      <span id="pg" class="mono"></span>
      <button id="next">next \u2192</button></div>`;let l=e.querySelector("form"),o=e.querySelector("#rows"),d=e.querySelector("#pg"),g=e.querySelector("#prev"),v=e.querySelector("#next"),m=r=>e.querySelector(`[data-dd="${r}"]`),c=(r,s)=>[{value:"",label:s},...r.map(u=>({value:u,label:u}))];b(m("state"),"state",c(V,"state: any")),b(m("venue"),"venue",c([],"venue: any")),b(m("tier"),"tier",c([],"tier: any")),b(m("direction"),"direction",c(["ABOVE","BELOW"],"direction: any")),w=r=>{T(m("venue"),c(r.venues,"venue: any")),T(m("tier"),c(r.tiers,"tier: any")),w=null},i.hello&&w(i.hello);let p=()=>{g.disabled=t===0,v.disabled=!(n&&t+n.items.length<n.total)};async function a(){o.innerHTML='<tr><td colspan="8" class="empty">querying\u2026</td></tr>';let r={limit:String(H),offset:String(t)};new FormData(l).forEach((s,u)=>r[u]=String(s));try{n=await O(r)}catch(s){n=null,o.innerHTML=`<tr><td colspan="8" class="empty err">query failed \u2014 ${s instanceof Error?s.message:String(s)}</td></tr>`,d.textContent="",p();return}o.innerHTML=n.items.length===0?'<tr><td colspan="8" class="empty">no alerts match</td></tr>':n.items.map(s=>`<tr>
              <td>${s.symbol}</td><td class="meta">${s.venue}/${s.tier}</td>
              <td class="meta">${s.price_type}</td>
              <td><span class="badge ${s.direction==="ABOVE"?"up":"down"}">${s.direction}</span></td>
              <td class="mono">${s.target_price}</td>
              <td><span class="chip state-${s.state}">${s.state}</span></td>
              <td class="mono time">${new Date(s.created_at_unix_nanos/1e6).toLocaleTimeString()}</td>
              <td class="mono meta">${s.id.slice(0,8)}</td>
            </tr>`).join(""),d.textContent=n.items.length===0?`0 of ${n.total}`:`${t+1}\u2013${t+n.items.length} of ${n.total}`,p()}l.addEventListener("submit",r=>{r.preventDefault(),t=0,a()}),g.addEventListener("click",()=>{t>0&&(t=Math.max(0,t-H),a())}),v.addEventListener("click",()=>{n&&t+n.items.length<n.total&&(t+=H,a())}),p(),a()}function j(){if(matchMedia("(prefers-reduced-motion: reduce)").matches)return;let e=document.createElement("div");e.className="intro",e.innerHTML='<span class="intro-mark">chrono<span class="brand-dot">\u25B8</span></span>',document.body.appendChild(e),setTimeout(()=>e.remove(),1800)}function N(){j(),R(A(document.getElementById("app")).inquiry);let e=new EventSource("/api/stream");e.onmessage=t=>{let n=JSON.parse(t.data);switch(n.type){case"hello":i.hello=n,M(i.hello.history),C(i.hello);break;case"snapshot":i.snapshot=n.snapshot,q(i.snapshot);break;case"triggers":S(n.triggers);break}I()}}N();})();
