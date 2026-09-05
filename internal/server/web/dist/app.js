"use strict";(()=>{var i={hello:null,snapshot:null,triggers:[],series:{ticks:[],fires:[],live:[],venues:{}}},R=120;function x(e,t){e.push(t),e.length>R&&e.shift()}function $(e){for(let t in v)delete v[t];i.series.ticks=[...e.t],i.series.fires=[...e.f],i.series.live=[...e.l],i.series.venues=Object.fromEntries(Object.entries(e.v).map(([t,n])=>[t,[...n]]))}var v={};function j(e,t){let n=i.series.venues[e]??=[],a=v[e]===void 0?0:t-v[e];v[e]=t,x(n,a)}function L(e){x(i.series.ticks,e.ticks_per_sec),x(i.series.fires,e.triggers_per_sec),x(i.series.live,e.engine.live);for(let[t,n]of Object.entries(e.venue_ticks))j(t,n)}function T(e){for(let t of e)i.triggers.unshift(t);i.triggers.length>10&&(i.triggers.length=10)}var f=e=>e>=1e9?`${(e/1e9).toFixed(1)}B`:e>=1e6?`${(e/1e6).toFixed(1)}M`:e>=1e4?`${(e/1e3).toFixed(1)}k`:e.toLocaleString();function M(e){let t=Math.floor(e/3600),n=Math.floor(e%3600/60),a=Math.floor(e%60);return t>0?`${t}h ${n}m`:n>0?`${n}m ${a}s`:`${a}s`}function H(e){return`<span class="dot ${e?"on":"off"}"></span>`}function q(e){if(e.length<2)return"";let t=100,n=28,a=Math.max(...e,1),o=e.map((s,g)=>`${g/(e.length-1)*t},${n-s/a*n}`);return`<svg viewBox="0 0 ${t} ${n}" preserveAspectRatio="none" role="img" aria-label="ticks per second, last two minutes">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" points="${o.join(" ")}"/>
  </svg>`}function y(e,t=100,n=16){if(e.length<2)return"";let a=Math.max(...e,1),o=Math.min(...e,0),s=a-o||1,g=e.map((m,u)=>{let p=u/(e.length-1)*t,c=n-(m-o)/s*n;return`${p.toFixed(1)},${c.toFixed(1)}`});return`<svg viewBox="0 0 ${t} ${n}" preserveAspectRatio="none" class="trend">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" points="${g.join(" ")}"/>
  </svg>`}function z(e){if(e.length<2)return"\u2014";let t=e[e.length-1]-e[e.length-2];return t>0?"\u25B2":t<0?"\u25BC":"\u2014"}function S(e){if(e.length<2)return"flat";let t=e[e.length-1]-e[e.length-2];return t>0?"up":t<0?"down":"flat"}var w={active:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="3.5" fill="none" stroke="currentColor" stroke-width="2"/><circle class="pulse" cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" stroke-width="1"/></svg>',triggered:'<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M9.5 1 3 9.2h3.6L6 15l6.3-8.2H8.7L9.5 1z" fill="currentColor"/></svg>',cancelled:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="5.5" fill="none" stroke="currentColor" stroke-width="1.5"/><line x1="3.4" y1="12.6" x2="12.6" y2="3.4" stroke="currentColor" stroke-width="1.5"/></svg>'};function D(e){e.innerHTML=`
  <header class="topbar">
    <span class="brand">chrono-tree<span class="brand-dot">\u25B8</span></span>
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
    <section class="card area-book"><h2>alert book</h2>
      <div class="trio">
        <div class="tile t-active"><span class="glyph">${w.active}</span><div class="big mono" id="st-active">\u2014</div><div class="sub">active</div><div class="tilespark" id="livetrendline"></div></div>
        <div class="tile t-triggered"><span class="glyph">${w.triggered}</span><div class="big mono" id="st-triggered">\u2014</div><div class="sub">triggered</div><div class="tilespark" id="firesspark"></div></div>
        <div class="tile t-cancelled"><span class="glyph">${w.cancelled}</span><div class="big mono" id="st-cancelled">\u2014</div><div class="sub">cancelled</div></div>
      </div>
    </section>
    <section class="card area-venues"><h2>venues</h2><div id="venues" class="rows"></div></section>
    <section class="card area-engine"><h2>engine</h2><div id="engine" class="rows"></div></section>
    <section class="card area-stream rail"><h2>trigger stream</h2><div class="stream" id="stream">
      <div class="empty">waiting for the first trigger\u2026</div></div></section>
    <section class="card area-inquiry" id="inquiry"></section>
  </main>`;let t=document.createElement("div");t.className="tip",document.body.appendChild(t);let n=document.getElementById("stream");return n.addEventListener("mouseover",a=>{let o=a.target.closest(".trg");if(!o?.dataset.tip)return;t.innerHTML=o.dataset.tip,t.style.opacity="1";let s=o.getBoundingClientRect();t.style.top=`${Math.round(s.top+s.height/2)}px`,t.style.transform="translateY(-50%)",s.right+330<window.innerWidth?(t.style.left=`${Math.round(s.right+10)}px`,t.style.transform="translateY(-50%)"):(t.style.left=`${Math.round(s.left-10)}px`,t.style.transform="translate(-100%, -50%)")}),n.addEventListener("mouseout",a=>{a.target.closest?.(".trg")&&(t.style.opacity="0")}),{inquiry:document.getElementById("inquiry")}}function B(){let e=i.snapshot,t=(p,c)=>{let r=document.getElementById(p);r&&(r.textContent=c)},n=(p,c)=>{let r=document.getElementById(p);r&&(r.innerHTML=H(c))};if(n("dot-feed",e?e.feed_ever_connected&&e.feed_last_seen_ms_ago<1e4:!1),n("dot-nats",!!e?.nats_connected),!e)return;t("published",f(e.triggers_published)),t("pubdropped",f(e.triggers_publish_dropped)),t("tickrate",f(e.ticks_per_sec)),t("ticks",f(e.ticks)),t("ticksdropped",f(e.ticks_dropped));let a=document.getElementById("spark");a&&(a.innerHTML=q(i.series.ticks)),t("st-active",String(e.alerts_by_state.active??0)),t("st-triggered",String(e.alerts_by_state.triggered??0)),t("st-cancelled",String(e.alerts_by_state.cancelled??0));let o=document.getElementById("firesspark");o&&(o.innerHTML=y(i.series.fires));let s=document.getElementById("livetrendline");s&&(s.innerHTML=y(i.series.live));let g=document.getElementById("venues");if(g){let p=Object.entries(e.venue_ticks).sort((c,r)=>r[1]-c[1]);g.innerHTML=p.map(([c,r])=>{let l=i.series.venues[c]??[];return`<div class="row">
          <span class="vname">${c} <span class="trendmark ${S(l)}">${z(l)}</span></span>
          <span class="vmeta">${y(l.slice(-40))}</span>
          <span class="mono">${f(r)}</span>
        </div>`}).join("")||'<div class="empty">no ticks yet</div>'}let m=document.getElementById("engine");m&&(m.innerHTML=`
      <div class="row"><span>ring drops</span><span class="mono">${f(e.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${i.hello?f(i.hello.symbol_count):"\u2014"}</span></div>
      <div class="row"><span>uptime</span><span class="mono">${M(e.uptime_sec)}</span></div>`);let u=document.getElementById("stream");if(u){let p=i.triggers[0],c=p?`${i.triggers.length}:${p.alert_id}:${p.fired_at_unix_nanos}`:"";c!==u.dataset.key&&(u.dataset.key=c,u.innerHTML=i.triggers.length===0?'<div class="empty">waiting for the first trigger\u2026</div>':i.triggers.map(r=>`<div class="trg" data-tip="<b>${r.symbol} \xB7 ${r.direction}</b><div class=tip-row><span class=mono>${r.fired_price}</span> (target <span class=mono>${r.target_price}</span>)</div><div class=tip-row>${r.venue}/${r.tier} \xB7 <span class=mono>${new Date(r.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span></div>">
                <span class="badge ${r.direction==="ABOVE"?"up":"down"}">${r.direction}</span>
                <span class="sym">${r.symbol}</span>
                <span class="mono">${r.fired_price}</span>
                <span class="meta">${r.venue}/${r.tier}</span>
                <span class="mono time">${new Date(r.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span>
              </div>`).join(""))}}async function A(e){let t=new URLSearchParams(Object.entries(e).filter(([,a])=>a!=="")),n=await fetch(`/api/alerts?${t.toString()}`);if(!n.ok)throw new Error(`alerts: HTTP ${n.status}`);return await n.json()}function b(e,t,n){let a=document.createElement("input");a.type="hidden",a.name=t;let o=document.createElement("button");o.type="button",o.className="dd",o.setAttribute("aria-haspopup","listbox"),o.setAttribute("aria-expanded","false");let s=document.createElement("ul");s.className="dd-list",s.role="listbox",s.hidden=!0;let g="",m=()=>{o.innerHTML=`<span>${n.find(r=>r.value===g)?.label??""}</span><i class="chev">\u25BE</i>`;for(let r of s.children)r.classList.toggle("sel",r.dataset.v===g)};n.forEach(r=>{let l=document.createElement("li");l.role="option",l.dataset.v=r.value,l.tabIndex=-1,l.textContent=r.label,l.addEventListener("click",()=>{g=r.value,a.value=g,m(),p()}),s.appendChild(l)});let u=()=>{s.hidden=!1,o.setAttribute("aria-expanded","true"),s.querySelector(`[data-v="${g}"]`)?.focus()},p=()=>{s.hidden=!0,o.setAttribute("aria-expanded","false")},c=()=>s.hidden?u():p();return o.addEventListener("click",c),s.addEventListener("keydown",r=>{let l=[...s.children],d=l.indexOf(document.activeElement);r.key==="Escape"?(p(),o.focus()):r.key==="ArrowDown"&&d<l.length-1?l[d+1].focus():r.key==="ArrowUp"&&d>0?l[d-1].focus():r.key==="Enter"&&d>=0&&(l[d].click(),r.preventDefault())}),document.addEventListener("click",r=>{e.contains(r.target)||p()}),e.append(a,o,s),m(),a}function _(e,t){let n=e.querySelector("input"),a=n.value;e.innerHTML="",b(e,n.name,t);let o=e.querySelector("input");o.value=a}var E=25,F=["active","triggered","cancelled"],k=null;function I(e){k?.(e)}function O(e){let t=0,n=null;e.innerHTML=`
    <h2>alert inquiry</h2>
    <form class="filters" id="f">
      <input name="symbol" placeholder="symbol (e.g. BTCUSDT)">
      <span class="ddhost" data-dd="state"></span>
      <span class="ddhost" data-dd="venue"></span>
      <span class="ddhost" data-dd="tier"></span>
      <span class="ddhost" data-dd="direction"></span>
      <button type="submit"><svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.5" fill="none" stroke="currentColor" stroke-width="1.8"/><line x1="10.6" y1="10.6" x2="14" y2="14" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>query</button>
    </form>
    <table class="tbl"><thead><tr>
      <th>symbol</th><th>dims</th><th>type</th><th>dir</th><th>target</th>
      <th>state</th><th>created</th><th></th>
    </tr></thead><tbody id="rows"></tbody></table>
    <div class="pager"><button id="prev">\u2190 prev</button>
      <span id="pg" class="mono"></span>
      <button id="next">next \u2192</button></div>`;let a=e.querySelector("form"),o=e.querySelector("#rows"),s=e.querySelector("#pg"),g=e.querySelector("#prev"),m=e.querySelector("#next"),u=l=>e.querySelector(`[data-dd="${l}"]`),p=(l,d)=>[{value:"",label:d},...l.map(h=>({value:h,label:h}))];b(u("state"),"state",p(F,"state: any")),b(u("venue"),"venue",p([],"venue: any")),b(u("tier"),"tier",p([],"tier: any")),b(u("direction"),"direction",p(["ABOVE","BELOW"],"direction: any")),k=l=>{_(u("venue"),p(l.venues,"venue: any")),_(u("tier"),p(l.tiers,"tier: any")),k=null},i.hello&&k(i.hello);let c=()=>{g.disabled=t===0,m.disabled=!(n&&t+n.items.length<n.total)};async function r(){o.innerHTML='<tr><td colspan="8" class="empty">querying\u2026</td></tr>';let l={limit:String(E),offset:String(t)};new FormData(a).forEach((d,h)=>l[h]=String(d));try{n=await A(l)}catch(d){n=null,o.innerHTML=`<tr><td colspan="8" class="empty err">query failed \u2014 ${d instanceof Error?d.message:String(d)}</td></tr>`,s.textContent="",c();return}o.innerHTML=n.items.length===0?'<tr><td colspan="8" class="empty">no alerts match</td></tr>':n.items.map(d=>`<tr>
              <td>${d.symbol}</td><td class="meta">${d.venue}/${d.tier}</td>
              <td class="meta">${d.price_type}</td>
              <td><span class="badge ${d.direction==="ABOVE"?"up":"down"}">${d.direction}</span></td>
              <td class="mono">${d.target_price}</td>
              <td><span class="chip state-${d.state}">${d.state}</span></td>
              <td class="mono time">${new Date(d.created_at_unix_nanos/1e6).toLocaleTimeString()}</td>
              <td class="mono meta">${d.id.slice(0,8)}</td>
            </tr>`).join(""),s.textContent=n.items.length===0?`0 of ${n.total}`:`${t+1}\u2013${t+n.items.length} of ${n.total}`,c()}a.addEventListener("submit",l=>{l.preventDefault(),t=0,r()}),g.addEventListener("click",()=>{t>0&&(t=Math.max(0,t-E),r())}),m.addEventListener("click",()=>{n&&t+n.items.length<n.total&&(t+=E,r())}),c(),r()}function C(){if(matchMedia("(prefers-reduced-motion: reduce)").matches)return;let e=document.createElement("div");e.className="intro",e.innerHTML='<span class="intro-mark">chrono-tree<span class="brand-dot">\u25B8</span></span>',document.body.classList.add("introing"),document.body.appendChild(e),setTimeout(()=>{document.body.classList.remove("introing"),e.remove()},1500)}function P(){C(),O(D(document.getElementById("app")).inquiry);let e=new EventSource("/api/stream");e.onmessage=t=>{let n=JSON.parse(t.data);switch(n.type){case"hello":i.hello=n,$(i.hello.history),I(i.hello);break;case"snapshot":i.snapshot=n.snapshot,L(i.snapshot);break;case"triggers":T(n.triggers);break}B()}}P();})();
