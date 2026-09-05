"use strict";(()=>{var a={hello:null,snapshot:null,triggers:[],series:{ticks:[],fires:[],live:[],venues:{}}},j=120;function h(e,t){e.push(t),e.length>j&&e.shift()}function L(e){for(let t in x)delete x[t];a.series.ticks=[...e.t],a.series.fires=[...e.f],a.series.live=[...e.l],a.series.venues=Object.fromEntries(Object.entries(e.v).map(([t,n])=>[t,[...n]]))}var x={};function F(e,t){let n=a.series.venues[e]??=[],l=x[e]===void 0?0:t-x[e];x[e]=t,h(n,l)}function T(e){h(a.series.ticks,e.ticks_per_sec),h(a.series.fires,e.triggers_per_sec),h(a.series.live,e.engine.live);for(let[t,n]of Object.entries(e.venue_ticks))F(t,n)}function H(e){for(let t of e)a.triggers.unshift(t);a.triggers.length>10&&(a.triggers.length=10)}var b=e=>e>=1e9?`${(e/1e9).toFixed(1)}B`:e>=1e6?`${(e/1e6).toFixed(1)}M`:e>=1e4?`${(e/1e3).toFixed(1)}k`:e.toLocaleString();function M(e){let t=Math.floor(e/3600),n=Math.floor(e%3600/60),l=Math.floor(e%60);return t>0?`${t}h ${n}m`:n>0?`${n}m ${l}s`:`${l}s`}function q(e){return`<span class="dot ${e?"on":"off"}"></span>`}function S(e){if(e.length<2)return"";let t=100,n=28,l=Math.max(...e,1),d=e.map((p,g)=>`${g/(e.length-1)*t},${n-p/l*n}`);return`<svg viewBox="0 0 ${t} ${n}" preserveAspectRatio="none" role="img" aria-label="ticks per second, last two minutes">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" points="${d.join(" ")}"/>
  </svg>`}function y(e,t=100,n=16){if(e.length<2)return"";let l=Math.max(...e,1),d=Math.min(...e,0),p=l-d||1,g=e.map((m,u)=>{let o=u/(e.length-1)*t,c=n-(m-d)/p*n;return`${o.toFixed(1)},${c.toFixed(1)}`});return`<svg viewBox="0 0 ${t} ${n}" preserveAspectRatio="none" class="trend">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" points="${g.join(" ")}"/>
  </svg>`}function z(e){if(e.length<2)return"\u2014";let t=e[e.length-1]-e[e.length-2];return t>0?"\u25B2":t<0?"\u25BC":"\u2014"}function D(e){if(e.length<2)return"flat";let t=e[e.length-1]-e[e.length-2];return t>0?"up":t<0?"down":"flat"}var _={active:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="3.5" fill="none" stroke="currentColor" stroke-width="2"/><circle class="pulse" cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" stroke-width="1"/></svg>',triggered:'<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M9.5 1 3 9.2h3.6L6 15l6.3-8.2H8.7L9.5 1z" fill="currentColor"/></svg>',cancelled:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="5.5" fill="none" stroke="currentColor" stroke-width="1.5"/><line x1="3.4" y1="12.6" x2="12.6" y2="3.4" stroke="currentColor" stroke-width="1.5"/></svg>'};function B(e){return e.innerHTML=`
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
    <section class="card area-book"><h2>alert book</h2>
      <div class="trio">
        <div class="tile t-active"><span class="glyph">${_.active}</span><div class="big mono" id="st-active">\u2014</div><div class="sub">active</div><div class="tilespark" id="livetrendline"></div></div>
        <div class="tile t-triggered"><span class="glyph">${_.triggered}</span><div class="big mono" id="st-triggered">\u2014</div><div class="sub">triggered</div><div class="tilespark" id="firesspark"></div></div>
        <div class="tile t-cancelled"><span class="glyph">${_.cancelled}</span><div class="big mono" id="st-cancelled">\u2014</div><div class="sub">cancelled</div></div>
      </div>
    </section>
    <section class="card area-venues"><h2>venues</h2><div id="venues" class="rows"></div></section>
    <section class="card area-engine"><h2>engine</h2><div id="engine" class="rows"></div></section>
    <section class="card area-stream rail"><h2>trigger stream</h2><div class="stream" id="stream">
      <div class="empty">waiting for the first trigger\u2026</div></div></section>
    <section class="card area-inquiry" id="inquiry"></section>
  </main>`,{inquiry:document.getElementById("inquiry")}}function A(){let e=a.snapshot,t=(o,c)=>{let r=document.getElementById(o);r&&(r.textContent=c)},n=(o,c)=>{let r=document.getElementById(o);r&&(r.innerHTML=q(c))};if(n("dot-feed",e?e.feed_ever_connected&&e.feed_last_seen_ms_ago<1e4:!1),n("dot-nats",!!e?.nats_connected),!e)return;t("published",b(e.triggers_published)),t("pubdropped",b(e.triggers_publish_dropped)),t("tickrate",b(e.ticks_per_sec)),t("ticks",b(e.ticks)),t("ticksdropped",b(e.ticks_dropped));let l=document.getElementById("spark");l&&(l.innerHTML=S(a.series.ticks)),t("st-active",String(e.alerts_by_state.active??0)),t("st-triggered",String(e.alerts_by_state.triggered??0)),t("st-cancelled",String(e.alerts_by_state.cancelled??0));let d=document.getElementById("firesspark");d&&(d.innerHTML=y(a.series.fires));let p=document.getElementById("livetrendline");p&&(p.innerHTML=y(a.series.live));let g=document.getElementById("venues");if(g){let o=Object.entries(e.venue_ticks).sort((i,f)=>f[1]-i[1]),c=o[0]?.[1]||1,r=i=>(a.series.venues[i]??[]).slice(-1)[0]??0,s=Math.max(1,...o.map(([i])=>r(i)));g.innerHTML=o.map(([i,f])=>{let w=a.series.venues[i]??[];return`<div class="row">
          <span class="vname">${i} <span class="trendmark ${D(w)}">${z(w)}</span></span>
          <span class="vmeta">${y(w.slice(-40))}</span>
          <span class="bar"><i style="width:${100*f/c}%"></i><i class="mark" style="left:${100*r(i)/s}%"></i></span>
          <span class="mono">${b(f)}</span>
        </div>`}).join("")||'<div class="empty">no ticks yet</div>'}let m=document.getElementById("engine");m&&(m.innerHTML=`
      <div class="row"><span>ring drops</span><span class="mono">${b(e.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${a.hello?b(a.hello.symbol_count):"\u2014"}</span></div>
      <div class="row"><span>uptime</span><span class="mono">${M(e.uptime_sec)}</span></div>`);let u=document.getElementById("stream");if(u){let o=a.triggers[0],c=o?`${a.triggers.length}:${o.alert_id}:${o.fired_at_unix_nanos}`:"";c!==u.dataset.key&&(u.dataset.key=c,u.innerHTML=a.triggers.length===0?'<div class="empty">waiting for the first trigger\u2026</div>':a.triggers.map(r=>`<div class="trg">
                <span class="badge ${r.direction==="ABOVE"?"up":"down"}">${r.direction}</span>
                <span class="sym">${r.symbol}</span>
                <span class="mono">${r.fired_price}</span>
                <span class="meta">${r.venue}/${r.tier}</span>
                <span class="mono time">${new Date(r.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span>
              </div>`).join(""))}}async function I(e){let t=new URLSearchParams(Object.entries(e).filter(([,l])=>l!=="")),n=await fetch(`/api/alerts?${t.toString()}`);if(!n.ok)throw new Error(`alerts: HTTP ${n.status}`);return await n.json()}function v(e,t,n){let l=document.createElement("input");l.type="hidden",l.name=t;let d=document.createElement("button");d.type="button",d.className="dd",d.setAttribute("aria-haspopup","listbox"),d.setAttribute("aria-expanded","false");let p=document.createElement("ul");p.className="dd-list",p.role="listbox",p.hidden=!0;let g="",m=()=>{d.innerHTML=`<span>${n.find(r=>r.value===g)?.label??""}</span><i class="chev">\u25BE</i>`;for(let r of p.children)r.classList.toggle("sel",r.dataset.v===g)};n.forEach(r=>{let s=document.createElement("li");s.role="option",s.dataset.v=r.value,s.tabIndex=-1,s.textContent=r.label,s.addEventListener("click",()=>{g=r.value,l.value=g,m(),o()}),p.appendChild(s)});let u=()=>{p.hidden=!1,d.setAttribute("aria-expanded","true"),p.querySelector(`[data-v="${g}"]`)?.focus()},o=()=>{p.hidden=!0,d.setAttribute("aria-expanded","false")},c=()=>p.hidden?u():o();return d.addEventListener("click",c),p.addEventListener("keydown",r=>{let s=[...p.children],i=s.indexOf(document.activeElement);r.key==="Escape"?(o(),d.focus()):r.key==="ArrowDown"&&i<s.length-1?s[i+1].focus():r.key==="ArrowUp"&&i>0?s[i-1].focus():r.key==="Enter"&&i>=0&&(s[i].click(),r.preventDefault())}),document.addEventListener("click",r=>{e.contains(r.target)||o()}),e.append(l,d,p),m(),l}function E(e,t){let n=e.querySelector("input"),l=n.value;e.innerHTML="",v(e,n.name,t);let d=e.querySelector("input");d.value=l}var $=25,P=["active","triggered","cancelled"],k=null;function O(e){k?.(e)}function C(e){let t=0,n=null;e.innerHTML=`
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
      <button id="next">next \u2192</button></div>`;let l=e.querySelector("form"),d=e.querySelector("#rows"),p=e.querySelector("#pg"),g=e.querySelector("#prev"),m=e.querySelector("#next"),u=s=>e.querySelector(`[data-dd="${s}"]`),o=(s,i)=>[{value:"",label:i},...s.map(f=>({value:f,label:f}))];v(u("state"),"state",o(P,"state: any")),v(u("venue"),"venue",o([],"venue: any")),v(u("tier"),"tier",o([],"tier: any")),v(u("direction"),"direction",o(["ABOVE","BELOW"],"direction: any")),k=s=>{E(u("venue"),o(s.venues,"venue: any")),E(u("tier"),o(s.tiers,"tier: any")),k=null},a.hello&&k(a.hello);let c=()=>{g.disabled=t===0,m.disabled=!(n&&t+n.items.length<n.total)};async function r(){d.innerHTML='<tr><td colspan="8" class="empty">querying\u2026</td></tr>';let s={limit:String($),offset:String(t)};new FormData(l).forEach((i,f)=>s[f]=String(i));try{n=await I(s)}catch(i){n=null,d.innerHTML=`<tr><td colspan="8" class="empty err">query failed \u2014 ${i instanceof Error?i.message:String(i)}</td></tr>`,p.textContent="",c();return}d.innerHTML=n.items.length===0?'<tr><td colspan="8" class="empty">no alerts match</td></tr>':n.items.map(i=>`<tr>
              <td>${i.symbol}</td><td class="meta">${i.venue}/${i.tier}</td>
              <td class="meta">${i.price_type}</td>
              <td><span class="badge ${i.direction==="ABOVE"?"up":"down"}">${i.direction}</span></td>
              <td class="mono">${i.target_price}</td>
              <td><span class="chip state-${i.state}">${i.state}</span></td>
              <td class="mono time">${new Date(i.created_at_unix_nanos/1e6).toLocaleTimeString()}</td>
              <td class="mono meta">${i.id.slice(0,8)}</td>
            </tr>`).join(""),p.textContent=n.items.length===0?`0 of ${n.total}`:`${t+1}\u2013${t+n.items.length} of ${n.total}`,c()}l.addEventListener("submit",s=>{s.preventDefault(),t=0,r()}),g.addEventListener("click",()=>{t>0&&(t=Math.max(0,t-$),r())}),m.addEventListener("click",()=>{n&&t+n.items.length<n.total&&(t+=$,r())}),c(),r()}function R(){if(matchMedia("(prefers-reduced-motion: reduce)").matches)return;let e=document.createElement("div");e.className="intro",e.innerHTML='<span class="intro-mark">chrono<span class="brand-dot">\u25B8</span></span>',document.body.appendChild(e),setTimeout(()=>e.remove(),1800)}function V(){R(),C(B(document.getElementById("app")).inquiry);let e=new EventSource("/api/stream");e.onmessage=t=>{let n=JSON.parse(t.data);switch(n.type){case"hello":a.hello=n,L(a.hello.history),O(a.hello);break;case"snapshot":a.snapshot=n.snapshot,T(a.snapshot);break;case"triggers":H(n.triggers);break}A()}}V();})();
