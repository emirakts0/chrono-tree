"use strict";(()=>{var a={hello:null,snapshot:null,triggers:[],series:{ticks:[],fires:[],live:[],venues:{}}},U=120;function E(e,t){e.push(t),e.length>U&&e.shift()}function z(e){for(let t in k)delete k[t];a.series.ticks=[...e.t],a.series.fires=[...e.f],a.series.live=[...e.l],a.series.venues=Object.fromEntries(Object.entries(e.v).map(([t,n])=>[t,[...n]]))}var k={};function G(e,t){let n=a.series.venues[e]??=[],r=k[e]===void 0?0:t-k[e];k[e]=t,E(n,r)}function S(e){E(a.series.ticks,e.ticks_per_sec),E(a.series.fires,e.triggers_per_sec),E(a.series.live,e.engine.live);for(let[t,n]of Object.entries(e.venue_ticks))G(t,n)}function q(e){for(let t of e)a.triggers.unshift(t);a.triggers.length>10&&(a.triggers.length=10)}var v=e=>e>=1e9?`${(e/1e9).toFixed(1)}B`:e>=1e6?`${(e/1e6).toFixed(1)}M`:e>=1e4?`${(e/1e3).toFixed(1)}k`:e.toLocaleString();function D(e){let t=Math.floor(e/3600),n=Math.floor(e%3600/60),r=Math.floor(e%60);return t>0?`${t}h ${n}m`:n>0?`${n}m ${r}s`:`${r}s`}function B(e){return`<span class="dot ${e?"on":"off"}"></span>`}function A(e){if(e.length<2)return"";let t=100,n=28,r=Math.max(...e,1),o=e.map((s,u)=>`${u/(e.length-1)*t},${n-s/r*n}`);return`<svg viewBox="0 0 ${t} ${n}" preserveAspectRatio="none" role="img" aria-label="ticks per second, last two minutes">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" points="${o.join(" ")}"/>
  </svg>`}function _(e,t=100,n=16){if(e.length<2)return"";let r=Math.max(...e,1),o=Math.min(...e,0),s=r-o||1,u=e.map((b,f)=>{let d=f/(e.length-1)*t,g=n-(b-o)/s*n;return`${d.toFixed(1)},${g.toFixed(1)}`});return`<svg viewBox="0 0 ${t} ${n}" preserveAspectRatio="none" class="trend">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" points="${u.join(" ")}"/>
  </svg>`}function I(e){if(e.length<2)return"\u2014";let t=e[e.length-1]-e[e.length-2];return t>0?"\u25B2":t<0?"\u25BC":"\u2014"}function O(e){if(e.length<2)return"flat";let t=e[e.length-1]-e[e.length-2];return t>0?"up":t<0?"down":"flat"}var T={active:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="3.5" fill="none" stroke="currentColor" stroke-width="2"/><circle class="pulse" cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" stroke-width="1"/></svg>',triggered:'<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M9.5 1 3 9.2h3.6L6 15l6.3-8.2H8.7L9.5 1z" fill="currentColor"/></svg>',cancelled:'<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="5.5" fill="none" stroke="currentColor" stroke-width="1.5"/><line x1="3.4" y1="12.6" x2="12.6" y2="3.4" stroke="currentColor" stroke-width="1.5"/></svg>'};function C(e){e.innerHTML=`
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
        <div class="tile t-active"><span class="glyph">${T.active}</span><div class="big mono" id="st-active">\u2014</div><div class="sub">active</div><div class="tilespark" id="livetrendline"></div></div>
        <div class="tile t-triggered"><span class="glyph">${T.triggered}</span><div class="big mono" id="st-triggered">\u2014</div><div class="sub">triggered</div><div class="tilespark" id="firesspark"></div></div>
        <div class="tile t-cancelled"><span class="glyph">${T.cancelled}</span><div class="big mono" id="st-cancelled">\u2014</div><div class="sub">cancelled</div></div>
      </div>
    </section>
    <section class="card area-venues"><h2>venues</h2><div id="venues" class="rows"></div></section>
    <section class="card area-engine"><h2>engine</h2><div id="engine" class="rows"></div></section>
    <section class="card area-stream rail"><h2>trigger stream</h2><div class="stream" id="stream">
      <div class="empty">waiting for the first trigger\u2026</div></div></section>
    <section class="card area-inquiry" id="inquiry"></section>
  </main>`;let t=document.createElement("div");t.className="tip",document.body.appendChild(t);let n=document.getElementById("stream");return n.addEventListener("mouseover",r=>{let o=r.target.closest(".trg");if(!o?.dataset.tip)return;t.innerHTML=o.dataset.tip,t.style.opacity="1";let s=o.getBoundingClientRect();t.style.top=`${Math.round(s.top+s.height/2)}px`,t.style.transform="translateY(-50%)",s.right+330<window.innerWidth?(t.style.left=`${Math.round(s.right+10)}px`,t.style.transform="translateY(-50%)"):(t.style.left=`${Math.round(s.left-10)}px`,t.style.transform="translate(-100%, -50%)")}),n.addEventListener("mouseout",r=>{r.target.closest?.(".trg")&&(t.style.opacity="0")}),{inquiry:document.getElementById("inquiry")}}function j(){let e=a.snapshot,t=(d,g)=>{let i=document.getElementById(d);i&&(i.textContent=g)},n=(d,g)=>{let i=document.getElementById(d);i&&(i.innerHTML=B(g))};if(n("dot-feed",e?e.feed_ever_connected&&e.feed_last_seen_ms_ago<1e4:!1),n("dot-nats",!!e?.nats_connected),!e)return;t("published",v(e.triggers_published)),t("pubdropped",v(e.triggers_publish_dropped)),t("tickrate",v(e.ticks_per_sec)),t("ticks",v(e.ticks)),t("ticksdropped",v(e.ticks_dropped));let r=document.getElementById("spark");r&&(r.innerHTML=A(a.series.ticks)),t("st-active",String(e.alerts_by_state.active??0)),t("st-triggered",String(e.alerts_by_state.triggered??0)),t("st-cancelled",String(e.alerts_by_state.cancelled??0));let o=document.getElementById("firesspark");o&&(o.innerHTML=_(a.series.fires));let s=document.getElementById("livetrendline");s&&(s.innerHTML=_(a.series.live));let u=document.getElementById("venues");if(u){let d=Object.entries(e.venue_ticks).sort((g,i)=>i[1]-g[1]);u.innerHTML=d.map(([g,i])=>{let p=a.series.venues[g]??[];return`<div class="row">
          <span class="vname">${g} <span class="trendmark ${O(p)}">${I(p)}</span></span>
          <span class="vmeta">${_(p.slice(-40))}</span>
          <span class="mono">${v(i)}</span>
        </div>`}).join("")||'<div class="empty">no ticks yet</div>'}let b=document.getElementById("engine");b&&(b.innerHTML=`
      <div class="row"><span>ring drops</span><span class="mono">${v(e.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${a.hello?v(a.hello.symbol_count):"\u2014"}</span></div>
      <div class="row"><span>uptime</span><span class="mono">${D(e.uptime_sec)}</span></div>`);let f=document.getElementById("stream");if(f){let d=a.triggers[0],g=d?`${a.triggers.length}:${d.alert_id}:${d.fired_at_unix_nanos}`:"";g!==f.dataset.key&&(f.dataset.key=g,f.innerHTML=a.triggers.length===0?'<div class="empty">waiting for the first trigger\u2026</div>':a.triggers.map(i=>`<div class="trg" data-tip="<b>${i.symbol} \xB7 ${i.direction}</b><div class=tip-row><span class=mono>${i.fired_price}</span> (target <span class=mono>${i.target_price}</span>)</div><div class=tip-row>${i.venue}/${i.tier} \xB7 <span class=mono>${new Date(i.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span></div>">
                <span class="badge ${i.direction==="ABOVE"?"up":"down"}">${i.direction}</span>
                <span class="sym">${i.symbol}</span>
                <span class="mono">${i.fired_price}</span>
                <span class="meta">${i.venue}/${i.tier}</span>
                <span class="mono time">${new Date(i.fired_at_unix_nanos/1e6).toLocaleTimeString()}</span>
              </div>`).join(""))}}async function R(e){let t=new URLSearchParams(Object.entries(e).filter(([,r])=>r!=="")),n=await fetch(`/api/alerts?${t.toString()}`);if(!n.ok)throw new Error(`alerts: HTTP ${n.status}`);return await n.json()}function y(e,t,n){let r=document.createElement("input");r.type="hidden",r.name=t;let o=document.createElement("button");o.type="button",o.className="dd",o.setAttribute("aria-haspopup","listbox"),o.setAttribute("aria-expanded","false");let s=document.createElement("ul");s.className="dd-list",s.role="listbox",s.hidden=!0;let u="",b=()=>{o.innerHTML=`<span>${n.find(i=>i.value===u)?.label??""}</span><i class="chev">\u25BE</i>`;for(let i of s.children)i.classList.toggle("sel",i.dataset.v===u)};n.forEach(i=>{let p=document.createElement("li");p.role="option",p.dataset.v=i.value,p.tabIndex=-1,p.textContent=i.label,p.addEventListener("click",()=>{u=i.value,r.value=u,b(),d()}),s.appendChild(p)});let f=()=>{s.hidden=!1,o.setAttribute("aria-expanded","true"),s.querySelector(`[data-v="${u}"]`)?.focus()},d=()=>{s.hidden=!0,o.setAttribute("aria-expanded","false")},g=()=>s.hidden?f():d();return o.addEventListener("click",g),s.addEventListener("keydown",i=>{let p=[...s.children],m=p.indexOf(document.activeElement);i.key==="Escape"?(d(),o.focus()):i.key==="ArrowDown"&&m<p.length-1?p[m+1].focus():i.key==="ArrowUp"&&m>0?p[m-1].focus():i.key==="Enter"&&m>=0&&(p[m].click(),i.preventDefault())}),document.addEventListener("click",i=>{e.contains(i.target)||d()}),e.append(r,o,s),b(),r}function M(e,t){let n=e.querySelector("input"),r=n.value;e.innerHTML="",y(e,n.name,t);let o=e.querySelector("input");o.value=r}var F=[25,50,100,250,500],W=["active","triggered","cancelled"],$=null;function P(e){$?.(e)}function N(e){let t=0,n=F[0],r=null;e.innerHTML=`
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
        <select id="pgsize">${F.map(c=>`<option value="${c}"${c===n?" selected":""}>${c}</option>`).join("")}</select>
      </label>
      <button id="first" title="first page">\xAB</button>
      <button id="prev">\u2190 prev</button>
      <span id="pg" class="mono"></span>
      <button id="next">next \u2192</button>
      <button id="last" title="last page">\xBB</button>
    </div>`;let o=e.querySelector("form"),s=e.querySelector("#rows"),u=e.querySelector(".tblwrap"),b=e.querySelector("#pg"),f=e.querySelector("#first"),d=e.querySelector("#prev"),g=e.querySelector("#next"),i=e.querySelector("#last"),p=e.querySelector("#pgsize"),m=c=>e.querySelector(`[data-dd="${c}"]`),h=(c,l)=>[{value:"",label:l},...c.map(w=>({value:w,label:w}))];y(m("state"),"state",h(W,"state: any")),y(m("venue"),"venue",h([],"venue: any")),y(m("tier"),"tier",h([],"tier: any")),y(m("direction"),"direction",h(["ABOVE","BELOW"],"direction: any")),$=c=>{M(m("venue"),h(c.venues,"venue: any")),M(m("tier"),h(c.tiers,"tier: any")),$=null},a.hello&&$(a.hello);let Y=()=>r?Math.max(0,Math.floor((r.total-1)/n)*n):0,L=()=>{let c=t===0,l=!(r&&t+r.items.length<r.total);f.disabled=d.disabled=c,g.disabled=i.disabled=l},x=c=>{t=c,u.scrollTop=0,H()};async function H(){s.innerHTML='<tr><td colspan="8" class="empty">querying\u2026</td></tr>';let c={limit:String(n),offset:String(t)};new FormData(o).forEach((l,w)=>c[w]=String(l));try{r=await R(c)}catch(l){r=null,s.innerHTML=`<tr><td colspan="8" class="empty err">query failed \u2014 ${l instanceof Error?l.message:String(l)}</td></tr>`,b.textContent="",L();return}s.innerHTML=r.items.length===0?'<tr><td colspan="8" class="empty">no alerts match</td></tr>':r.items.map(l=>`<tr>
              <td>${l.symbol}</td><td class="meta">${l.venue}/${l.tier}</td>
              <td class="meta">${l.price_type}</td>
              <td><span class="badge ${l.direction==="ABOVE"?"up":"down"}">${l.direction}</span></td>
              <td class="mono">${l.target_price}</td>
              <td><span class="chip state-${l.state}">${l.state}</span></td>
              <td class="mono time">${new Date(l.created_at_unix_nanos/1e6).toLocaleTimeString()}</td>
              <td class="mono meta">${l.id.slice(0,8)}</td>
            </tr>`).join(""),b.textContent=r.items.length===0?`0 of ${r.total}`:`${t+1}\u2013${t+r.items.length} of ${r.total}`,L()}o.addEventListener("submit",c=>{c.preventDefault(),x(0)}),f.addEventListener("click",()=>{t>0&&x(0)}),d.addEventListener("click",()=>{t>0&&x(Math.max(0,t-n))}),g.addEventListener("click",()=>{r&&t+r.items.length<r.total&&x(t+n)}),i.addEventListener("click",()=>{r&&t+r.items.length<r.total&&x(Y())}),p.addEventListener("change",()=>{n=Number(p.value),x(0)}),L(),H()}function V(){if(matchMedia("(prefers-reduced-motion: reduce)").matches)return;let e=document.createElement("div");e.className="intro",e.innerHTML='<span class="intro-mark">chrono-tree<span class="brand-dot">\u25B8</span></span>',document.body.classList.add("introing"),document.body.appendChild(e),setTimeout(()=>{document.body.classList.remove("introing"),e.remove()},1500)}function K(){V(),N(C(document.getElementById("app")).inquiry);let e=new EventSource("/api/stream");e.onmessage=t=>{let n=JSON.parse(t.data);switch(n.type){case"hello":a.hello=n,z(a.hello.history),P(a.hello);break;case"snapshot":a.snapshot=n.snapshot,S(a.snapshot);break;case"triggers":q(n.triggers);break}j()}}K();})();
