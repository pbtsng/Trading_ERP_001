function loadSO(el){
let so = el.value;
let row = el.closest("tr");

fetch("/get_so/"+so)
.then(res=>res.json())
.then(data=>{
row.querySelector('[name="qty"]').value = data.balance;
});
}

function loadPO(el){
let po = el.value;
let row = el.closest("tr");

fetch("/get_po/"+po)
.then(res=>res.json())
.then(data=>{
row.querySelector('[name="item"]').value = data.item;
});
}

document.addEventListener("DOMContentLoaded",()=>{
  const btn = document.getElementById("menuBtn");
  const overlay = document.getElementById("overlay");
  const helpBtn = document.getElementById("helpBtn");
  const helpMenuParent = helpBtn ? helpBtn.parentElement : null;
  const collapseBtn = document.getElementById("collapseBtn");
  const collapsibles = document.querySelectorAll(".menu-title.collapsible");
  const rightToggle = document.getElementById("rightToggle");
  const bottomToggle = document.getElementById("bottomToggle");
  const tabsBar = document.getElementById("tabsBar");
  const aiForm = document.getElementById("aiForm");
  const aiInput = document.getElementById("aiInput");
  const aiStatus = document.getElementById("aiStatus");
  const aiAnswer = document.getElementById("aiAnswer");
  const aiSql = document.getElementById("aiSql");
  const themeSelect = document.getElementById("themeSelect");
  const densitySelect = document.getElementById("densitySelect");
  function toggle(){
    document.body.classList.toggle("sidebar-open");
  }
  if(btn) btn.addEventListener("click", toggle);
  if(overlay) overlay.addEventListener("click", toggle);
  if(collapseBtn){
    collapseBtn.addEventListener("click",()=>{
      document.body.classList.toggle("sidebar-collapsed");
    });
  }
  collapsibles.forEach(title=>{
    title.addEventListener("click",()=>{
      const id = title.getAttribute("data-target");
      const el = document.getElementById(id);
      if(el){
        el.classList.toggle("open");
      }
    });
  });
  if(helpBtn && helpMenuParent){
    helpBtn.addEventListener("click",(e)=>{
      e.stopPropagation();
      helpMenuParent.classList.toggle("open");
    });
    document.addEventListener("click",()=>{
      helpMenuParent.classList.remove("open");
    });
  }
  if(rightToggle){
    rightToggle.addEventListener("click",()=>{
      document.body.classList.toggle("show-right");
    });
  }
  if(bottomToggle){
    bottomToggle.addEventListener("click",()=>{
      document.body.classList.toggle("show-bottom");
    });
  }
  function routeLabel(path){
    const map = {
      "/dashboard":"Dashboard",
      "/sale":"Sale Order",
      "/purchase":"Purchase Order",
      "/loading_advice":"Loading Advice",
      "/control_report":"Reports",
      "/ai":"AI Assistant"
    };
    return map[path] || path;
  }
  function renderTabs(){
    if(!tabsBar) return;
    const tabs = JSON.parse(localStorage.getItem("erpTabs")||"[]");
    tabsBar.innerHTML = "";
    const current = window.location.pathname;
    tabs.forEach((t,idx)=>{
      const el = document.createElement("div");
      el.className = "tab"+(t.path===current?" active":"");
      const link = document.createElement("a");
      link.href = t.path;
      link.textContent = t.label;
      const close = document.createElement("button");
      close.className = "close";
      close.textContent = "×";
      close.addEventListener("click",(e)=>{
        e.preventDefault();
        const arr = JSON.parse(localStorage.getItem("erpTabs")||"[]");
        const currentPath = window.location.pathname;
        const removed = arr.splice(idx,1);
        localStorage.setItem("erpTabs",JSON.stringify(arr));
        // If the closed tab is the current view, navigate to a neighbor or dashboard
        if(removed.length && removed[0].path === currentPath){
          const target = arr[idx-1]?.path || arr[idx]?.path || "/dashboard";
          window.location.href = target;
          return;
        }
        renderTabs();
      });
      el.appendChild(link);
      el.appendChild(close);
      tabsBar.appendChild(el);
    });
  }
  function ensureCurrentTab(){
    if(!tabsBar) return;
    const current = window.location.pathname;
    let tabs = JSON.parse(localStorage.getItem("erpTabs")||"[]");
    if(!tabs.find(t=>t.path===current)){
      tabs.push({path: current, label: routeLabel(current)});
      if(tabs.length>8) tabs = tabs.slice(tabs.length-8);
      localStorage.setItem("erpTabs", JSON.stringify(tabs));
    }
    renderTabs();
  }
  ensureCurrentTab();
  if(aiForm && aiInput){
    aiForm.addEventListener("submit",(e)=>{
      e.preventDefault();
      const q = aiInput.value.trim();
      if(!q) return;
      aiStatus.textContent = "Working…";
      fetch("/ask_ai",{
        method:"POST",
        headers:{"Content-Type":"application/x-www-form-urlencoded"},
        body:"question="+encodeURIComponent(q)
      })
      .then(r=>r.json())
      .then(d=>{
        if(d.error){
          aiStatus.textContent = "Error";
          aiAnswer.textContent = d.error;
          aiSql.textContent = d.sql||"";
        }else{
          aiStatus.textContent = "Done";
          aiAnswer.textContent = d.answer;
          aiSql.textContent = d.sql;
        }
        document.body.classList.add("show-right");
      })
      .catch(()=>{
        aiStatus.textContent = "Error";
      });
    });
  }
  function applyTheme(name){
    if(name==="light"){
      document.documentElement.removeAttribute("data-theme");
    }else{
      document.documentElement.setAttribute("data-theme", name);
    }
    localStorage.setItem("erpTheme", name);
    if(themeSelect){ themeSelect.value = name; }
    document.dispatchEvent(new CustomEvent("themechange",{detail:{theme:name}}));
  }
  const serverTheme = themeSelect ? themeSelect.value : null;
  const savedTheme = localStorage.getItem("erpTheme") || serverTheme || "light";
  applyTheme(savedTheme);
  if(themeSelect){
    themeSelect.addEventListener("change",()=>{
      applyTheme(themeSelect.value);
    });
  }
  function applyDensity(mode){
    document.documentElement.setAttribute("data-density", mode);
    localStorage.setItem("erpDensity", mode);
    if(densitySelect){ densitySelect.value = mode; }
  }
  const serverDensity = densitySelect ? densitySelect.value : null;
  const savedDensity = localStorage.getItem("erpDensity") || serverDensity || "compact";
  applyDensity(savedDensity);
  if(densitySelect){
    densitySelect.addEventListener("change",()=>{
      applyDensity(densitySelect.value);
    });
  }
  
});
