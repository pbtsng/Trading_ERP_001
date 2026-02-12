self.addEventListener("install",e=>{
  e.waitUntil(
    caches.open("erp").then(c=>c.addAll(["/"]))
  );
});
