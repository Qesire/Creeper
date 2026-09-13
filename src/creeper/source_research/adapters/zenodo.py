"""Zenodo REST root with bounded page traversal and file leads."""
from __future__ import annotations
from .base import *
API="https://zenodo.org/api/records/"
def recognize_archives_unleashed(title,filenames,schema_text=""):
    t,n,s=str(title).lower()," ".join(map(str,filenames)).lower(),str(schema_text).lower()
    return ("web archive collection derivatives" in t and (any(x.endswith(("-auk.tar.gz","-parquet.tar.gz")) for x in n.split()) or all(k in s for k in ("crawl_date","src","dest","anchor")))) or (any(x.endswith(("-auk.tar.gz","-parquet.tar.gz")) for x in n.split()) and all(k in s for k in ("crawl_date","src","dest","anchor")))
class ZenodoAdapter:
    root_id="zenodo"
    def __init__(self,*,transport: JsonTransport,endpoint: str=API,token: str|None=None): self.transport,self.endpoint,self.token=transport,endpoint if endpoint.endswith("/") else endpoint+"/",token
    def _headers(self): return {"Authorization":"Bearer "+self.token} if self.token else {}
    async def probe_capabilities(self):
        r=await self.transport(self.endpoint,{"page":1,"size":1},self._headers())
        return RootCapabilityReport(self.root_id,not is_retryable(r),("records","files","versions"),status_code=int(getattr(r,"status_code",200)))
    async def search(self,query,checkpoint):
        cp=checkpoint or SearchCheckpoint(page=1); page=cp.page
        p={} if cp.next_url else {"q":query.query_text,"page":page,"size":min(query.page_size,100 if self.token else 25),"all_versions":True,**query.native_filters}
        r=await self.transport(cp.next_url or self.endpoint,p,self._headers())
        if is_retryable(r): return SearchPage(next_checkpoint=cp,terminal=False,retry_after=retry_after_seconds(r))
        payload=response_json(r); rows=((payload.get("hits") or {}).get("hits") or []); hits=[]; leads=[]; seen=set()
        for row in rows:
            rid=str(row.get("id") or "")
            if not rid or rid in seen: continue
            seen.add(rid); md=row.get("metadata") or {}; links=row.get("links") or {}; files=row.get("files") or []
            hits.append(SearchHit(self.root_id,query.query_id,rid,links.get("self") or "https://zenodo.org/records/"+rid,"RECORD",str(md.get("title") or ""),str(md.get("description") or ""),{"doi":md.get("doi"),"conceptrecid":row.get("conceptrecid"),"version":md.get("version"),"authors":md.get("creators") or [],"files":files,"family_prior":recognize_archives_unleashed(md.get("title",""),[f.get("key") or f.get("filename","") for f in files],md.get("description",""))}))
            for f in files:
                key=str(f.get("key") or f.get("filename") or ""); loc=(f.get("links") or {}).get("self") or f.get("link")
                if key and loc: leads.append(ArtifactLead(self.root_id,rid+":"+key,loc,str(f.get("type") or ""),_int(f.get("size")),f.get("checksum"),str(md.get("doi") or "") or None,str(row.get("conceptrecid") or "") or None))
        total=int((payload.get("hits") or {}).get("total") or 0); nxt=page+1 if rows and page*len(rows)<total else None
        return SearchPage(tuple(hits),tuple(leads),None if nxt is None else SearchCheckpoint(page=nxt),nxt is None,bytes_read=len(str(payload)))
    async def resolve(self,node): return (node,)
def _int(v):
    try:return int(v) if v is not None else None
    except (TypeError,ValueError):return None
