"""Dataverse REST root with deterministic dataset/file search."""
from __future__ import annotations
from .base import *
class DataverseAdapter:
    def __init__(self,instance: str,*,transport: JsonTransport): self.instance=instance.rstrip("/"); self.transport=transport; self.root_id="dataverse:"+self.instance.split("://",1)[-1]
    async def probe_capabilities(self):
        try:
            r=await self.transport(self.instance+"/api/search",{"q":"*","type":"dataset","per_page":1,"start":0,"show_api_urls":"true"},{})
            return RootCapabilityReport(self.root_id,not is_retryable(r),("dataset_search","file_search","persistent_ids"),status_code=int(getattr(r,"status_code",200)))
        except Exception as exc:return RootCapabilityReport(self.root_id,False,reason=str(exc))
    async def search(self,query,checkpoint):
        cp=checkpoint or SearchCheckpoint(start=0,query_variant="dataset"); p={"q":query.query_text,"type":cp.query_variant or "dataset","per_page":min(1000,query.page_size),"start":cp.start,"show_api_urls":"true",**query.native_filters}
        r=await self.transport(self.instance+"/api/search",p,{})
        if is_retryable(r):return SearchPage(next_checkpoint=cp,terminal=False,retry_after=retry_after_seconds(r))
        payload=response_json(r); items=((payload.get("data") or {}).get("items") or []); hits=[]; leads=[]; seen=set()
        for item in items:
            typ=str(item.get("type") or "").lower(); df=item.get("dataFile") or {}; ident=str(df.get("id") or item.get("global_id") or item.get("entity_id") or "")
            if not ident or (typ,ident) in seen:continue
            seen.add((typ,ident))
            if typ=="file":
                pid=str(df.get("persistentId") or ""); dpid=str(df.get("datasetPersistentId") or ""); loc=self.instance+"/api/access/datafile/"+str(df.get("id"))
                leads.append(ArtifactLead(self.root_id,"file:"+ident,loc,str(df.get("contentType") or ""),_int(df.get("filesize")),df.get("md5"),pid or None,dpid or None))
                hits.append(SearchHit(self.root_id,query.query_id,ident,loc,"FILE",str(df.get("filename") or ""),metadata={"persistent_id":pid,"dataset_persistent_id":dpid,"size":df.get("filesize"),"md5":df.get("md5")}))
            else:hits.append(SearchHit(self.root_id,query.query_id,ident,str(item.get("url") or ""),"DATASET",str(item.get("name") or ""),metadata={"global_id":item.get("global_id"),"publication_date":item.get("published_at")}))
        count=len(items); total=int((payload.get("data") or {}).get("total_count") or 0); start=cp.start+count; terminal=count==0 or start>=total or count<p["per_page"]
        return SearchPage(tuple(hits),tuple(leads),None if terminal else SearchCheckpoint(start=start,query_variant=cp.query_variant),terminal,bytes_read=len(str(payload)))
    async def resolve(self,node):return (node,)
def _int(v):
    try:return int(v) if v is not None else None
    except (TypeError,ValueError):return None
