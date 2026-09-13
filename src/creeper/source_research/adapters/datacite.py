"""DataCite REST root with deterministic cursor traversal and metadata pivots."""
from __future__ import annotations
from .base import *
API="https://api.datacite.org/dois"
PROJECTION="id,types,titles,descriptions,publisher,publicationYear,creators,contributors,dates,client,provider,relatedIdentifiers,container,landingPage,contentUrl"
class DataCiteAdapter:
    root_id="datacite"
    def __init__(self,*,transport: JsonTransport,endpoint: str=API): self.transport,self.endpoint=transport,endpoint.rstrip("/")
    async def probe_capabilities(self):
        try:
            r=await self.transport(self.endpoint,{"page[size]":1},{})
            return RootCapabilityReport(self.root_id,not is_retryable(r),("cursor","projection","related_identifiers"),status_code=int(getattr(r,"status_code",200)))
        except Exception as exc: return RootCapabilityReport(self.root_id,False,reason=str(exc))
    async def search(self,query,checkpoint):
        cp=checkpoint or SearchCheckpoint(cursor="1"); url=cp.next_url or self.endpoint
        params={} if cp.next_url else {"query":query.query_text,"page[size]":min(1000,query.page_size),"page[cursor]":cp.cursor or "1","fields[dois]":PROJECTION,**query.native_filters}
        r=await self.transport(url,params,{"Accept":"application/vnd.api+json"})
        if is_retryable(r): return SearchPage(next_checkpoint=cp,terminal=False,retry_after=retry_after_seconds(r))
        payload=response_json(r); hits=[]; seen=set()
        for item in payload.get("data",[]) or []:
            attrs=item.get("attributes") or {}; doi=str(item.get("id") or attrs.get("doi") or "").removeprefix("doi:").strip().lower()
            if not doi or doi in seen: continue
            seen.add(doi); links=item.get("links") or {}
            hits.append(SearchHit(self.root_id,query.query_id,doi,links.get("self") or attrs.get("url") or "https://doi.org/"+doi,"DOI",_title(attrs),_description(attrs),_metadata(attrs,item)))
        nxt=(payload.get("links") or {}).get("next")
        return SearchPage(tuple(hits),next_checkpoint=None if not nxt else SearchCheckpoint(next_url=nxt),terminal=not bool(nxt),bytes_read=len(str(payload)))
    async def resolve(self,node):
        leads=tuple(ArtifactLead(self.root_id,node.provider_native_id,u,persistent_id=node.provider_native_id) for u in node.metadata.get("content_urls",()))
        return leads or (node,)
def _title(a): return str(((a.get("titles") or [{}])[0] or {}).get("title") or "")
def _description(a): return str(((a.get("descriptions") or [{}])[0] or {}).get("description") or "")
def _metadata(a,item):
    def ident(v): return v.get("id") if isinstance(v,dict) else v
    return {"publisher":a.get("publisher"),"publication_year":a.get("publicationYear"),"client":ident(a.get("client")),"provider":ident(a.get("provider")),"creators":a.get("creators") or [],"related_identifiers":a.get("relatedIdentifiers") or [],"landing_url":a.get("url") or (item.get("links") or {}).get("self"),"content_urls":tuple(a.get("contentUrl") or ())}
