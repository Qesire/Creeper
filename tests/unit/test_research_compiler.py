from __future__ import annotations
import unittest
from creeper.source_discovery.research_compiler import ResearchCompiler, ResearchCompilerError

def region(**overrides):
    item = {
        "proposal_id":"r1","surface_kind":"FILENAME_FAMILY","root":"HTTPS://Example.test/a/../catalog",
        "purpose":"enumerate annual shards","query_family":{"YEAR":[1996,1997,1998,1999,2000,2001],"SHARD":["00","01"]},
        "enumerator":"FILENAME_PATTERN","artifact_predicate":{"suffix":[".cdxj.gz"]},
        "hard_bounds":{"max_items":100},"stop_conditions":["after max_items","on terminal 404"],
        "expected_source_family":"archive-index","expected_contract_family":"CDXJ","expected_mechanism":"year/shard family",
        "expected_fanout":1200,"confidence":0.8,"validation":{"method":"HEAD_OR_RANGE"}
    }
    item.update(overrides)
    return item

class ResearchCompilerTests(unittest.TestCase):
    def test_compiles_and_normalizes_finite_region(self):
        plan=ResearchCompiler().compile_response({"query":"q","regions":[region()]})[0]
        self.assertEqual(plan.root,"https://example.test/catalog")
        self.assertEqual(plan.expected_fanout,1200)
        self.assertEqual(plan.state.value,"VALIDATED")
        self.assertTrue(plan.region_key.startswith("region:"))

    def test_rejects_unbounded_or_low_fanout(self):
        with self.assertRaises(ResearchCompilerError):
            ResearchCompiler().compile_response({"query":"q","regions":[region(hard_bounds={})]})
        with self.assertRaisesRegex(ResearchCompilerError,"minimum"):
            ResearchCompiler().compile_response({"query":"q","regions":[region(expected_fanout=1, query_family={"YEAR":[1999]}, hard_bounds={"max_items":1})]})

    def test_rejects_common_crawl_and_negative_knowledge(self):
        with self.assertRaises(ResearchCompilerError):
            ResearchCompiler().compile_response({"query":"q","regions":[region(root="https://data.commoncrawl.org/index") ]})
        with self.assertRaises(ResearchCompilerError):
            ResearchCompiler(negative_knowledge=lambda root: True).compile_response({"query":"q","regions":[region()]})

    def test_accepts_catalog_exception_below_minimum(self):
        plan=ResearchCompiler().compile_response({"query":"q","regions":[region(expected_fanout=1, surface_kind="MANIFEST", hard_bounds={"max_requests":1})]})[0]
        self.assertEqual(plan.expected_fanout,1)

if __name__=="__main__":
    unittest.main()
