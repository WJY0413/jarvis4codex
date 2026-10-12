"""Exact current17-tool registry/schema and explicit pre-release version checks."""
import copy
import json
from pathlib import Path
from types import SimpleNamespace
import tomllib
import unittest
from jsonschema import Draft202012Validator
from jarvis_mcp.server import JarvisMcpServer
from jarvis_control import operations,file_task,contact_task

VERSION='0.3.0'
ROOT=Path(__file__).resolve().parents[1]
TOOLS={'jarvis_contract_check','jarvis_receipt','jarvis_callback','jarvis_relay','jarvis_read_delivery','jarvis_capacity','jarvis_dispatch',
       'jarvis_create','jarvis_hold','jarvis_loop','jarvis_close','jarvis_read','jarvis_resume','jarvis_monitor','jarvis_heartbeat','jarvis_update','jarvis_notify'}
class CurrentRegistryVersions(unittest.TestCase):
    def test_exact_seventeen_tools_preserve_slice4_public_schema(self):
        server=JarvisMcpServer(SimpleNamespace());current={tool.name:tool.parameters for tool in server.mcp._tool_manager.list_tools()}
        self.assertEqual(set(current),TOOLS);self.assertEqual(len(current),17)
        loop=current['jarvis_loop']['properties']
        self.assertEqual(loop['action']['enum'],['preflight','start','status','stop','schema_register','schema_read'])
        new_fields={'schema_id','schema_version','schema_sha256','schema','schema_json'}
        self.assertTrue(new_fields.issubset(loop))
        verification=loop['threads']['anyOf'][0]['items']['properties']['lane']['properties']['result_verification']
        reference=verification['properties']['schema_ref']
        self.assertEqual(set(reference['required']),{'schema_id','schema_version','schema_sha256'})
        self.assertEqual(reference['properties']['schema_sha256']['pattern'],r'^[0-9a-fA-F]{64}$')
        validator=Draft202012Validator(verification)
        valid={'schema_ref':{'schema_id':'result','schema_version':'1','schema_sha256':'a'*64}}
        self.assertTrue(validator.is_valid(valid))
        for invalid in ({'schema_ref':{'schema_id':'result','schema_version':'1'}},
                        {'schema_ref':{**valid['schema_ref'],'schema_sha256':None}},
                        {**valid,'output_schema':{'type':'object'}}):
            self.assertFalse(validator.is_valid(invalid))
        legacy=copy.deepcopy(current)
        legacy_loop=legacy['jarvis_loop']['properties']
        legacy_loop['action']['enum']=['preflight','start','status','stop']
        for field in new_fields:legacy_loop.pop(field)
        legacy_verification=legacy_loop['threads']['anyOf'][0]['items']['properties']['lane']['properties']['result_verification']
        legacy_verification['properties'].pop('schema_ref',None)
        legacy_verification.pop('not',None)
        baseline=json.loads((ROOT/'qa/slice4-registry-schema-baseline.json').read_text())
        legacy_loop['threads']['description']=baseline['jarvis_loop']['properties']['threads']['description']
        self.assertEqual(legacy,baseline)
        self.assertNotIn('target_capacity',current['jarvis_capacity']['properties'])
        self.assertNotIn('load_for_read',current['jarvis_read']['properties'])
    def test_distribution_and_advertised_mcp_version_are_same_prerelease(self):
        version=tomllib.loads((ROOT/'pyproject.toml').read_text())['project']['version']
        self.assertEqual(version,VERSION);self.assertEqual(JarvisMcpServer(SimpleNamespace()).mcp._lowlevel_server.version,VERSION)
    def test_all_product_builds_use_the_same_candidate_version(self):
        builds={operations.BUILD,file_task.BUILD,contact_task.BUILD}
        self.assertEqual(builds,{'Jarvis '+VERSION+' Linux (candidate)'})
