import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from personal_memory.configuration import load_settings
from personal_memory.hindsight_runtime import Runtime, normalize
from personal_memory.setup import install


class HindsightDefaultTests(unittest.TestCase):
    def test_setup_enables_managed_all_source_hindsight_without_opt_in(self):
        try:import yaml  # noqa: F401
        except ImportError:self.skipTest("PyYAML setup extra required")
        with tempfile.TemporaryDirectory() as tmp:
            home=Path(tmp);install(home)
            public=json.loads((home/"personal-memory/config.json").read_text())
            self.assertNotIn("enabled",public["retrieval"]["hindsight"])
            cfg=load_settings(home)
            hindsight=cfg["retrieval"]["hindsight"]
            self.assertTrue(hindsight["enabled"])
            self.assertTrue(hindsight["managed"])
            self.assertEqual(hindsight["sources"],["*"])

    def test_old_disabled_setting_is_migrated_to_enabled(self):
        try:import yaml  # noqa: F401
        except ImportError:self.skipTest("PyYAML setup extra required")
        with tempfile.TemporaryDirectory() as tmp:
            home=Path(tmp);install(home)
            path=home/"personal-memory/config.json"
            public=json.loads(path.read_text())
            public["retrieval"]["hindsight"]["enabled"]=False
            path.write_text(json.dumps(public))
            install(home)
            migrated=json.loads(path.read_text())
            self.assertNotIn("enabled",migrated["retrieval"]["hindsight"])
            self.assertTrue(load_settings(home)["retrieval"]["hindsight"]["enabled"])

    def test_external_runtime_is_supported_but_engine_cannot_be_disabled(self):
        cfg={"data_dir":"/tmp/example","retrieval":{"hindsight":{
            "enabled":False,"managed":False,"url":"https://memory.example"}}}
        normalize(cfg)
        self.assertTrue(cfg["retrieval"]["hindsight"]["enabled"])
        self.assertEqual(cfg["retrieval"]["hindsight"]["backend_id"],"external:https://memory.example")
        runtime=Runtime(cfg).start();self.assertIsNone(runtime.manager);runtime.close()

    def test_managed_runtime_injects_url_and_stops_owned_daemon(self):
        calls=[]
        class Manager:
            def ensure_running(self,config,profile):calls.append(("start",config,profile));return True
            def get_url(self,profile):return "http://127.0.0.1:9123"
            def stop(self,profile):calls.append(("stop",profile))
        module=types.ModuleType("hindsight_embed");module.get_embed_manager=lambda:Manager()
        cfg=normalize({"data_dir":"/tmp/example","retrieval":{"hindsight":{"llm_provider":"none"}}})
        with patch.dict(sys.modules,{"hindsight_embed":module}):
            runtime=Runtime(cfg).start()
            self.assertEqual(cfg["retrieval"]["hindsight"]["url"],"http://127.0.0.1:9123")
            self.assertEqual(cfg["retrieval"]["hindsight"]["runtime"]["llm_provider"],"none")
            runtime.close()
        self.assertEqual(calls[-1][0],"stop")

    def test_unknown_and_invalid_external_config_fail_closed(self):
        with self.assertRaises(ValueError):normalize({"data_dir":"/tmp/x","retrieval":{"hindsight":{"mystery":1}}})
        with self.assertRaises(ValueError):normalize({"data_dir":"/tmp/x","retrieval":{"hindsight":{"managed":False}}})
        with self.assertRaises(ValueError):normalize({"data_dir":"/tmp/x","retrieval":{"hindsight":{"profile":"../escape"}}})


if __name__=="__main__":unittest.main()
