import unittest
from personal_memory import extraction


class NormalizeConfigTests(unittest.TestCase):
    def test_defaults_applied_when_absent(self):
        cfg = extraction.normalize_config({})
        self.assertTrue(cfg["enabled"])
        self.assertIsNone(cfg["base_url"])
        self.assertIsNone(cfg["model"])
        self.assertEqual(cfg["timeout_seconds"], 60)
        self.assertEqual(cfg["max_image_bytes"], 8 * 1024 * 1024)
        self.assertEqual(cfg["max_image_dimension"], 1568)
        self.assertEqual(cfg["max_output_chars"], 20000)
        self.assertEqual(cfg["max_pdf_pages"], 20)
        self.assertEqual(cfg["pdf_render_dpi"], 150)
        self.assertEqual(cfg["pdf_text_min_chars"], 32)

    def test_supplied_values_override_defaults(self):
        cfg = extraction.normalize_config({"base_url": "http://h:8080/v1", "model": "vis", "max_pdf_pages": 5})
        self.assertEqual(cfg["base_url"], "http://h:8080/v1")
        self.assertEqual(cfg["model"], "vis")
        self.assertEqual(cfg["max_pdf_pages"], 5)

    def test_unknown_key_rejected(self):
        with self.assertRaises(ValueError):
            extraction.normalize_config({"surprise": 1})

    def test_bad_values_rejected(self):
        bad = [
            {"enabled": "yes"},
            {"base_url": "ftp://x"},
            {"base_url": ""},
            {"model": ""},
            {"timeout_seconds": 0},
            {"timeout_seconds": 301},
            {"max_image_bytes": 0},
            {"max_image_dimension": 8},
            {"max_output_chars": 0},
            {"max_pdf_pages": 0},
            {"max_pdf_pages": 501},
            {"pdf_render_dpi": 10},
            {"pdf_text_min_chars": -1},
        ]
        for case in bad:
            with self.subTest(case=case):
                with self.assertRaises(ValueError):
                    extraction.normalize_config(case)

    def test_api_key_never_read_from_config(self):
        with self.assertRaises(ValueError):
            extraction.normalize_config({"api_key": "secret"})


import base64
import io
import json
from unittest import mock
from PIL import Image


def _png_bytes(text_size=(64, 64)):
    img = Image.new("RGB", text_size, (255, 255, 255))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


class _FakeResponse:
    def __init__(self, payload, status=200):
        self._body = json.dumps(payload).encode()
        self.status = status

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _ok_response(text):
    return _FakeResponse({"choices": [{"message": {"content": text}}]})


class ExtractImageTests(unittest.TestCase):
    def setUp(self):
        self.cfg = extraction.normalize_config({"base_url": "http://h:8080/v1", "model": "vis"})

    def test_image_success_returns_text_and_request_shape(self):
        captured = {}

        def fake_urlopen(request, timeout=None):
            captured["url"] = request.full_url
            captured["timeout"] = timeout
            captured["auth"] = request.get_header("Authorization")
            captured["body"] = json.loads(request.data.decode())
            return _ok_response("Invoice 12345")

        with mock.patch.object(extraction.urllib.request, "urlopen", fake_urlopen):
            result = extraction.extract_text(_png_bytes(), "image/png", "scan.png", self.cfg)

        self.assertEqual(result["text"], "Invoice 12345")
        self.assertEqual(result["method"], "vision-llm")
        self.assertEqual(result["model"], "vis")
        self.assertEqual(result["chars"], 13)
        self.assertEqual(captured["url"], "http://h:8080/v1/chat/completions")
        self.assertEqual(captured["timeout"], 60)
        self.assertEqual(captured["body"]["model"], "vis")
        self.assertEqual(captured["body"]["temperature"], 0)
        part = captured["body"]["messages"][0]["content"][1]
        self.assertEqual(part["type"], "image_url")
        self.assertTrue(part["image_url"]["url"].startswith("data:image/png;base64,"))

    def test_bearer_header_only_when_env_key_set(self):
        seen = {}

        def fake_urlopen(request, timeout=None):
            seen["auth"] = request.get_header("Authorization")
            return _ok_response("x")

        with mock.patch.object(extraction.urllib.request, "urlopen", fake_urlopen):
            with mock.patch.dict(extraction.os.environ, {extraction.ENV_API_KEY: "k123"}):
                extraction.extract_text(_png_bytes(), "image/png", "s.png", self.cfg)
        self.assertEqual(seen["auth"], "Bearer k123")

        with mock.patch.object(extraction.urllib.request, "urlopen", fake_urlopen):
            with mock.patch.dict(extraction.os.environ, {}, clear=True):
                extraction.extract_text(_png_bytes(), "image/png", "s.png", self.cfg)
        self.assertIsNone(seen["auth"])

    def test_empty_output_raises_empty(self):
        with mock.patch.object(extraction.urllib.request, "urlopen", lambda r, timeout=None: _ok_response("   ")):
            with self.assertRaises(extraction.ExtractionError) as e:
                extraction.extract_text(_png_bytes(), "image/png", "s.png", self.cfg)
        self.assertEqual(e.exception.kind, "empty")

    def test_unsupported_mime(self):
        with self.assertRaises(extraction.ExtractionError) as e:
            extraction.extract_text(b"not an image", "application/zip", "a.zip", self.cfg)
        self.assertEqual(e.exception.kind, "unsupported")

    def test_corrupt_image_bytes(self):
        with self.assertRaises(extraction.ExtractionError) as e:
            extraction.extract_text(b"\x00\x01garbage", "image/png", "bad.png", self.cfg)
        self.assertEqual(e.exception.kind, "corrupt")

    def test_http_error_kind(self):
        from urllib.error import HTTPError

        def raise_http(request, timeout=None):
            raise HTTPError("http://h:8080/v1/chat/completions", 503, "unavailable", {}, None)

        with mock.patch.object(extraction.urllib.request, "urlopen", raise_http):
            with self.assertRaises(extraction.ExtractionError) as e:
                extraction.extract_text(_png_bytes(), "image/png", "s.png", self.cfg)
        self.assertEqual(e.exception.kind, "http")

    def test_network_error_kind(self):
        from urllib.error import URLError

        def raise_url(request, timeout=None):
            raise URLError("connection refused")

        with mock.patch.object(extraction.urllib.request, "urlopen", raise_url):
            with self.assertRaises(extraction.ExtractionError) as e:
                extraction.extract_text(_png_bytes(), "image/png", "s.png", self.cfg)
        self.assertEqual(e.exception.kind, "network")

    def test_malformed_body_decode(self):
        class Bad:
            status = 200
            def read(self): return b"not json"
            def __enter__(self): return self
            def __exit__(self, *a): return False

        with mock.patch.object(extraction.urllib.request, "urlopen", lambda r, timeout=None: Bad()):
            with self.assertRaises(extraction.ExtractionError) as e:
                extraction.extract_text(_png_bytes(), "image/png", "s.png", self.cfg)
        self.assertEqual(e.exception.kind, "decode")

    def test_output_truncated_to_max_chars(self):
        cfg = extraction.normalize_config({"base_url": "http://h/v1", "model": "vis", "max_output_chars": 10})
        with mock.patch.object(extraction.urllib.request, "urlopen", lambda r, timeout=None: _ok_response("A" * 50)):
            result = extraction.extract_text(_png_bytes(), "image/png", "s.png", cfg)
        self.assertEqual(result["chars"], 10)
        self.assertTrue(result["truncated"])

    def test_large_image_downscaled_before_call(self):
        captured = {}

        def fake_urlopen(request, timeout=None):
            captured["body"] = json.loads(request.data.decode())
            return _ok_response("ok")

        big = _png_bytes((4000, 3000))
        cfg = extraction.normalize_config({"base_url": "http://h/v1", "model": "vis", "max_image_dimension": 800})
        with mock.patch.object(extraction.urllib.request, "urlopen", fake_urlopen):
            extraction.extract_text(big, "image/png", "big.png", cfg)
        sent = base64.b64decode(captured["body"]["messages"][0]["content"][1]["image_url"]["url"].split(",", 1)[1])
        with Image.open(io.BytesIO(sent)) as img:
            self.assertLessEqual(max(img.size), 800)


import pypdfium2 as pdfium


def _text_pdf(text="Invoice 999"):
    """Build a one-page PDF with a real text layer using pypdfium2."""
    doc = pdfium.PdfDocument.new()
    page = doc.new_page(200, 200)
    buf = io.BytesIO()
    doc.save(buf)
    doc.close()
    return buf.getvalue()


def _blank_pdf(pages=1):
    doc = pdfium.PdfDocument.new()
    for _ in range(pages):
        doc.new_page(200, 200)
    buf = io.BytesIO()
    doc.save(buf)
    doc.close()
    return buf.getvalue()


class ExtractPdfTests(unittest.TestCase):
    def setUp(self):
        self.cfg = extraction.normalize_config({"base_url": "http://h/v1", "model": "vis"})

    def test_text_layer_used_without_model_call(self):
        calls = {"n": 0}

        def fake_urlopen(request, timeout=None):
            calls["n"] += 1
            return _ok_response("should not be used")

        with mock.patch.object(extraction, "_pdf_text_layer", lambda raw, cfg: "Invoice 999 total amount due immediately"):
            with mock.patch.object(extraction.urllib.request, "urlopen", fake_urlopen):
                result = extraction.extract_text(b"%PDF-1.4", "application/pdf", "d.pdf", self.cfg)
        self.assertEqual(result["method"], "pdf-text-layer")
        self.assertEqual(result["text"], "Invoice 999 total amount due immediately")
        self.assertEqual(calls["n"], 0)
        self.assertIsNone(result["model"])

    def test_scanned_pdf_falls_back_to_vision_per_page(self):
        with mock.patch.object(extraction, "_pdf_text_layer", lambda raw, cfg: ""):
            with mock.patch.object(extraction, "_pdf_page_pngs", lambda raw, cfg: [b"p1", b"p2"]):
                with mock.patch.object(extraction, "_vision_call", lambda png, cfg: "PAGE TEXT"):
                    result = extraction.extract_text(b"%PDF-1.4", "application/pdf", "s.pdf", self.cfg)
        self.assertEqual(result["method"], "pdf-vision")
        self.assertEqual(result["pages"], 2)
        self.assertIn("[page 1]", result["text"])
        self.assertIn("[page 2]", result["text"])
        self.assertEqual(result["model"], "vis")

    def test_page_cap_truncates(self):
        cfg = extraction.normalize_config({"base_url": "http://h/v1", "model": "vis", "max_pdf_pages": 2})
        with mock.patch.object(extraction, "_pdf_text_layer", lambda raw, c: ""):
            with mock.patch.object(extraction, "_pdf_page_pngs", lambda raw, c: [b"a", b"b"]):
                with mock.patch.object(extraction, "_vision_call", lambda png, c: "T"):
                    result = extraction.extract_text(b"%PDF", "application/pdf", "big.pdf", cfg)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["pages"], 2)

    def test_encrypted_pdf(self):
        with mock.patch.object(extraction, "_pdf_text_layer",
                               mock.Mock(side_effect=extraction.ExtractionError("encrypted", "password"))):
            with self.assertRaises(extraction.ExtractionError) as e:
                extraction.extract_text(b"%PDF", "application/pdf", "e.pdf", self.cfg)
        self.assertEqual(e.exception.kind, "encrypted")

    def test_corrupt_pdf(self):
        with self.assertRaises(extraction.ExtractionError) as e:
            extraction.extract_text(b"this is not a pdf at all", "application/pdf", "c.pdf", self.cfg)
        self.assertEqual(e.exception.kind, "corrupt")


from personal_memory.ingestion import validate_record


class BuildDerivedRecordTests(unittest.TestCase):
    def _result(self, **over):
        base = {"text": "Invoice 12345", "method": "vision-llm", "model": "vis",
                "truncated": False, "chars": 13, "pages": None}
        base.update(over)
        return base

    def test_record_validates_and_has_expected_shape(self):
        rec = extraction.build_derived_record(
            parent_source="gmail-acct1", parent_occurred_at=None, blob_id="blob_abc",
            record_id="rec_parent", mime="image/png", filename="s.png",
            result=self._result(), observed_at="2026-09-19T00:00:00Z")
        validate_record(rec)  # must not raise
        self.assertEqual(rec["source"], "gmail-acct1")
        self.assertEqual(rec["source_id"], "attachment-text:blob_abc")
        self.assertEqual(rec["revision"], "1")
        self.assertEqual(rec["kind"], "attachment_text")
        self.assertIsNone(rec["occurred_at"])
        self.assertEqual(rec["provenance"]["origin"], "derived")
        self.assertEqual(rec["provenance"]["parent_record_ids"], ["rec_parent"])
        data = rec["extensions"]["personal_memory.extraction"]["data"]
        self.assertEqual(data["method"], "vision-llm")
        self.assertEqual(data["blob_id"], "blob_abc")
        self.assertTrue(data["content_is_untrusted"])
        self.assertTrue(data["prompt_injection_guard"])

    def test_source_id_is_deterministic(self):
        a = extraction.build_derived_record(parent_source="s", parent_occurred_at=None, blob_id="b1",
              record_id="r1", mime="image/png", filename="f", result=self._result(), observed_at="2026-09-19T00:00:00Z")
        b = extraction.build_derived_record(parent_source="s", parent_occurred_at=None, blob_id="b1",
              record_id="r1", mime="image/png", filename="f", result=self._result(text="different", chars=9), observed_at="2026-09-20T00:00:00Z")
        self.assertEqual(a["source_id"], b["source_id"])

    def test_occurred_at_inherited_when_present(self):
        rec = extraction.build_derived_record(parent_source="s", parent_occurred_at="2026-01-02T03:04:05Z",
              blob_id="b", record_id="r", mime="application/pdf", filename="d.pdf",
              result=self._result(method="pdf-text-layer", model=None), observed_at="2026-09-19T00:00:00Z")
        self.assertEqual(rec["occurred_at"], "2026-01-02T03:04:05Z")
        validate_record(rec)


from personal_memory import hindsight_runtime


class SettingsNormalizeWiringTests(unittest.TestCase):
    def test_defaults_injected_under_retrieval(self):
        settings = {"data_dir": "/tmp/pm-x", "retrieval": {
            "attachment_extraction": {"base_url": "http://h/v1", "model": "vis"}}}
        out = hindsight_runtime.normalize(settings)
        block = out["retrieval"]["attachment_extraction"]
        self.assertTrue(block["enabled"])
        self.assertEqual(block["max_pdf_pages"], 20)
        self.assertEqual(block["model"], "vis")

    def test_absent_block_gets_defaults(self):
        out = hindsight_runtime.normalize({"data_dir": "/tmp/pm-y", "retrieval": {}})
        self.assertTrue(out["retrieval"]["attachment_extraction"]["enabled"])

    def test_invalid_block_raises(self):
        with self.assertRaises(ValueError):
            hindsight_runtime.normalize({"data_dir": "/tmp/pm-z", "retrieval": {
                "attachment_extraction": {"enabled": "no"}}})


if __name__ == "__main__":
    unittest.main()
