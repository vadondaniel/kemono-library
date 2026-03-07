from kemono_library.web import _prettify_content_for_edit


def test_prettify_preserves_br_only_paragraph_structure():
    raw = "<p>\n<br>\n</p>"
    pretty = _prettify_content_for_edit(raw)
    assert pretty == raw


def test_prettify_splits_adjacent_sibling_tags():
    raw = "<p>One</p><p>Two</p><a href=\"x\">x</a><p>Three</p>"
    pretty = _prettify_content_for_edit(raw)
    assert "<p>One</p>\n<p>Two</p>" in pretty
    assert "</a>\n<p>Three</p>" in pretty
