import re

def test_readme_configuration_defaults():
    with open("README.md", "r", encoding="utf-8") as f:
        content = f.read()

    # Check if there is a Default column or explicit default values mentioned for config options
    # This should fail currently because the configuration table lacks default values.
    table_match = re.search(r"\|\s*`wal_file_size`\s*\|", content)
    assert table_match is not None, "Configuration table not found in README"
    assert "Default" in content or "DEFAULT_" in content or re.search(r"\|\s*`wal_file_size`\s*\|[^|]+\|\s*\d+", content), "README configuration table is missing default values"
