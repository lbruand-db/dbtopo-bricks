from pathlib import Path

import pytest
import yaml


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def expected_features():
    path = FIXTURES_DIR / "expected_features.yaml"
    if not path.exists():
        pytest.skip("No expected_features.yaml fixture file found")
    with open(path) as f:
        return yaml.safe_load(f)
