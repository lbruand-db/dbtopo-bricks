"""Tests for the metadata module."""

import logging

from dbtopo.metadata import get_column_descriptions, get_table_description


class TestGetColumnDescriptions:
    def test_known_layer_returns_common_and_specific(self):
        descs = get_column_descriptions("batiment")
        # Common columns
        assert "cleabs" in descs
        assert "geometry" in descs
        assert "dept" in descs
        # Layer-specific columns
        assert "usage_1" in descs
        assert "hauteur" in descs

    def test_unknown_layer_returns_empty_with_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            descs = get_column_descriptions("nonexistent_layer_xyz")
        assert descs == {}
        assert "No metadata found for layer" in caplog.text

    def test_english_is_default(self):
        descs = get_column_descriptions("arrondissement")
        assert "Unique object identifier" in descs["cleabs"]

    def test_french_language(self):
        descs = get_column_descriptions("arrondissement", lang="fr")
        assert "Identifiant unique" in descs["cleabs"]

    def test_all_layers_have_table_descriptions(self):
        """Every layer in _LAYER_COLUMNS should also have a table description."""
        from dbtopo.metadata import _LAYER_COLUMNS, _TABLE_DESCRIPTIONS

        missing = set(_LAYER_COLUMNS.keys()) - set(_TABLE_DESCRIPTIONS.keys())
        assert missing == set(), f"Layers missing table descriptions: {missing}"

    def test_all_layers_have_both_languages(self):
        """Every table description should have both en and fr."""
        from dbtopo.metadata import _TABLE_DESCRIPTIONS

        for layer, translations in _TABLE_DESCRIPTIONS.items():
            assert "en" in translations, f"{layer} missing English description"
            assert "fr" in translations, f"{layer} missing French description"


class TestGetTableDescription:
    def test_known_layer(self):
        desc = get_table_description(
            "batiment", version="3-5", version_date="2025-09-15"
        )
        assert "IGN BD TOPO" in desc
        assert "v3-5" in desc
        assert "2025-09-15" in desc

    def test_unknown_layer_with_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            desc = get_table_description("nonexistent_xyz")
        assert "nonexistent_xyz" in desc
        assert "No table description found" in caplog.text

    def test_french_table_description(self):
        desc = get_table_description("batiment", lang="fr")
        assert "Batiments" in desc

    def test_english_table_description(self):
        desc = get_table_description("batiment", lang="en")
        assert "Buildings" in desc
