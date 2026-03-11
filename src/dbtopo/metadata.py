"""IGN BD TOPO v3.5 metadata: table and column descriptions.

Descriptions sourced from the official IGN BD TOPO v3.5 data model
documentation (DC_BDTOPO_3-5.pdf, Novembre 2025).

Supports multiple languages (English and French). Default is English.

Themes (chapters 6-14):
  6. Administratif       9. Hydrographie      12. Services et activites
  7. Adresses           10. Lieux nommes      13. Transport
  8. Bati               11. Occupation du sol  14. Zones reglementees
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Type alias: {column_name: {lang: description}}
_LangDict = dict[str, dict[str, str]]

# --------------------------------------------------------------------------- #
# Common attributes shared across most BD TOPO themes (section 5)
# --------------------------------------------------------------------------- #
_COMMON_COLUMNS: _LangDict = {
    "cleabs": {
        "en": "Unique object identifier in BD TOPO",
        "fr": "Identifiant unique de l'objet dans la BD TOPO",
    },
    "date_creation": {
        "en": "Date of first entry in the IGN database",
        "fr": "Date de premiere saisie dans la base de donnees IGN",
    },
    "date_modification": {
        "en": "Date of last modification in the database",
        "fr": "Date de derniere modification dans la base de donnees",
    },
    "date_d_apparition": {
        "en": "Earliest known date confirming presence on the ground",
        "fr": "Date de creation la plus ancienne attestant la presence sur le terrain",
    },
    "date_de_confirmation": {
        "en": "Most recent date confirming presence on the ground",
        "fr": "Date la plus recente attestant la presence sur le terrain",
    },
    "etat_de_l_objet": {
        "en": "Object lifecycle state (Under construction / Planned / In service)",
        "fr": "Etat du cycle de vie de l'objet (En construction / En projet / En service)",
    },
    "sources": {
        "en": "Sources attesting the existence of the object",
        "fr": "Sources attestant l'existence de l'objet",
    },
    "identifiants_sources": {
        "en": "Object identifiers in the source organizations registries",
        "fr": "Identifiants de l'objet dans les repertoires des organismes consultes",
    },
    "methode_d_acquisition_planimetrique": {
        "en": "Planimetric geometry acquisition method",
        "fr": "Methode d'acquisition de la geometrie planimetrique",
    },
    "methode_d_acquisition_altimetrique": {
        "en": "Altimetric (Z) acquisition method",
        "fr": "Methode d'acquisition de l'altimetrie (Z)",
    },
    "precision_planimetrique": {
        "en": "Planimetric precision in meters",
        "fr": "Precision planimetrique en metres",
    },
    "precision_altimetrique": {
        "en": "Altimetric precision in meters",
        "fr": "Precision altimetrique en metres",
    },
    "geometry": {
        "en": "Native geometry (EPSG:4326), reprojected from Lambert-93 via ST_Transform",
        "fr": "Geometrie native (EPSG:4326), reprojetee depuis Lambert-93 via ST_Transform",
    },
    "dept": {
        "en": "Source department code (e.g. D001)",
        "fr": "Code departement source (ex: D001)",
    },
    "layer": {
        "en": "Source BD TOPO layer name",
        "fr": "Nom de la couche BD TOPO source",
    },
}

# --------------------------------------------------------------------------- #
# Theme-level shared column sets
# --------------------------------------------------------------------------- #
_ADMIN_COMMON: _LangDict = {
    "nom_officiel": {
        "en": "Official name of the administrative entity",
        "fr": "Nom officiel de l'entite administrative",
    },
    "code_insee": {
        "en": "INSEE code of the administrative entity",
        "fr": "Code INSEE de l'entite administrative",
    },
    "code_insee_du_departement": {
        "en": "INSEE code of the parent department",
        "fr": "Code INSEE du departement de rattachement",
    },
    "code_insee_de_la_region": {
        "en": "INSEE code of the parent region",
        "fr": "Code INSEE de la region de rattachement",
    },
    "liens_vers_autorite_administrative": {
        "en": "Link to the administrative authority (prefecture/sub-prefecture/town hall)",
        "fr": "Lien vers l'autorite administrative (prefecture/sous-prefecture/mairie)",
    },
}

_BATI_COMMON: _LangDict = {
    "nature": {
        "en": "Nature of the object",
        "fr": "Nature de l'objet",
    },
    "nature_detaillee": {
        "en": "Detailed nature of the object",
        "fr": "Nature detaillee de l'objet",
    },
    "toponyme": {
        "en": "Place name of the object",
        "fr": "Toponyme de l'objet",
    },
    "statut_du_toponyme": {
        "en": "Status of the place name (Official / Unofficial)",
        "fr": "Statut du toponyme (Officiel / Non officiel)",
    },
    "importance": {
        "en": "Importance of the object",
        "fr": "Importance de l'objet",
    },
    "fictif": {
        "en": "Indicates whether the object is fictitious",
        "fr": "Indique si l'objet est fictif",
    },
}

_HYDRO_COMMON: _LangDict = {
    "nature": {
        "en": "Nature of the hydrographic element",
        "fr": "Nature de l'element hydrographique",
    },
    "toponyme": {
        "en": "Place name of the hydrographic element",
        "fr": "Toponyme de l'element hydrographique",
    },
    "statut_du_toponyme": {
        "en": "Status of the place name",
        "fr": "Statut du toponyme",
    },
    "importance": {
        "en": "Importance of the element",
        "fr": "Importance de l'element",
    },
    "fictif": {
        "en": "Indicates whether the element is fictitious",
        "fr": "Indique si l'element est fictif",
    },
    "persistance": {
        "en": "Flow persistence (Permanent / Intermittent)",
        "fr": "Persistance de l'ecoulement (Permanent / Intermittent)",
    },
    "position_par_rapport_au_sol": {
        "en": "Position relative to the ground",
        "fr": "Position par rapport au sol",
    },
    "code_hydrographique": {
        "en": "National hydrographic code",
        "fr": "Code hydrographique national",
    },
}

_LIEUX_COMMON: _LangDict = {
    "nature": {
        "en": "Nature of the named place",
        "fr": "Nature du lieu nomme",
    },
    "nature_detaillee": {
        "en": "Detailed nature of the named place",
        "fr": "Nature detaillee du lieu nomme",
    },
    "toponyme": {
        "en": "Place name",
        "fr": "Toponyme du lieu nomme",
    },
    "statut_du_toponyme": {
        "en": "Status of the place name",
        "fr": "Statut du toponyme",
    },
    "importance": {
        "en": "Importance of the named place",
        "fr": "Importance du lieu nomme",
    },
}

_OCCUSOL_COMMON: _LangDict = {
    "nature": {
        "en": "Nature of the land cover",
        "fr": "Nature de l'occupation du sol",
    },
}

_SERVICES_COMMON: _LangDict = {
    "nature": {
        "en": "Nature of the service or activity",
        "fr": "Nature du service ou de l'activite",
    },
    "nature_detaillee": {
        "en": "Detailed nature of the service or activity",
        "fr": "Nature detaillee du service ou de l'activite",
    },
    "toponyme": {
        "en": "Place name of the service or activity",
        "fr": "Toponyme du service ou de l'activite",
    },
    "statut_du_toponyme": {
        "en": "Status of the place name",
        "fr": "Statut du toponyme",
    },
    "importance": {
        "en": "Importance of the service or activity",
        "fr": "Importance du service ou de l'activite",
    },
    "fictif": {
        "en": "Indicates whether the object is fictitious",
        "fr": "Indique si l'objet est fictif",
    },
}

_TRANSPORT_COMMON: _LangDict = {
    "nature": {
        "en": "Nature of the transport infrastructure",
        "fr": "Nature de l'infrastructure de transport",
    },
    "nature_detaillee": {
        "en": "Detailed nature of the transport infrastructure",
        "fr": "Nature detaillee de l'infrastructure de transport",
    },
    "toponyme": {
        "en": "Place name of the transport infrastructure",
        "fr": "Toponyme de l'infrastructure de transport",
    },
    "statut_du_toponyme": {
        "en": "Status of the place name",
        "fr": "Statut du toponyme",
    },
    "importance": {
        "en": "Importance of the infrastructure",
        "fr": "Importance de l'infrastructure",
    },
    "fictif": {
        "en": "Indicates whether the object is fictitious",
        "fr": "Indique si l'objet est fictif",
    },
    "position_par_rapport_au_sol": {
        "en": "Position relative to the ground",
        "fr": "Position par rapport au sol",
    },
}

_ZONES_REGL_COMMON: _LangDict = {
    "nature": {
        "en": "Nature of the regulated zone",
        "fr": "Nature de la zone reglementee",
    },
    "toponyme": {
        "en": "Place name of the regulated zone",
        "fr": "Toponyme de la zone reglementee",
    },
    "statut_du_toponyme": {
        "en": "Status of the place name",
        "fr": "Statut du toponyme",
    },
}


# --------------------------------------------------------------------------- #
# Helper to create bilingual entry
# --------------------------------------------------------------------------- #
def _bi(en: str, fr: str) -> dict[str, str]:
    return {"en": en, "fr": fr}


# --------------------------------------------------------------------------- #
# Layer-specific columns, organized by theme
# --------------------------------------------------------------------------- #
_LAYER_COLUMNS: dict[str, _LangDict] = {
    # ── Theme 6: Administratif ──────────────────────────────────────────────
    "arrondissement": {
        **_ADMIN_COMMON,
        "code_insee_de_l_arrondissement": _bi(
            "INSEE code of the arrondissement", "Code INSEE de l'arrondissement"
        ),
        "numero_de_l_arrondissement": _bi(
            "Official number assigned by INSEE", "Numero officiel attribue par l'INSEE"
        ),
    },
    "arrondissement_municipal": {
        **_ADMIN_COMMON,
        "code_postal": _bi("Postal code", "Code postal de l'arrondissement municipal"),
    },
    "canton": {
        **_ADMIN_COMMON,
    },
    "collectivite_territoriale": {
        **_ADMIN_COMMON,
        "nature": _bi(
            "Nature of the territorial authority",
            "Nature de la collectivite territoriale",
        ),
    },
    "commune": {
        **_ADMIN_COMMON,
        "statut": _bi(
            "Administrative status of the commune", "Statut administratif de la commune"
        ),
        "population": _bi("Population of the commune", "Population de la commune"),
        "code_postal": _bi(
            "Postal code (PLURI if multiple)", "Code postal (PLURI si pluricode)"
        ),
        "code_siren": _bi(
            "SIREN identifier (9 digits)", "Identifiant SIREN (9 chiffres)"
        ),
        "code_insee_de_l_arrondissement": _bi(
            "INSEE code of the arrondissement", "Code INSEE de l'arrondissement"
        ),
        "code_insee_de_la_collectivite_terr": _bi(
            "INSEE code of the territorial collectivity",
            "Code INSEE de la collectivite territoriale",
        ),
        "superficie_cadastrale": _bi(
            "Cadastral area in hectares", "Superficie cadastrale en hectares"
        ),
        "chef_lieu_d_arrondissement": _bi(
            "Is chef-lieu of arrondissement",
            "Indique si la commune est chef-lieu d'arrondissement",
        ),
        "chef_lieu_de_collectivite_terr": _bi(
            "Is chef-lieu of territorial collectivity",
            "Indique si la commune est chef-lieu de collectivite territoriale",
        ),
        "chef_lieu_de_departement": _bi(
            "Is chef-lieu of department",
            "Indique si la commune est chef-lieu de departement",
        ),
        "chef_lieu_de_region": _bi(
            "Is chef-lieu of region", "Indique si la commune est chef-lieu de region"
        ),
        "capitale_d_etat": _bi(
            "Is state capital", "Indique si la commune est la capitale de l'Etat"
        ),
        "date_du_recensement": _bi(
            "Census date for population figure", "Date du recensement"
        ),
        "codes_siren_des_epci": _bi(
            "SIREN codes of EPCIs the commune belongs to",
            "Codes SIREN des EPCI auxquels appartient la commune",
        ),
        "nom_de_la_commune_deleguee": _bi(
            "Name of the delegated commune", "Nom de la commune deleguee"
        ),
        "code_insee_de_la_commune_deleguee": _bi(
            "INSEE code of the delegated commune", "Code INSEE de la commune deleguee"
        ),
    },
    "commune_associee_ou_deleguee": {
        **_ADMIN_COMMON,
        "type": _bi(
            "Type of commune (associated or delegated)",
            "Type de commune (associee ou deleguee)",
        ),
        "code_insee_de_la_commune_de_rattachement": _bi(
            "INSEE code of the parent commune",
            "Code INSEE de la commune de rattachement",
        ),
    },
    "condominium": {
        **_ADMIN_COMMON,
    },
    "departement": {
        **_ADMIN_COMMON,
    },
    "epci": {
        **_ADMIN_COMMON,
        "code_siren": _bi("SIREN code of the EPCI", "Code SIREN de l'EPCI"),
        "nature": _bi("Legal nature of the EPCI", "Nature juridique de l'EPCI"),
    },
    "region": {
        **_ADMIN_COMMON,
    },
    # ── Theme 7: Adresses ───────────────────────────────────────────────────
    "adresse_ban": {
        "numero": _bi("Street number", "Numero dans la voie"),
        "repetition": _bi(
            "Repetition index (bis, ter, etc.)", "Indice de repetition (bis, ter, etc.)"
        ),
        "nom_de_la_voie": _bi("Street name", "Nom de la voie"),
        "code_postal": _bi("Postal code", "Code postal"),
        "nom_de_la_commune": _bi("Commune name", "Nom de la commune"),
        "code_insee_de_la_commune": _bi(
            "INSEE code of the commune", "Code INSEE de la commune"
        ),
        "id_ban": _bi(
            "BAN identifier (Base Adresse Nationale)",
            "Identifiant BAN (Base Adresse Nationale)",
        ),
        "type_de_localisation": _bi(
            "Address location type", "Type de localisation de l'adresse"
        ),
    },
    "lien_adresse_vers_bdtopo": {
        "id_ban": _bi("BAN identifier", "Identifiant BAN"),
        "cleabs_bdtopo": _bi(
            "BD TOPO identifier of the linked object",
            "Identifiant BD TOPO de l'objet lie",
        ),
        "type_de_lien": _bi(
            "Type of link between address and BD TOPO object",
            "Type de lien entre l'adresse et l'objet BD TOPO",
        ),
    },
    "voie_nommee": {
        "nom_minuscule": _bi("Street name in lowercase", "Nom de la voie en minuscule"),
        "nom_majuscule": _bi("Street name in uppercase", "Nom de la voie en majuscule"),
        "type_de_voie": _bi(
            "Street type (street, avenue, etc.)", "Type de voie (rue, avenue, etc.)"
        ),
        "code_insee_de_la_commune": _bi(
            "INSEE code of the commune", "Code INSEE de la commune"
        ),
    },
    # ── Theme 8: Bati ───────────────────────────────────────────────────────
    "batiment": {
        **_BATI_COMMON,
        "usage_1": _bi("Primary use of the building", "Usage principal du batiment"),
        "usage_2": _bi(
            "Secondary use of a mixed-function building",
            "Autre usage d'un batiment de fonction mixte",
        ),
        "construction_legere": _bi(
            "Lightweight structure not attached to the ground by foundations",
            "Structure legere non attachee au sol par des fondations",
        ),
        "nombre_de_logements": _bi(
            "Number of dwellings in the building",
            "Nombre de logements dans le batiment",
        ),
        "nombre_d_etages": _bi(
            "Total number of floors including ground floor",
            "Nombre total d'etages, RDC compris",
        ),
        "materiaux_des_murs": _bi(
            "Wall material code (from cadastral files)",
            "Code materiaux des murs (fichiers fonciers)",
        ),
        "materiaux_de_la_toiture": _bi(
            "Roof material code (from cadastral files)",
            "Code materiaux de la toiture (fichiers fonciers)",
        ),
        "hauteur": _bi(
            "Building height from ground to gutter in meters",
            "Hauteur du batiment mesuree entre le sol et la gouttiere en metres",
        ),
        "altitude_minimale_sol": _bi(
            "Ground altitude at the downhill foot of the building in meters",
            "Altitude au pied du batiment cote bas de la pente en metres",
        ),
        "altitude_minimale_toit": _bi(
            "Roof altitude at the outline edge in meters",
            "Altitude du toit au niveau de l'arete en metres",
        ),
        "altitude_maximale_toit": _bi(
            "Maximum roof altitude (ridge) in meters",
            "Altitude maximale du toit (faite) en metres",
        ),
        "altitude_maximale_sol": _bi(
            "Maximum ground altitude at the building foot in meters",
            "Altitude maximale au pied du batiment en metres",
        ),
        "origine_du_batiment": _bi(
            "Origin of building geometry (Cadastre / Aerial imagery / Other)",
            "Origine de la geometrie (Cadastre / Imagerie aerienne / Autre)",
        ),
        "appariement_fichiers_fonciers": _bi(
            "Quality of matching with cadastral files",
            "Qualite de l'appariement avec les fichiers fonciers",
        ),
        "identifiants_rnb": _bi(
            "Identifiers in the National Building Registry",
            "Identifiants dans le Registre National des Batiments",
        ),
    },
    "batiment_rnb_lien_bdtopo": {
        "identifiant_rnb": _bi(
            "RNB building identifier", "Identifiant RNB du batiment"
        ),
        "cleabs_batiment": _bi(
            "BD TOPO identifier of the linked building",
            "Identifiant BD TOPO du batiment lie",
        ),
    },
    "cimetiere": {**_BATI_COMMON},
    "construction_lineaire": {**_BATI_COMMON},
    "construction_ponctuelle": {**_BATI_COMMON},
    "construction_surfacique": {**_BATI_COMMON},
    "ligne_orographique": {**_BATI_COMMON},
    "pylone": {
        **_BATI_COMMON,
        "hauteur": _bi("Pylon height in meters", "Hauteur du pylone en metres"),
    },
    "reservoir": {
        **_BATI_COMMON,
        "hauteur": _bi("Reservoir height in meters", "Hauteur du reservoir en metres"),
        "volume": _bi("Reservoir volume", "Volume du reservoir"),
    },
    "terrain_de_sport": {**_BATI_COMMON},
    # ── Theme 9: Hydrographie ───────────────────────────────────────────────
    "bassin_versant_topographique": {
        "code_hydro": _bi(
            "Hydrographic code of the watershed",
            "Code hydrographique du bassin versant",
        ),
        "toponyme": _bi("Watershed place name", "Toponyme du bassin versant"),
        "statut_du_toponyme": _bi("Status of the place name", "Statut du toponyme"),
        "superficie": _bi(
            "Watershed area in km2", "Superficie du bassin versant en km2"
        ),
    },
    "cours_d_eau": {
        **_HYDRO_COMMON,
        "classe_de_largeur": _bi(
            "Width class of the watercourse", "Classe de largeur du cours d'eau"
        ),
    },
    "detail_hydrographique": {**_HYDRO_COMMON},
    "entite_de_transition": {**_HYDRO_COMMON},
    "limite_terre_mer": {
        "type_de_limite": _bi("Type of land-sea boundary", "Type de limite terre-mer"),
        "origine": _bi("Origin of the boundary", "Origine de la limite"),
    },
    "noeud_hydrographique": {
        **_HYDRO_COMMON,
        "categorie": _bi(
            "Category of the hydrographic node", "Categorie du noeud hydrographique"
        ),
    },
    "plan_d_eau": {
        **_HYDRO_COMMON,
        "hauteur_d_eau": _bi("Water height in meters", "Hauteur d'eau en metres"),
    },
    "surface_hydrographique": {**_HYDRO_COMMON},
    "troncon_hydrographique": {
        **_HYDRO_COMMON,
        "largeur": _bi(
            "Width of the section in meters", "Largeur du troncon en metres"
        ),
        "sens_de_l_ecoulement": _bi("Flow direction", "Sens d'ecoulement de l'eau"),
        "sens_d_ecoulement": _bi("Flow direction", "Sens d'ecoulement de l'eau"),
        "numero_d_ordre_de_strahler": _bi(
            "Strahler stream order number", "Numero d'ordre de Strahler"
        ),
        "numero_d_ordre": _bi(
            "Strahler stream order number", "Numero d'ordre de Strahler"
        ),
        "navigabilite": _bi("Navigability of the section", "Navigabilite du troncon"),
        "salinite": _bi(
            "Salinity (Freshwater / Brackish / Saltwater)",
            "Salinite (Douce / Saumatre / Salee)",
        ),
        "code_du_cours_d_eau": _bi("Watercourse code", "Code du cours d'eau"),
    },
    # ── Theme 10: Lieux nommes ──────────────────────────────────────────────
    "detail_orographique": {**_LIEUX_COMMON},
    "lieu_dit_non_habite": {**_LIEUX_COMMON},
    "toponymie": {**_LIEUX_COMMON},
    "zone_d_habitation": {
        **_LIEUX_COMMON,
        "code_postal": _bi("Postal code", "Code postal"),
        "population": _bi(
            "Population of the inhabited area", "Population de la zone d'habitation"
        ),
    },
    # ── Theme 11: Occupation du sol ─────────────────────────────────────────
    "haie": {**_OCCUSOL_COMMON},
    "zone_d_estran": {**_OCCUSOL_COMMON},
    "zone_de_vegetation": {**_OCCUSOL_COMMON},
    # ── Theme 12: Services et activites ─────────────────────────────────────
    "canalisation": {**_SERVICES_COMMON},
    "erp": {
        **_SERVICES_COMMON,
        "categorie": _bi(
            "ERP category (public-access establishment)",
            "Categorie de l'ERP (Etablissement Recevant du Public)",
        ),
        "capacite_accueil": _bi(
            "Capacity of the establishment", "Capacite d'accueil de l'ERP"
        ),
    },
    "ligne_electrique": {
        **_SERVICES_COMMON,
        "tension": _bi("Line voltage in kV", "Tension de la ligne en kV"),
    },
    "poste_de_transformation": {**_SERVICES_COMMON},
    "zone_d_activite_ou_d_interet": {
        **_SERVICES_COMMON,
        "categorie": _bi(
            "Category of the activity or interest zone",
            "Categorie de la zone d'activite ou d'interet",
        ),
    },
    # ── Theme 13: Transport ─────────────────────────────────────────────────
    "aerodrome": {
        **_TRANSPORT_COMMON,
        "code_iata": _bi("IATA code of the aerodrome", "Code IATA de l'aerodrome"),
        "code_icao": _bi("ICAO code of the aerodrome", "Code ICAO de l'aerodrome"),
    },
    "equipement_de_transport": {**_TRANSPORT_COMMON},
    "itineraire_autre": {**_TRANSPORT_COMMON},
    "non_communication": {
        "sens": _bi(
            "Direction of the non-communication", "Sens de la non-communication"
        ),
    },
    "piste_d_aerodrome": {**_TRANSPORT_COMMON},
    "point_d_acces": {**_TRANSPORT_COMMON},
    "point_de_repere": {**_TRANSPORT_COMMON},
    "point_du_reseau": {
        **_TRANSPORT_COMMON,
        "nature_du_noeud": _bi(
            "Nature of the network node", "Nature du noeud de reseau"
        ),
    },
    "route_numerotee_ou_nommee": {
        **_TRANSPORT_COMMON,
        "numero": _bi("Route number", "Numero de la route"),
        "gestionnaire": _bi("Route manager", "Gestionnaire de la route"),
        "type_de_route": _bi(
            "Route type (motorway, national, departmental, etc.)",
            "Type de route (autoroute, nationale, departementale, etc.)",
        ),
    },
    "section_de_points_de_repere": {
        "route": _bi("Route identifier", "Identifiant de la route"),
        "departement": _bi("Department of the section", "Departement de la section"),
    },
    "transport_par_cable": {**_TRANSPORT_COMMON},
    "troncon_de_route": {
        **_TRANSPORT_COMMON,
        "numero": _bi("Route number", "Numero de la route"),
        "nom_voie_ban_gauche": _bi(
            "BAN street name on the left side", "Nom de la voie BAN cote gauche"
        ),
        "nom_voie_ban_droite": _bi(
            "BAN street name on the right side", "Nom de la voie BAN cote droite"
        ),
        "nom_rue_gauche": _bi(
            "Street name on the left side", "Nom de la rue cote gauche"
        ),
        "nom_rue_droite": _bi(
            "Street name on the right side", "Nom de la rue cote droite"
        ),
        "nombre_de_voies": _bi(
            "Number of traffic lanes", "Nombre de voies de circulation"
        ),
        "largeur_de_chaussee": _bi(
            "Road width in meters", "Largeur de la chaussee en metres"
        ),
        "sens_de_circulation": _bi(
            "Authorized traffic direction (Double sens / Sens direct / Sens inverse)",
            "Sens de circulation autorise (Double sens / Sens direct / Sens inverse)",
        ),
        "vitesse_moyenne_vl": _bi(
            "Average speed of light vehicles in km/h",
            "Vitesse moyenne des vehicules legers en km/h",
        ),
        "acces_vehicule_leger": _bi(
            "Light vehicle access authorized", "Acces autorise aux vehicules legers"
        ),
        "acces_pieton": _bi("Pedestrian access authorized", "Acces pieton autorise"),
        "bande_cyclable": _bi(
            "Presence of a cycle lane", "Presence d'une bande cyclable"
        ),
        "reserve_aux_bus": _bi("Reserved for buses", "Reserve aux bus"),
        "urbain": _bi("Located in urban area", "Situe en zone urbaine"),
        "itineraire_vert": _bi(
            "Part of a green route", "Fait partie d'un itineraire vert"
        ),
        "prive": _bi("Private road", "Voie privee"),
        "date_de_mise_en_service": _bi(
            "Planned opening date", "Date prevue de mise en service"
        ),
        "nature_de_la_restriction": _bi(
            "Type of traffic restriction", "Nature de la restriction de circulation"
        ),
        "restriction_de_hauteur": _bi(
            "Height restriction in meters", "Restriction de hauteur en metres"
        ),
        "restriction_de_poids_total": _bi(
            "Total weight restriction in tonnes", "Restriction de poids total en tonnes"
        ),
        "restriction_de_poids_par_essieu": _bi(
            "Per-axle weight restriction in tonnes",
            "Restriction de poids par essieu en tonnes",
        ),
        "restriction_de_largeur": _bi(
            "Width restriction in meters", "Restriction de largeur en metres"
        ),
        "restriction_de_longueur": _bi(
            "Length restriction in meters", "Restriction de longueur en metres"
        ),
        "cpx_numero": _bi(
            "Route number (denormalized)", "Numero de la route (denormalise)"
        ),
        "cpx_classement_administratif": _bi(
            "Administrative road classification", "Classement administratif de la route"
        ),
    },
    "troncon_de_voie_ferree": {
        **_TRANSPORT_COMMON,
        "nombre_de_voies": _bi("Number of railway tracks", "Nombre de voies ferrees"),
        "largeur_de_voie": _bi("Track gauge in meters", "Largeur de voie en metres"),
        "electrifie": _bi("Electrified line", "Ligne electrifiee"),
        "vitesse_maximale": _bi(
            "Maximum authorized speed in km/h", "Vitesse maximale autorisee en km/h"
        ),
        "usage": _bi(
            "Track usage (Freight / HSR / Main / Service / Urban transit)",
            "Usage du troncon (Fret / LGV / Principal / Service / Transport urbain)",
        ),
    },
    "voie_ferree_nommee": {**_TRANSPORT_COMMON},
    # ── Theme 14: Zones reglementees ────────────────────────────────────────
    "foret_publique": {**_ZONES_REGL_COMMON},
    "parc_ou_reserve": {**_ZONES_REGL_COMMON},
}

# --------------------------------------------------------------------------- #
# Table-level descriptions: {layer: {lang: description}}
# --------------------------------------------------------------------------- #
_TABLE_DESCRIPTIONS: dict[str, dict[str, str]] = {
    # Theme 6: Administratif
    "arrondissement": _bi(
        "Arrondissements — administrative subdivisions between department and commune (IGN BD TOPO)",
        "Arrondissements — decoupage administratif intermediaire entre departement et commune (IGN BD TOPO)",
    ),
    "arrondissement_municipal": _bi(
        "Municipal arrondissements of Paris, Lyon, Marseille (IGN BD TOPO)",
        "Arrondissements municipaux de Paris, Lyon, Marseille (IGN BD TOPO)",
    ),
    "canton": _bi(
        "Cantons — electoral subdivisions of the department (IGN BD TOPO)",
        "Cantons — subdivisions electorales du departement (IGN BD TOPO)",
    ),
    "collectivite_territoriale": _bi(
        "Territorial authorities — authorities with special status (IGN BD TOPO)",
        "Collectivites territoriales — collectivites a statut particulier (IGN BD TOPO)",
    ),
    "commune": _bi(
        "Communes — smallest administrative subdivision in France (IGN BD TOPO)",
        "Communes — plus petite subdivision administrative francaise (IGN BD TOPO)",
    ),
    "commune_associee_ou_deleguee": _bi(
        "Associated or delegated communes (IGN BD TOPO)",
        "Communes associees ou deleguees (IGN BD TOPO)",
    ),
    "condominium": _bi(
        "Condominiums — internationally co-owned territories (IGN BD TOPO)",
        "Condominiums — territoires en copropriete internationale (IGN BD TOPO)",
    ),
    "departement": _bi(
        "Departments — administrative subdivisions of France (IGN BD TOPO)",
        "Departements — subdivision administrative de la France (IGN BD TOPO)",
    ),
    "epci": _bi(
        "Inter-communal cooperation public bodies (IGN BD TOPO)",
        "Etablissements publics de cooperation intercommunale (IGN BD TOPO)",
    ),
    "region": _bi(
        "Regions — first-level administrative subdivisions (IGN BD TOPO)",
        "Regions — subdivision administrative de premier niveau (IGN BD TOPO)",
    ),
    # Theme 7: Adresses
    "adresse_ban": _bi(
        "Addresses from the Base Adresse Nationale (IGN BD TOPO)",
        "Adresses issues de la Base Adresse Nationale (IGN BD TOPO)",
    ),
    "lien_adresse_vers_bdtopo": _bi(
        "Links between BAN addresses and BD TOPO objects (IGN BD TOPO)",
        "Liens entre adresses BAN et objets BD TOPO (IGN BD TOPO)",
    ),
    "voie_nommee": _bi(
        "Named roads — road segments with names (IGN BD TOPO)",
        "Voies nommees — segments de voies avec denomination (IGN BD TOPO)",
    ),
    # Theme 8: Bati
    "batiment": _bi(
        "Buildings — constructions raised above ground level (IGN BD TOPO)",
        "Batiments — constructions elevees au-dessus du niveau du sol (IGN BD TOPO)",
    ),
    "batiment_rnb_lien_bdtopo": _bi(
        "Links between RNB and BD TOPO buildings (IGN BD TOPO)",
        "Liens entre batiments RNB et BD TOPO (IGN BD TOPO)",
    ),
    "cimetiere": _bi("Cemeteries (IGN BD TOPO)", "Cimetieres (IGN BD TOPO)"),
    "construction_lineaire": _bi(
        "Linear constructions — walls, fences, quays (IGN BD TOPO)",
        "Constructions lineaires — murs, clotures, quais (IGN BD TOPO)",
    ),
    "construction_ponctuelle": _bi(
        "Point constructions — turrets, chimneys, etc. (IGN BD TOPO)",
        "Constructions ponctuelles — tourelles, cheminees, etc. (IGN BD TOPO)",
    ),
    "construction_surfacique": _bi(
        "Surface constructions — pools, covered parking, etc. (IGN BD TOPO)",
        "Constructions surfaciques — piscines, parkings couverts, etc. (IGN BD TOPO)",
    ),
    "ligne_orographique": _bi(
        "Orographic lines — ridge lines, thalwegs (IGN BD TOPO)",
        "Lignes orographiques — lignes de crete, talweg (IGN BD TOPO)",
    ),
    "pylone": _bi(
        "Pylons — power line or telecom supports (IGN BD TOPO)",
        "Pylones — supports de lignes electriques ou de telecoms (IGN BD TOPO)",
    ),
    "reservoir": _bi(
        "Reservoirs — fluid storage structures (IGN BD TOPO)",
        "Reservoirs — ouvrages de stockage de fluides (IGN BD TOPO)",
    ),
    "terrain_de_sport": _bi(
        "Sports grounds — outdoor sports facilities (IGN BD TOPO)",
        "Terrains de sport — equipements sportifs de plein air (IGN BD TOPO)",
    ),
    # Theme 9: Hydrographie
    "bassin_versant_topographique": _bi(
        "Topographic watersheds (IGN BD TOPO)",
        "Bassins versants topographiques (IGN BD TOPO)",
    ),
    "cours_d_eau": _bi(
        "Watercourses — linear hydrographic sections (IGN BD TOPO)",
        "Cours d'eau — troncons hydrographiques lineaires (IGN BD TOPO)",
    ),
    "detail_hydrographique": _bi(
        "Hydrographic details — springs, waterfalls, etc. (IGN BD TOPO)",
        "Details hydrographiques — sources, cascades, etc. (IGN BD TOPO)",
    ),
    "entite_de_transition": _bi(
        "Transition entities — estuaries, deltas (IGN BD TOPO)",
        "Entites de transition — estuaires, deltas (IGN BD TOPO)",
    ),
    "limite_terre_mer": _bi(
        "Land-sea boundaries — coastline (IGN BD TOPO)",
        "Limites terre-mer — trait de cote (IGN BD TOPO)",
    ),
    "noeud_hydrographique": _bi(
        "Hydrographic nodes — confluences, springs (IGN BD TOPO)",
        "Noeuds hydrographiques — confluences, sources (IGN BD TOPO)",
    ),
    "plan_d_eau": _bi(
        "Water bodies — permanent water surfaces (IGN BD TOPO)",
        "Plans d'eau — surfaces en eau permanentes (IGN BD TOPO)",
    ),
    "surface_hydrographique": _bi(
        "Hydrographic surfaces — water areas (IGN BD TOPO)",
        "Surfaces hydrographiques — zones en eau (IGN BD TOPO)",
    ),
    "troncon_hydrographique": _bi(
        "Hydrographic sections — hydrographic network segments (IGN BD TOPO)",
        "Troncons hydrographiques — segments du reseau hydrographique (IGN BD TOPO)",
    ),
    # Theme 10: Lieux nommes
    "detail_orographique": _bi(
        "Orographic details — peaks, passes, caves (IGN BD TOPO)",
        "Details orographiques — pics, cols, grottes (IGN BD TOPO)",
    ),
    "lieu_dit_non_habite": _bi(
        "Uninhabited named places (IGN BD TOPO)", "Lieux-dits non habites (IGN BD TOPO)"
    ),
    "toponymie": _bi(
        "Place names — geographic names (IGN BD TOPO)",
        "Toponymes — noms geographiques (IGN BD TOPO)",
    ),
    "zone_d_habitation": _bi(
        "Inhabited areas — towns, hamlets (IGN BD TOPO)",
        "Zones d'habitation — agglomerations, hameaux (IGN BD TOPO)",
    ),
    # Theme 11: Occupation du sol
    "haie": _bi(
        "Hedges — linear vegetation elements (IGN BD TOPO)",
        "Haies — elements lineaires de vegetation (IGN BD TOPO)",
    ),
    "zone_d_estran": _bi(
        "Foreshore zones — areas between high and low tide (IGN BD TOPO)",
        "Zones d'estran — zones entre maree haute et basse (IGN BD TOPO)",
    ),
    "zone_de_vegetation": _bi(
        "Vegetation zones — wooded or natural areas (IGN BD TOPO)",
        "Zones de vegetation — espaces naturels boises ou non (IGN BD TOPO)",
    ),
    # Theme 12: Services et activites
    "canalisation": _bi(
        "Pipelines — fluid conduits (IGN BD TOPO)",
        "Canalisations — conduites de fluides (IGN BD TOPO)",
    ),
    "erp": _bi(
        "Public-access establishments (IGN BD TOPO)",
        "Etablissements recevant du public (IGN BD TOPO)",
    ),
    "ligne_electrique": _bi(
        "Power lines (IGN BD TOPO)", "Lignes electriques (IGN BD TOPO)"
    ),
    "poste_de_transformation": _bi(
        "Electrical substations (IGN BD TOPO)",
        "Postes de transformation electrique (IGN BD TOPO)",
    ),
    "zone_d_activite_ou_d_interet": _bi(
        "Activity or interest zones — industrial, commercial areas, etc. (IGN BD TOPO)",
        "Zones d'activite ou d'interet — zones industrielles, commerciales, etc. (IGN BD TOPO)",
    ),
    # Theme 13: Transport
    "aerodrome": _bi(
        "Aerodromes — airports and airfields (IGN BD TOPO)",
        "Aerodromes — aeroports et aerodromes (IGN BD TOPO)",
    ),
    "equipement_de_transport": _bi(
        "Transport equipment — stations, stops, etc. (IGN BD TOPO)",
        "Equipements de transport — gares, stations, etc. (IGN BD TOPO)",
    ),
    "itineraire_autre": _bi(
        "Other routes — trails, cycle paths (IGN BD TOPO)",
        "Itineraires autres — sentiers, pistes cyclables (IGN BD TOPO)",
    ),
    "non_communication": _bi(
        "Non-communications — no-turn restrictions (IGN BD TOPO)",
        "Non-communications — interdictions de tourner (IGN BD TOPO)",
    ),
    "piste_d_aerodrome": _bi(
        "Aerodrome runways (IGN BD TOPO)", "Pistes d'aerodrome (IGN BD TOPO)"
    ),
    "point_d_acces": _bi(
        "Access points — motorway entries/exits, interchanges (IGN BD TOPO)",
        "Points d'acces — entrees/sorties d'autoroute, echangeurs (IGN BD TOPO)",
    ),
    "point_de_repere": _bi(
        "Reference points — markers, kilometer posts (IGN BD TOPO)",
        "Points de repere — bornes, points kilometriques (IGN BD TOPO)",
    ),
    "point_du_reseau": _bi(
        "Network points — road network nodes (IGN BD TOPO)",
        "Points du reseau — noeuds du reseau routier (IGN BD TOPO)",
    ),
    "route_numerotee_ou_nommee": _bi(
        "Numbered or named roads — motorways, national roads, etc. (IGN BD TOPO)",
        "Routes numerotees ou nommees — autoroutes, nationales, etc. (IGN BD TOPO)",
    ),
    "section_de_points_de_repere": _bi(
        "Reference point sections (IGN BD TOPO)",
        "Sections de points de repere (IGN BD TOPO)",
    ),
    "transport_par_cable": _bi(
        "Cable transport — cable cars, gondolas (IGN BD TOPO)",
        "Transports par cable — telepheriques, telecabines (IGN BD TOPO)",
    ),
    "troncon_de_route": _bi(
        "Road sections — road network segments (IGN BD TOPO)",
        "Troncons de route — segments du reseau routier (IGN BD TOPO)",
    ),
    "troncon_de_voie_ferree": _bi(
        "Railway sections — railway network segments (IGN BD TOPO)",
        "Troncons de voie ferree — segments du reseau ferre (IGN BD TOPO)",
    ),
    "voie_ferree_nommee": _bi(
        "Named railways (IGN BD TOPO)", "Voies ferrees nommees (IGN BD TOPO)"
    ),
    # Theme 14: Zones reglementees
    "foret_publique": _bi(
        "Public forests — state and communal forests (IGN BD TOPO)",
        "Forets publiques — forets domaniales et communales (IGN BD TOPO)",
    ),
    "parc_ou_reserve": _bi(
        "Parks or nature reserves (IGN BD TOPO)",
        "Parcs ou reserves naturelles (IGN BD TOPO)",
    ),
}


def get_column_descriptions(layer: str, lang: str = "en") -> dict[str, str]:
    """Return column descriptions for a given BD TOPO layer.

    Merges common attributes with layer-specific ones.
    Columns not found in the metadata registry are skipped with a warning.

    Parameters
    ----------
    layer : str
        Layer name (e.g. "batiment", "arrondissement").
    lang : str
        Language code ("en" or "fr"). Defaults to "en".
    """
    if layer not in _LAYER_COLUMNS:
        logger.warning(
            "No metadata found for layer '%s' — column comments will be skipped", layer
        )
        return {}

    descs: dict[str, str] = {}
    for col, translations in _COMMON_COLUMNS.items():
        descs[col] = translations.get(lang, translations.get("en", ""))
    for col, translations in _LAYER_COLUMNS[layer].items():
        descs[col] = translations.get(lang, translations.get("en", ""))
    return descs


def get_table_description(
    layer: str, version: str = "3-5", version_date: str = "", lang: str = "en"
) -> str:
    """Return a table-level description for a BD TOPO layer.

    Parameters
    ----------
    layer : str
        Layer name.
    version : str
        BD TOPO version string.
    version_date : str
        Edition date string.
    lang : str
        Language code ("en" or "fr"). Defaults to "en".
    """
    if layer in _TABLE_DESCRIPTIONS:
        base = _TABLE_DESCRIPTIONS[layer].get(
            lang, _TABLE_DESCRIPTIONS[layer].get("en", "")
        )
    else:
        logger.warning("No table description found for layer '%s'", layer)
        base = (
            f"IGN BD TOPO — layer {layer}"
            if lang == "en"
            else f"IGN BD TOPO — couche {layer}"
        )

    parts = [base, f"v{version}"]
    if version_date:
        parts.append(
            f"edition {version_date}" if lang == "en" else f"edition {version_date}"
        )
    return ", ".join(parts)
