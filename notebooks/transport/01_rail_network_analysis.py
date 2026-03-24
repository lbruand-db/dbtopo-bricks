# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Rail & Transport Network Analysis
# MAGIC
# MAGIC **Use case**: Transport operators (SNCF, RATP) need to analyze rail infrastructure,
# MAGIC station surroundings, road-rail crossings, and flood risk exposure for their networks.
# MAGIC
# MAGIC This notebook demonstrates how BD TOPO enables:
# MAGIC 1. Mapping the complete rail network
# MAGIC 2. Station catchment area analysis (buildings & population zones)
# MAGIC 3. Road-rail crossing inventory
# MAGIC 4. Flood risk assessment along rail corridors
# MAGIC 5. Land use around planned infrastructure

# COMMAND ----------

dbutils.widgets.text("catalog", "lucasbruand_catalog")
dbutils.widgets.text("schema", "ign_bdtopo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
prefix = f"{catalog}.{schema}.ign_bdtopo_"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Rail Network Overview
# MAGIC
# MAGIC BD TOPO provides detailed railway geometry including track type,
# MAGIC electrification, gauge, and operational status.

# COMMAND ----------

# Railway sections by type
rail_overview = spark.sql(f"""
    SELECT
        dept,
        nature,
        nature_detaillee,
        etat_de_l_objet,
        COUNT(*) AS segment_count,
        ROUND(SUM(ST_Length(geometry)) * 111.32, 1) AS approx_length_km
    FROM {prefix}troncon_de_voie_ferree_dedup
    GROUP BY dept, nature, nature_detaillee, etat_de_l_objet
    ORDER BY approx_length_km DESC
""")

display(rail_overview)

# COMMAND ----------

# Named railway lines
named_lines = spark.sql(f"""
    SELECT
        cleabs,
        toponyme,
        nature,
        importance,
        ST_AsText(geometry) AS wkt
    FROM {prefix}voie_ferree_nommee_dedup
    WHERE toponyme IS NOT NULL
    ORDER BY importance
    LIMIT 200
""")

display(named_lines)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Station Catchment Area Analysis
# MAGIC
# MAGIC How many buildings and inhabitants are within walking distance of each station?
# MAGIC Transport equipment includes stations, stops, and platforms.

# COMMAND ----------

# Transport equipment — stations and stops
stations = spark.sql(f"""
    SELECT
        cleabs,
        nature,
        nature_detaillee,
        toponyme,
        dept,
        ST_AsText(geometry) AS wkt
    FROM {prefix}equipement_de_transport_dedup
    WHERE nature_detaillee LIKE '%gare%'
       OR nature_detaillee LIKE '%station%'
       OR nature LIKE '%Gare%'
    ORDER BY dept
    LIMIT 200
""")

display(stations)

# COMMAND ----------

# Buildings within ~500m of railway stations (Lyon area)
station_catchment = spark.sql(f"""
    SELECT
        s.toponyme AS station_name,
        s.nature_detaillee AS station_type,
        COUNT(DISTINCT b.cleabs) AS building_count,
        s.dept
    FROM {prefix}equipement_de_transport_dedup s
    JOIN {prefix}batiment_dedup b
        ON ST_Intersects(
            ST_Buffer(s.geometry, 0.005),  -- ~500m
            b.geometry
        )
    WHERE s.dept = b.dept
      AND s.dept = 'D069'
      AND (s.nature_detaillee LIKE '%gare%'
           OR s.nature_detaillee LIKE '%station%'
           OR s.nature LIKE '%Gare%')
    GROUP BY s.toponyme, s.nature_detaillee, s.dept
    ORDER BY building_count DESC
    LIMIT 50
""")

display(station_catchment)

# COMMAND ----------

# Inhabited areas near stations
station_population = spark.sql(f"""
    SELECT
        s.toponyme AS station_name,
        z.toponyme AS area_name,
        z.nature AS area_type,
        z.importance AS area_importance,
        s.dept
    FROM {prefix}equipement_de_transport_dedup s
    JOIN {prefix}zone_d_habitation_dedup z
        ON ST_Intersects(
            ST_Buffer(s.geometry, 0.01),  -- ~1km
            z.geometry
        )
    WHERE s.dept = z.dept
      AND s.dept = 'D069'
      AND (s.nature_detaillee LIKE '%gare%'
           OR s.nature LIKE '%Gare%')
    ORDER BY s.toponyme
    LIMIT 100
""")

display(station_population)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Road-Rail Crossing Inventory
# MAGIC
# MAGIC Level crossings are critical safety points. Identify where roads
# MAGIC intersect with the rail network.

# COMMAND ----------

road_rail_crossings = spark.sql(f"""
    SELECT
        r.nature AS road_type,
        r.nature_detaillee AS road_detail,
        t.nature AS rail_type,
        r.dept,
        COUNT(*) AS crossing_count
    FROM {prefix}troncon_de_route_dedup r
    JOIN {prefix}troncon_de_voie_ferree_dedup t
        ON ST_Intersects(r.geometry, t.geometry)
    WHERE r.dept = t.dept
      AND r.dept = 'D069'
    GROUP BY r.nature, r.nature_detaillee, t.nature, r.dept
    ORDER BY crossing_count DESC
""")

display(road_rail_crossings)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Rail Corridor Flood Risk
# MAGIC
# MAGIC Railways near watercourses and water bodies are vulnerable to flooding.
# MAGIC Assess flood exposure by overlaying hydro layers with the rail network.

# COMMAND ----------

# Rail segments near watercourses
rail_flood_risk = spark.sql(f"""
    SELECT
        t.nature AS rail_type,
        w.toponyme AS watercourse_name,
        w.persistance AS flow_type,
        t.dept,
        COUNT(*) AS intersection_count,
        ROUND(SUM(ST_Length(t.geometry)) * 111.32, 2) AS exposed_length_km
    FROM {prefix}troncon_de_voie_ferree_dedup t
    JOIN {prefix}cours_d_eau_dedup w
        ON ST_Intersects(
            ST_Buffer(w.geometry, 0.003),  -- ~300m buffer
            t.geometry
        )
    WHERE t.dept = w.dept
      AND t.dept = 'D069'
    GROUP BY t.nature, w.toponyme, w.persistance, t.dept
    ORDER BY exposed_length_km DESC
    LIMIT 30
""")

display(rail_flood_risk)

# COMMAND ----------

# Rail near water bodies (lakes, reservoirs)
rail_near_water = spark.sql(f"""
    SELECT
        t.nature AS rail_type,
        p.toponyme AS water_body_name,
        p.nature AS water_body_type,
        t.dept,
        COUNT(*) AS segment_count
    FROM {prefix}troncon_de_voie_ferree_dedup t
    JOIN {prefix}plan_d_eau_dedup p
        ON ST_Intersects(
            ST_Buffer(p.geometry, 0.003),
            t.geometry
        )
    WHERE t.dept = p.dept
      AND t.dept = 'D069'
    GROUP BY t.nature, p.toponyme, p.nature, t.dept
    ORDER BY segment_count DESC
    LIMIT 30
""")

display(rail_near_water)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Land Use Around Rail Corridors
# MAGIC
# MAGIC What type of land surrounds the rail network? Useful for
# MAGIC environmental impact studies and new line planning.

# COMMAND ----------

rail_land_use = spark.sql(f"""
    SELECT
        v.nature AS land_cover,
        t.nature AS rail_type,
        t.dept,
        COUNT(*) AS zone_count,
        ROUND(SUM(ST_Area(v.geometry)) * 111.32 * 111.32, 2) AS approx_area_km2
    FROM {prefix}zone_de_vegetation_dedup v
    JOIN {prefix}troncon_de_voie_ferree_dedup t
        ON ST_Intersects(
            ST_Buffer(t.geometry, 0.005),  -- ~500m corridor
            v.geometry
        )
    WHERE v.dept = t.dept
      AND v.dept = 'D069'
    GROUP BY v.nature, t.nature, t.dept
    ORDER BY approx_area_km2 DESC
""")

display(rail_land_use)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Aerodrome & Runway Inventory
# MAGIC
# MAGIC Multi-modal transport analysis: airports and airfields.

# COMMAND ----------

aerodromes = spark.sql(f"""
    SELECT
        a.toponyme AS aerodrome_name,
        a.nature AS aerodrome_type,
        a.dept,
        COUNT(p.cleabs) AS runway_count,
        ST_AsText(a.geometry) AS wkt
    FROM {prefix}aerodrome_dedup a
    LEFT JOIN {prefix}piste_d_aerodrome_dedup p
        ON ST_Intersects(a.geometry, p.geometry)
        AND a.dept = p.dept
    GROUP BY a.toponyme, a.nature, a.dept, ST_AsText(a.geometry)
    ORDER BY runway_count DESC
    LIMIT 50
""")

display(aerodromes)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Analysis | BD TOPO Layers Used |
# MAGIC |----------|-------------------|
# MAGIC | Rail network mapping | `troncon_de_voie_ferree`, `voie_ferree_nommee` |
# MAGIC | Station catchment | `equipement_de_transport` + `batiment`, `zone_d_habitation` |
# MAGIC | Road-rail crossings | `troncon_de_route` + `troncon_de_voie_ferree` |
# MAGIC | Flood risk | `troncon_de_voie_ferree` + `cours_d_eau`, `plan_d_eau` |
# MAGIC | Land use corridors | `zone_de_vegetation` + `troncon_de_voie_ferree` |
# MAGIC | Aerodromes | `aerodrome`, `piste_d_aerodrome` |
# MAGIC
# MAGIC **Next steps**: Integrate with passenger flow data, maintenance schedules,
# MAGIC and weather forecasts for predictive operations and capacity planning.
