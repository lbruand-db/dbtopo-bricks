# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Telecom Network Deployment Analysis
# MAGIC
# MAGIC **Use case**: Telecom operators (Orange, SFR, Bouygues Telecom) need to plan
# MAGIC fiber and antenna deployment, analyze coverage gaps, and assess terrain
# MAGIC constraints for network infrastructure rollout.
# MAGIC
# MAGIC This notebook demonstrates how BD TOPO enables:
# MAGIC 1. Building density analysis for fiber deployment prioritization
# MAGIC 2. Terrain and elevation analysis for antenna placement
# MAGIC 3. Existing infrastructure mapping (pylons, cable transport)
# MAGIC 4. Road network analysis for fiber duct routing
# MAGIC 5. Coverage gap identification in inhabited areas

# COMMAND ----------

dbutils.widgets.text("catalog", "lucasbruand_catalog")
dbutils.widgets.text("schema", "ign_bdtopo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
prefix = f"{catalog}.{schema}.ign_bdtopo_"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Building Density for Fiber Deployment
# MAGIC
# MAGIC Fiber rollout prioritizes dense areas first. BD TOPO building data
# MAGIC helps identify high-density zones by commune.

# COMMAND ----------

# Building count and density per commune
building_density = spark.sql(f"""
    SELECT
        c.nom_officiel AS commune_name,
        c.code_insee,
        c.dept,
        COUNT(b.cleabs) AS building_count,
        ROUND(ST_Area(c.geometry) * 111.32 * 111.32, 2) AS commune_area_km2,
        ROUND(COUNT(b.cleabs) / NULLIF(ST_Area(c.geometry) * 111.32 * 111.32, 0), 0)
            AS buildings_per_km2
    FROM {prefix}commune_dedup c
    JOIN {prefix}batiment_dedup b
        ON ST_Intersects(c.geometry, b.geometry)
        AND c.dept = b.dept
    WHERE c.dept = 'D069'  -- Rhone / Lyon metro area
    GROUP BY c.nom_officiel, c.code_insee, c.dept, ST_Area(c.geometry)
    ORDER BY buildings_per_km2 DESC
    LIMIT 50
""")

display(building_density)

# COMMAND ----------

# Building types — residential vs commercial for FTTH vs FTTO planning
building_types = spark.sql(f"""
    SELECT
        dept,
        nature AS building_type,
        COUNT(*) AS count
    FROM {prefix}batiment_dedup
    WHERE dept = 'D069'
    GROUP BY dept, nature
    ORDER BY count DESC
""")

display(building_types)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Terrain Analysis for Antenna Placement
# MAGIC
# MAGIC Antenna sites need elevation, line-of-sight, and terrain clearance.
# MAGIC Orographic features (ridges, peaks) help identify optimal locations.

# COMMAND ----------

# Orographic features — peaks, ridges, passes
high_points = spark.sql(f"""
    SELECT
        cleabs,
        nature,
        nature_detaillee,
        toponyme,
        dept,
        ST_AsText(geometry) AS wkt
    FROM {prefix}detail_orographique_dedup
    WHERE dept = 'D069'
      AND (nature LIKE '%Pic%'
           OR nature LIKE '%Sommet%'
           OR nature LIKE '%Col%'
           OR nature_detaillee LIKE '%pic%'
           OR nature_detaillee LIKE '%sommet%')
    ORDER BY nature
    LIMIT 100
""")

display(high_points)

# COMMAND ----------

# Existing pylons — potential co-location sites for antennas
existing_pylons = spark.sql(f"""
    SELECT
        dept,
        nature,
        nature_detaillee,
        COUNT(*) AS pylon_count
    FROM {prefix}pylone_dedup
    GROUP BY dept, nature, nature_detaillee
    ORDER BY pylon_count DESC
    LIMIT 30
""")

display(existing_pylons)

# COMMAND ----------

# Pylons in a specific department with geometry
pylons_map = spark.sql(f"""
    SELECT
        cleabs,
        nature,
        nature_detaillee,
        dept,
        ST_AsText(geometry) AS wkt
    FROM {prefix}pylone_dedup
    WHERE dept = 'D069'
    LIMIT 500
""")

display(pylons_map)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Road Network for Fiber Duct Routing
# MAGIC
# MAGIC Fiber cables follow roads. Analyzing the road network helps plan
# MAGIC duct routes and estimate deployment costs.

# COMMAND ----------

# Road network by type — identifies main routes for backbone fiber
road_network = spark.sql(f"""
    SELECT
        dept,
        nature AS road_type,
        nature_detaillee AS road_detail,
        COUNT(*) AS segment_count,
        ROUND(SUM(ST_Length(geometry)) * 111.32, 1) AS approx_length_km
    FROM {prefix}troncon_de_route_dedup
    WHERE dept = 'D069'
    GROUP BY dept, nature, nature_detaillee
    ORDER BY approx_length_km DESC
""")

display(road_network)

# COMMAND ----------

# Named/numbered roads — major routes for backbone deployment
major_roads = spark.sql(f"""
    SELECT
        cleabs,
        toponyme,
        nature,
        importance,
        ST_AsText(geometry) AS wkt
    FROM {prefix}route_numerotee_ou_nommee_dedup
    WHERE dept = 'D069'
      AND importance IN ('1', '2', '3')
    ORDER BY importance
    LIMIT 200
""")

display(major_roads)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Coverage Gap Analysis
# MAGIC
# MAGIC Identify inhabited areas that are far from existing infrastructure
# MAGIC (pylons, power lines) — potential coverage dead zones.

# COMMAND ----------

# Inhabited areas with no pylon within ~2km
coverage_gaps = spark.sql(f"""
    SELECT
        z.toponyme AS area_name,
        z.nature AS area_type,
        z.importance,
        z.dept,
        ST_AsText(z.geometry) AS wkt
    FROM {prefix}zone_d_habitation_dedup z
    WHERE z.dept = 'D069'
      AND NOT EXISTS (
          SELECT 1
          FROM {prefix}pylone_dedup p
          WHERE p.dept = z.dept
            AND ST_Intersects(ST_Buffer(z.geometry, 0.02), p.geometry)  -- ~2km
      )
    ORDER BY z.importance
    LIMIT 100
""")

display(coverage_gaps)

# COMMAND ----------

# Commune-level coverage gap score
commune_coverage = spark.sql(f"""
    WITH commune_pylons AS (
        SELECT
            c.code_insee,
            c.nom_officiel,
            COUNT(DISTINCT p.cleabs) AS pylon_count,
            COUNT(DISTINCT b.cleabs) AS building_count,
            ROUND(ST_Area(c.geometry) * 111.32 * 111.32, 2) AS area_km2
        FROM {prefix}commune_dedup c
        LEFT JOIN {prefix}pylone_dedup p
            ON ST_Intersects(c.geometry, p.geometry) AND c.dept = p.dept
        LEFT JOIN {prefix}batiment_dedup b
            ON ST_Intersects(c.geometry, b.geometry) AND c.dept = b.dept
        WHERE c.dept = 'D069'
        GROUP BY c.code_insee, c.nom_officiel, ST_Area(c.geometry)
    )
    SELECT
        nom_officiel,
        code_insee,
        building_count,
        pylon_count,
        area_km2,
        ROUND(building_count / NULLIF(pylon_count, 0), 0) AS buildings_per_pylon,
        CASE
            WHEN pylon_count = 0 AND building_count > 0 THEN 'NO COVERAGE'
            WHEN building_count / NULLIF(pylon_count, 0) > 500 THEN 'UNDERSERVED'
            ELSE 'ADEQUATE'
        END AS coverage_status
    FROM commune_pylons
    ORDER BY buildings_per_pylon DESC NULLS FIRST
    LIMIT 50
""")

display(commune_coverage)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Vegetation & Terrain Obstacles
# MAGIC
# MAGIC Dense forests block radio signals. Identify vegetation zones
# MAGIC that may require signal boosters or alternative routing.

# COMMAND ----------

vegetation_obstacles = spark.sql(f"""
    SELECT
        nature AS vegetation_type,
        dept,
        COUNT(*) AS zone_count,
        ROUND(SUM(ST_Area(geometry)) * 111.32 * 111.32, 2) AS total_area_km2
    FROM {prefix}zone_de_vegetation_dedup
    WHERE dept = 'D069'
    GROUP BY nature, dept
    ORDER BY total_area_km2 DESC
""")

display(vegetation_obstacles)

# COMMAND ----------

# Forests overlapping inhabited areas — signal attenuation risk
forest_overlap = spark.sql(f"""
    SELECT
        z.toponyme AS area_name,
        v.nature AS vegetation_type,
        z.dept,
        ROUND(ST_Area(ST_Intersection(v.geometry, z.geometry)) * 111.32 * 111.32, 4)
            AS overlap_area_km2
    FROM {prefix}zone_d_habitation_dedup z
    JOIN {prefix}zone_de_vegetation_dedup v
        ON ST_Intersects(z.geometry, v.geometry)
        AND z.dept = v.dept
    WHERE z.dept = 'D069'
      AND v.nature LIKE '%bois%' OR v.nature LIKE '%foret%' OR v.nature LIKE '%Forêt%'
    ORDER BY overlap_area_km2 DESC
    LIMIT 50
""")

display(forest_overlap)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Address Density for Last-Mile Planning
# MAGIC
# MAGIC The BAN (Base Adresse Nationale) layer provides precise address points
# MAGIC for last-mile fiber connection planning.

# COMMAND ----------

address_density = spark.sql(f"""
    SELECT
        c.nom_officiel AS commune_name,
        c.code_insee,
        COUNT(a.cleabs) AS address_count,
        ROUND(ST_Area(c.geometry) * 111.32 * 111.32, 2) AS area_km2,
        ROUND(COUNT(a.cleabs) / NULLIF(ST_Area(c.geometry) * 111.32 * 111.32, 0), 0)
            AS addresses_per_km2
    FROM {prefix}commune_dedup c
    JOIN {prefix}adresse_ban_dedup a
        ON ST_Intersects(c.geometry, a.geometry)
        AND c.dept = a.dept
    WHERE c.dept = 'D069'
    GROUP BY c.nom_officiel, c.code_insee, ST_Area(c.geometry)
    ORDER BY addresses_per_km2 DESC
    LIMIT 50
""")

display(address_density)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Analysis | BD TOPO Layers Used |
# MAGIC |----------|-------------------|
# MAGIC | Building density | `commune` + `batiment` |
# MAGIC | Antenna placement | `detail_orographique`, `pylone` |
# MAGIC | Fiber duct routing | `troncon_de_route`, `route_numerotee_ou_nommee` |
# MAGIC | Coverage gaps | `zone_d_habitation` + `pylone` |
# MAGIC | Signal obstacles | `zone_de_vegetation` + `zone_d_habitation` |
# MAGIC | Last-mile planning | `commune` + `adresse_ban` |
# MAGIC
# MAGIC **Next steps**: Combine with ARCEP coverage data, subscriber databases,
# MAGIC and terrain elevation models (RGE ALTI) for full network planning.
