# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Energy Grid Infrastructure Analysis
# MAGIC
# MAGIC **Use case**: Utility companies (Engie, EDF, Veolia) need to plan and manage energy
# MAGIC infrastructure across France — power lines, substations, pipelines, and their
# MAGIC proximity to buildings and hydrographic features for risk assessment.
# MAGIC
# MAGIC This notebook demonstrates how BD TOPO enables:
# MAGIC 1. Mapping the electrical grid (power lines + substations)
# MAGIC 2. Identifying buildings near high-voltage infrastructure
# MAGIC 3. Pipeline corridor analysis along watercourses
# MAGIC 4. Vegetation encroachment risk zones

# COMMAND ----------

dbutils.widgets.text("catalog", "lucasbruand_catalog")
dbutils.widgets.text("schema", "ign_bdtopo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
prefix = f"{catalog}.{schema}.ign_bdtopo_"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Electrical Grid Overview
# MAGIC
# MAGIC Power lines and substations form the backbone of the energy network.
# MAGIC BD TOPO provides precise geometry for both.

# COMMAND ----------

# Power lines with voltage classification
power_lines = spark.sql(f"""
    SELECT
        cleabs,
        nature,
        nature_detaillee,
        ST_AsText(geometry) AS wkt,
        ST_Length(geometry) AS length_deg,
        dept
    FROM {prefix}ligne_electrique_dedup
    ORDER BY dept
""")

display(power_lines.limit(100))

# COMMAND ----------

# Summary: power line length by type and department
power_summary = spark.sql(f"""
    SELECT
        dept,
        nature_detaillee,
        COUNT(*) AS segment_count,
        ROUND(SUM(ST_Length(geometry)) * 111.32, 1) AS approx_length_km
    FROM {prefix}ligne_electrique_dedup
    GROUP BY dept, nature_detaillee
    ORDER BY approx_length_km DESC
""")

display(power_summary)

# COMMAND ----------

# Substations per department
substations = spark.sql(f"""
    SELECT
        dept,
        nature_detaillee,
        COUNT(*) AS substation_count
    FROM {prefix}poste_de_transformation_dedup
    GROUP BY dept, nature_detaillee
    ORDER BY substation_count DESC
""")

display(substations)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Buildings Near High-Voltage Infrastructure
# MAGIC
# MAGIC Regulations require minimum clearance between buildings and power lines.
# MAGIC Spatial joins identify buildings within a buffer zone.

# COMMAND ----------

# Buildings within ~200m of power lines (using ST_Buffer + ST_Intersects)
# Focus on a single department for performance
buildings_near_grid = spark.sql(f"""
    SELECT
        b.cleabs AS building_id,
        b.nature AS building_type,
        l.nature_detaillee AS line_type,
        b.dept,
        ST_AsText(b.geometry) AS building_wkt
    FROM {prefix}batiment_dedup b
    JOIN {prefix}ligne_electrique_dedup l
        ON ST_Intersects(
            ST_Buffer(l.geometry, 0.002),  -- ~200m buffer in degrees
            b.geometry
        )
    WHERE b.dept = l.dept
      AND b.dept = 'D069'  -- Lyon area
    LIMIT 1000
""")

display(buildings_near_grid)

# COMMAND ----------

# Count buildings near power lines per department
building_risk_summary = spark.sql(f"""
    SELECT
        b.dept,
        COUNT(DISTINCT b.cleabs) AS buildings_near_lines
    FROM {prefix}batiment_dedup b
    JOIN {prefix}ligne_electrique_dedup l
        ON ST_Intersects(
            ST_Buffer(l.geometry, 0.002),
            b.geometry
        )
    WHERE b.dept = l.dept
    GROUP BY b.dept
    ORDER BY buildings_near_lines DESC
    LIMIT 20
""")

display(building_risk_summary)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Pipeline Corridors Along Watercourses
# MAGIC
# MAGIC Gas and fluid pipelines often follow river valleys. Identifying
# MAGIC pipelines near watercourses helps assess flood risk exposure.

# COMMAND ----------

# Pipelines near watercourses
pipeline_hydro = spark.sql(f"""
    SELECT
        c.cleabs AS pipeline_id,
        c.nature AS pipeline_type,
        c.nature_detaillee AS pipeline_detail,
        w.toponyme AS watercourse_name,
        c.dept,
        ST_AsText(c.geometry) AS pipeline_wkt
    FROM {prefix}canalisation_dedup c
    JOIN {prefix}cours_d_eau_dedup w
        ON ST_Intersects(
            ST_Buffer(w.geometry, 0.005),  -- ~500m buffer
            c.geometry
        )
    WHERE c.dept = w.dept
      AND c.dept = 'D069'
    LIMIT 500
""")

display(pipeline_hydro)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Vegetation Encroachment Risk
# MAGIC
# MAGIC Trees near power lines cause outages. Identify vegetation zones
# MAGIC that intersect with the electrical grid.

# COMMAND ----------

vegetation_risk = spark.sql(f"""
    SELECT
        v.nature AS vegetation_type,
        l.nature_detaillee AS line_type,
        v.dept,
        COUNT(*) AS intersection_count,
        ROUND(SUM(ST_Area(ST_Intersection(v.geometry, ST_Buffer(l.geometry, 0.001)))) * 111.32 * 111.32, 2)
            AS overlap_area_approx_km2
    FROM {prefix}zone_de_vegetation_dedup v
    JOIN {prefix}ligne_electrique_dedup l
        ON ST_Intersects(v.geometry, ST_Buffer(l.geometry, 0.001))  -- ~100m
    WHERE v.dept = l.dept
      AND v.dept = 'D069'
    GROUP BY v.nature, l.nature_detaillee, v.dept
    ORDER BY intersection_count DESC
""")

display(vegetation_risk)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Substation Accessibility Analysis
# MAGIC
# MAGIC How well are substations connected to the road network?

# COMMAND ----------

substation_access = spark.sql(f"""
    SELECT
        p.cleabs AS substation_id,
        p.nature_detaillee AS substation_type,
        r.nature AS road_type,
        r.nature_detaillee AS road_detail,
        p.dept,
        ST_AsText(p.geometry) AS substation_wkt
    FROM {prefix}poste_de_transformation_dedup p
    JOIN {prefix}troncon_de_route_dedup r
        ON ST_Intersects(
            ST_Buffer(p.geometry, 0.001),  -- ~100m
            r.geometry
        )
    WHERE p.dept = r.dept
      AND p.dept = 'D069'
    LIMIT 500
""")

display(substation_access)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook demonstrated how BD TOPO enables energy infrastructure analysis:
# MAGIC
# MAGIC | Analysis | BD TOPO Layers Used |
# MAGIC |----------|-------------------|
# MAGIC | Grid mapping | `ligne_electrique`, `poste_de_transformation` |
# MAGIC | Building proximity | `batiment` + `ligne_electrique` |
# MAGIC | Pipeline flood risk | `canalisation` + `cours_d_eau` |
# MAGIC | Vegetation encroachment | `zone_de_vegetation` + `ligne_electrique` |
# MAGIC | Substation access | `poste_de_transformation` + `troncon_de_route` |
# MAGIC
# MAGIC **Next steps**: Combine with consumption data, weather models, and asset
# MAGIC management systems for predictive maintenance and network planning.
