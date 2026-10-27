from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser(description="Prototipo Spark SQL para Aula Viva")
    parser.add_argument("--data-dir", default="data", help="Carpeta con los CSV de prueba")
    parser.add_argument("--output-dir", default="salida", help="Carpeta de resultados")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    spark = (SparkSession.builder.appName("AulaVivaSparkSQL").master("local[*]")
              .config("spark.driver.host", "127.0.0.1")
              .config("spark.driver.bindAddress", "127.0.0.1")
              .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        spaces = spark.read.option("header", True).option("inferSchema", True).csv(str(data_dir / "spaces_sample.csv"))
        reports = spark.read.option("header", True).option("inferSchema", True).csv(str(data_dir / "reports_sample.csv"))
        spaces.createOrReplaceTempView("catalogo_espacios")
        reports.createOrReplaceTempView("informes_brutos")

        # La consulta integra el catálogo exportado del sistema de información
        # con los informes enviados a Aula Viva, limpia filas inválidas y
        # conserva el informe más reciente para cada espacio.
        latest = spark.sql("""
            WITH validos AS (
                SELECT r.report_id, r.space_id,
                       to_timestamp(r.reported_at, 'yyyy-MM-dd HH:mm:ss') AS instante,
                       CAST(r.occupancy_ratio AS DOUBLE) AS ocupacion,
                       CAST(r.noise_db AS DOUBLE) AS ruido_db,
                       s.building, s.space_name, CAST(s.capacity AS INT) AS capacidad,
                       ROW_NUMBER() OVER (
                           PARTITION BY r.space_id
                           ORDER BY to_timestamp(r.reported_at, 'yyyy-MM-dd HH:mm:ss') DESC,
                                    r.report_id DESC
                       ) AS orden
                FROM informes_brutos r
                INNER JOIN catalogo_espacios s ON r.space_id = s.space_id
                WHERE s.active = true
                  AND to_timestamp(r.reported_at, 'yyyy-MM-dd HH:mm:ss') IS NOT NULL
                  AND CAST(r.occupancy_ratio AS DOUBLE) BETWEEN 0.0 AND 1.0
                  AND CAST(r.noise_db AS DOUBLE) BETWEEN 0.0 AND 120.0
            )
            SELECT report_id, space_id, instante, ocupacion, ruido_db,
                   building, space_name, capacidad
            FROM validos WHERE orden = 1
        """)
        latest.createOrReplaceTempView("estado_ultimo")
        summary = spark.sql("""
            SELECT building,
                   AVG(ocupacion) AS ocupacion_media,
                   COUNT(space_id) AS espacios_informados
            FROM estado_ultimo
            GROUP BY building
            ORDER BY building
        """)

        print("Último informe válido por espacio:")
        latest.orderBy("space_id").show(truncate=False)
        print("Resumen por edificio:")
        summary.show(truncate=False)

        # La muestra es pequeña: Spark calcula los resultados y Python los
        # escribe localmente para no requerir winutils.exe en Windows.
        output_dir.mkdir(parents=True, exist_ok=True)

        def write_csv(dataframe, destination: Path) -> None:
            rows = dataframe.collect()
            if not rows:
                return
            fieldnames = list(rows[0].asDict().keys())
            with destination.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    values = row.asDict()
                    for key, value in values.items():
                        if hasattr(value, "isoformat"):
                            values[key] = value.isoformat(sep=" ")
                    writer.writerow(values)

        write_csv(latest.orderBy("space_id"), output_dir / "estado_espacios.csv")
        write_csv(summary, output_dir / "resumen_edificios.csv")
        print(f"Resultados CSV escritos en: {output_dir.resolve()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


