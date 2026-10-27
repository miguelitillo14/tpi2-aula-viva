# TPI 2 - Aula Viva con Spark SQL

Prototipo individual para aplicar los apartados 5.4 (procesamiento con Spark SQL) y 5.5 (integración con sistemas de información empresarial) de Ampliación de Bases de Datos.

El caso de uso procede de la propuesta Aula Viva: ayudar a estudiantes a consultar espacios de estudio con información reciente de ocupación y ruido. El prototipo combina informes de Aula Viva con el catálogo de espacios que, en un despliegue real, mantendría el sistema de información del campus.

> **Alcance de la demostración:** los CSV incluidos son sintéticos y no contienen datos personales ni proceden del sistema del campus. La integración con el sistema empresarial se simula mediante una exportación CSV porque no se dispone de acceso ni credenciales. La solución ejecuta Spark localmente; no necesita Docker ni un clúster Hadoop.

## Requisitos

- Windows 10/11 y PowerShell.
- Python 3.9 o superior. La ejecución verificada se hizo con Python 3.12.10.
- Java instalado y accesible desde PowerShell (`java -version`). PySpark 3.5.1 funciona con Java 8, 11 o 17; la ejecución verificada usó Java 8u431.
- Conexión a Internet solo para instalar PySpark la primera vez.

No hace falta activar el entorno virtual. Así se evitan problemas de política de ejecución de scripts de PowerShell.

## Descargar el proyecto

```powershell
git clone https://github.com/miguelitillo14/tpi2-aula-viva.git
cd tpi2-aula-viva
```

Si ya tienes el proyecto, abre PowerShell en la carpeta `TPI2_Aula_Viva` y continúa con el siguiente paso.

## Instalar y ejecutar

Ejecuta estos comandos desde la carpeta raíz del proyecto:

```powershell
python --version
java -version
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts\run_spark_sql.py --data-dir data --output-dir salida
```

La primera instalación descarga PySpark (aproximadamente 317 MB) y puede tardar varios minutos. Si la carpeta `.venv` ya existe, omite `python -m venv .venv`. No ejecutes los comandos desde `scripts` ni desde `data`.

## Qué hace el programa

1. Lee `data/spaces_sample.csv`, que representa el catálogo de espacios del sistema institucional, y `data/reports_sample.csv`, que contiene informes recientes de ocupación y ruido.
2. Crea una sesión Spark en modo local y registra ambas fuentes como vistas SQL.
3. Une informes y catálogo por `space_id`; descarta claves inexistentes, fechas no válidas, espacios inactivos y valores fuera de rango.
4. Usa `ROW_NUMBER()` para elegir el informe más reciente por espacio.
5. Calcula con Spark SQL la ocupación media del último estado por edificio.
6. Guarda las tablas resultantes en `salida/estado_espacios.csv` y `salida/resumen_edificios.csv`.

Para el prototipo, Python escribe esos dos CSV locales porque solo contienen unas pocas filas. En una instalación de producción, la salida debería escribirse desde Spark a almacenamiento distribuido o a una base de datos usando un conector autorizado.

## Resultado esperado

La consola debe mostrar cuatro espacios con el informe más reciente:

| Espacio | Informe | Ocupación | Edificio |
|---|---|---:|---|
| A101 | R002 | 0,80 | A |
| A102 | R003 | 0,20 | A |
| B201 | R006 | 0,50 | B |
| C101 | R005 | 0,90 | C |

Y este resumen:

| Edificio | Ocupación media | Espacios informados |
|---|---:|---:|
| A | 0,50 | 2 |
| B | 0,50 | 1 |
| C | 0,90 | 1 |

El informe `R007` se excluye porque referencia el espacio `X999`, que no existe en el catálogo de ejemplo. Los CSV de salida incluidos en `salida/` son la última ejecución comprobada y se reemplazan al volver a ejecutar el programa.

## Avisos de Windows

Spark puede mostrar avisos sobre `winutils.exe` o sobre la biblioteca nativa de Hadoop. En esta demostración local son avisos esperables: la consulta y la escritura de los CSV deben terminar igualmente. El script fija la dirección del controlador en `127.0.0.1`, por lo que no depende del nombre del ordenador.

Si aparece `ModuleNotFoundError: No module named 'pyspark'`, instala las dependencias con `.\.venv\Scripts\python.exe -m pip install -r requirements.txt` y ejecuta siempre el programa con `.\.venv\Scripts\python.exe`. Si aparece `Invalid Spark URL` con el nombre del equipo, asegúrate de ejecutar el script de la versión actual del repositorio; contiene la configuración local corregida.

## Archivos del proyecto

- `main.tex`: memoria del TPI 2 en la plantilla del profesor.
- `scripts/run_spark_sql.py`: integración, limpieza y agregación en Spark SQL.
- `data/spaces_sample.csv`: catálogo sintético de espacios.
- `data/reports_sample.csv`: informes sintéticos de ejemplo.
- `salida/estado_espacios.csv`: última salida comprobada, un estado por espacio.
- `salida/resumen_edificios.csv`: última salida comprobada, resumen por edificio.
- `requirements.txt`: versión fijada de PySpark.
- `logo_uloyola.png`: logotipo requerido para compilar la memoria LaTeX.

## Integración empresarial considerada

En el campus, el catálogo debería leerse desde una exportación controlada, una réplica de lectura o una fuente JDBC autorizada. Spark uniría ese catálogo con los informes de Aula Viva y publicaría una tabla analítica de solo lectura para la aplicación. Este repositorio no contiene conexión, contraseña ni credenciales de un sistema real. No se modifica el sistema administrativo de origen.
