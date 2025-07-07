# Práctica 2: Consumo y transformación de datos en Azure Databricks

## Objetivo 

Como analista de datos, aprenderás a consumir información desde una tabla existente, evaluar su calidad, aplicar limpieza y enriquecimiento mediante dos enfoques distintos: transformaciones en PySpark y transformaciones en SQL. Finalmente, aprenderás a guardar una versión limpia del dataset para análisis posteriores.

## Requisitos  

- Haber completado la Práctica 1  
- Tener acceso al clúster Databricks y a la tabla `ventas`  
- Conocimientos básicos de PySpark y SQL

## Duración aproximada  

- 45 minutos

---

**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab1.html)** | **[Lista General](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo3/lab3.html)**

---

## Instrucciones

### Tarea 1: Creación/Verificación del servidor en databricks y carga de los datos

#### Tarea 1.1

- **Paso 1.** En caso de haber eliminado el workspace de databricks y el servidor, **repite la tarea 1 de la creación de tu ambiente**

    **NOTA:** Si ya tienes el cluster creado avanza al **Paso 2**

    [Clic Aquí para ir a Práctica: Configurar entorno individual en Azure Databricks](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab0.md)

- **Paso 2.** Accede a tu workspace de Databricks dando clic en el boton **Launch Workspace**

    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img12.png)

- **Paso 3.** Descargar el dataset demostrativo **[Aquí](https://neteclabs.blob.core.windows.net/courses/databricks/intermedio/databricks-int-dataset.csv)**

- **Paso 4.** Ahora da clic en el menu lateral izquierdo para cargar el archivo con los datos. Da clic en la parte lateral derecha para **Agregar los datos**

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img23.png)
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img24.png)

- **Paso 5.** Da clie en la opción **Create or modify table**.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img25.png)

- **Paso 6.** Arrastra o carga el archivo previamente descargado, verifica los datos y da clic en **Create table**.

  **NOTA:** Puede tardar unos segundos en mostrar los datos.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img26.png)
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img27.png)

- **Paso 7.** Da clic en el **Workspace** del menu lateral izquierdo.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img18.png)

- **Paso 8.** Crea un nuevo notebook con el nombre `Lab2_Processing`. Selecciona el lenguaje `SQL`.

    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img19.png)
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img1.png)

- **Paso 9.** Adjunta tu clúster activo al Notebook.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img21.png)

- **Paso 10.** Ejecuta el siguiente comando para cambiar el nombre de la tabla a **`ventas`** para mayor facilidad.

  ```sql
  ALTER TABLE databricks_int_dataset RENAME TO ventas;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img29.png)
  ---

> **TAREA FINALIZADA**  

**Resultado esperado:** Haz preprado tu ambiente para la ejecucion de las consultas y scripts.

### Tarea 2: Evaluar la calidad del dataset  

Antes de transformar los datos, es esencial entender su estado actual: estructura, valores nulos y duplicados.

#### Tarea 2.1

- **Paso 1.** Revisar el esquema de la tabla `ventas`  

    ```sql
    DESCRIBE TABLE ventas;
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img2.png)


- **Paso 2** Contar registros totales  
    
    ```python
    %python
    spark.table("ventas").count()
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img3.png)

- **Paso 3.** Contar valores nulos por columna  
    
    ```python
    %python
    from pyspark.sql.functions import col, sum as _sum
    ventas = spark.table("ventas")
    ventas.select([_sum(col(c).isNull().cast("int")).alias(c) for c in ventas.columns]).show()
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img4.png)

- **Paso 4.** Contar duplicados exactos 

    ```python
    %python
    ventas.groupBy(ventas.columns).count().filter("count > 1").count()
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img5.png)

> **TAREA FINALIZADA**

**Resultado esperado:** El analista sabrá si es necesario limpiar valores nulos, duplicados o corregir tipos de datos.

---

### Tarea 3: Limpieza y transformación con PySpark

En esta tarea se procesarán los datos usando PySpark, generando un nuevo DataFrame enriquecido.

#### Tarea 3.1

- **Paso 1.** Eliminar filas con valores nulos en columnas críticas  

    ```python
    %python
    display(ventas.filter(col("PrecioUd").isNull() | col("Unidades").isNull() | col("Fecha").isNull()))
    ventas_limpias = ventas.dropna(subset=["PrecioUd", "Unidades", "Fecha"])
    display(ventas_limpias)
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img6.png)

- **Paso 2.** Eliminar duplicados exactos

    ```python
    %python
    from pyspark.sql.functions import count
    ventas.groupBy("NumPed", "Fecha", "Articulo").count().filter("count > 1").show()
    ventas.exceptAll(ventas.dropDuplicates()).show()
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img7.png)

- **Paso 3.** Convertir columna `Fecha` a tipo fecha  

    ```python
    %python
    ventas.select("Fecha").distinct().show(5)
    ventas_limpias.select("Fecha").distinct().show(5)
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img8.png)

#### Tarea 3.2 

- **Paso 1.** Calcular columna `TotalNeto`  

    ```python
    %python
    from pyspark.sql.functions import col, round
    ventas_limpias = ventas_limpias.withColumn(
        "TotalNeto", round(col("Unidades") * col("PrecioUd") * (1 - col("Descuento") / 100), 2)
    )
    ventas_limpias.select("NumPed", "Unidades", "PrecioUd", "Descuento", "TotalNeto").show(10)
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img9.png)


- **Paso 2.** Extraer `Mes` y `Anio`  
    
    ```python
    %python
    from pyspark.sql.functions import month, year
    ventas_limpias = ventas_limpias.withColumn("Mes", month("Fecha")).withColumn("Anio", year("Fecha"))
    ventas_limpias.select("Fecha", "Mes", "Anio").show(10)
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img10.png)

> **TAREA FINALIZADA**

**Resultado esperado:** DataFrame PySpark con los datos limpios y enriquecidos para análisis.

---

### Tarea 4: Guardar versión limpia con PySpark

Guardar los resultados de PySpark en una nueva tabla Delta llamada `ventas_limpias`.

#### Tarea 4.1

- **Paso 1.** Guardar la tabla  

    ```python
    %python
    ventas_limpias.write.format("delta").mode("overwrite").saveAsTable("ventas_limpias")
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img11.png)


- **Paso 2.** Validar desde SQL  

    ```sql
    SELECT COUNT(*), AVG(TotalNeto), MIN(Fecha), MAX(Fecha) FROM ventas_limpias;
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img12.png)

- **Paso 3.** Para validar desde la interfaz gráfica da clic en la seccion **Catalog** del menú lateral izquierdo.

    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img13.png)

- **Paso 4.** Si es necesario conecta tu **`cluster al catalog`**, **`expande el metastore`** de hive y encontraras la tabla guardada.

    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img14.png)

- **Paso 5.** Regresa a tu **Notebook** crea una celda mas debajo de la ultima.

- **Paso 6.** Ejecuta el siguiente comando que escribe el CSV como archivo único en una carpeta pública.

    ```python
    %python
    df = spark.table("ventas_limpias")
    # Ruta correcta para DBFS virtual (dbutils, navegación, descarga)
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv("dbfs:/FileStore/ventas_limpias")
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img21.png)

- **Paso 7.** Lista y extrae el nombre real del archivo

    ```python
    %python
    files = dbutils.fs.ls("dbfs:/FileStore/ventas_limpias")

    for f in files:
        if f.name.endswith(".csv"):
            print(f.name)
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img22.png)

- **Paso 8.** Descargalo comodamente generando un enlace desde notebook.

    ```python
    %python
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    filename = [f.name for f in dbutils.fs.ls("dbfs:/FileStore/ventas_limpias") if f.name.endswith(".csv")][0]
    displayHTML(f"<a href='https://{workspace_url}/files/ventas_limpias/{filename}' target='_blank'>📥 Descargar CSV</a>")
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img23.png)

- **Paso 9.** Guarda el archivo como **`ventas_limpias`**.

> **TAREA FINALIZADA**

**Resultado esperado:** Tener una tabla optimizada y limpia para análisis posteriores, con transformaciones aplicadas vía PySpark.

---

### Tarea 5: Transformación alternativa usando solo SQL  

Para analistas que prefieren SQL, se ofrece un flujo equivalente a las tareas 2 y 3, usando solo consultas SQL.

#### Tarea 5.1 

- **Paso 1.** Crear una vista filtrada sin valores nulos y verifica el rsultado

    ```sql
    CREATE OR REPLACE TEMP VIEW ventas_filtradas AS
    SELECT * FROM ventas
    WHERE PrecioUd IS NOT NULL AND Unidades IS NOT NULL AND Fecha IS NOT NULL;
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img15.png)
    ---
    ```sql
    SELECT * FROM ventas_filtradas LIMIT 10;
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img16.png)
    ---

- **Paso 2.** Crear vista sin duplicados  

    ```sql
    CREATE OR REPLACE TEMP VIEW ventas_deduplicadas AS
    SELECT DISTINCT * FROM ventas_filtradas;
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img17.png)
    ---
    ```sql
    SELECT * FROM ventas_deduplicadas LIMIT 10;
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img18.png)

#### Tarea 5.2  

- **Paso 1.** Crear tabla enriquecida con nuevas columnas  

    ```sql
    CREATE OR REPLACE TABLE ventas_limpias_sql AS
    SELECT 
    *,
    ROUND(Unidades * PrecioUd * (1 - Descuento / 100), 2) AS TotalNeto,
    year(Fecha) AS Anio,
    month(Fecha) AS Mes,
    CASE WHEN Provincia = 'CDMX' THEN true ELSE false END AS EsCDMX
    FROM ventas_deduplicadas;
    SELECT * FROM ventas_limpias_sql LIMIT 10;
    SELECT COUNT(*) FROM ventas_limpias_sql;
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img19.png)

- **Paso 2.** Validar registros  

    ```sql
    SELECT COUNT(*), AVG(TotalNeto), MIN(Fecha), MAX(Fecha) FROM ventas_limpias_sql;
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img20.png)

- **Paso 3.** Puedes repetir el **Paso 4** de la **Tarea 3** para visualizar la creación de la tabla mediante la interfaz.

- **Paso 4.** Ejecuta el siguiente comando que escribe el CSV como archivo único en una carpeta pública.

    ```python
    %python
    df = spark.table("ventas_limpias_sql")
    # Ruta correcta para DBFS virtual (dbutils, navegación, descarga)
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv("dbfs:/FileStore/ventas_limpias_sql")
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img24.png)

- **Paso 5.** Lista y extrae el nombre real del archivo

    ```python
    %python
    files = dbutils.fs.ls("dbfs:/FileStore/ventas_limpias_sql")

    for f in files:
        if f.name.endswith(".csv"):
            print(f.name)
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img25.png)

- **Paso 6.** Descargalo comodamente generando un enlace desde notebook.

    ```python
    %python
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    filename = [f.name for f in dbutils.fs.ls("dbfs:/FileStore/ventas_limpias_sql") if f.name.endswith(".csv")][0]
    displayHTML(f"<a href='https://{workspace_url}/files/ventas_limpias_sql/{filename}' target='_blank'>📥 Descargar CSV</a>")
    ```
    ---
    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img26.png)

- **Paso 7.** Guarda el archivo como **`ventas_limpias_sql`**.

> **TAREA FINALIZADA**

**Resultado esperado:** Se genera una tabla `ventas_limpias_sql` equivalente a la generada con PySpark, 100% construida con SQL, ideal para visualización.

---

> **¡FELICIDADES HAZ COMPLETADO EL LABORATORIO 2!**

## Resultado final

El analista tendrá dos versiones limpias del dataset:

- `ventas_limpias`: transformada con PySpark  
- `ventas_limpias_sql`: transformada con SQL

Ambas listas para visualización, análisis estadístico, o integración con dashboards. Se reforzará el entendimiento de procesos ETL simples desde dos perspectivas, aumentando la flexibilidad del analista según su experiencia técnica.

---
**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab1.html)** | **[Lista General](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo3/lab3.html)**