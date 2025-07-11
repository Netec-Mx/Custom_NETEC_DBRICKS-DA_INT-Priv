# Práctica 2. Consumo y transformación de datos en Azure Databricks

## 🎯 Objetivos:
Al finalizar la práctica, serás capaz de:
- Consumir información desde una tabla existente, evaluar su calidad y aplicar procesos de limpieza y enriquecimiento utilizando dos enfoques distintos: transformaciones con PySpark y transformaciones con SQL.
- Guardar una versión limpia del dataset para usarla en análisis posteriores.

## 📝 Requisitos previos:

- Haber completado la Práctica 1  
- Tener acceso al clúster Databricks y a la tabla `ventas`  
- Conocimientos básicos de PySpark y SQL

## 🕒 Duración aproximada:
- 45 minutos.

---

**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab1.html)** | **[Lista General](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo3/lab3.html)**

---

## Instrucciones:

### Tarea 1: Creación o verificación del servidor en Databricks y carga de los datos

#### Tarea 1.1.

- **Paso 1.** Si eliminaste tu workspace de Databricks o el clúster, **repite la Tarea 1 de la práctica de configuración del entorno**.

    ***💡 NOTA:** Si ya tienes el clúster creado, avanza directamente al **Paso 2**.*

    [Haz clic aquí para ir a la práctica: Configurar entorno individual en Azure Databricks](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab0.html)

- **Paso 2.** Accede a tu workspace de Databricks haciendo clic en el botón **Launch Workspace**.

    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img12.png)

- **Paso 3.** Descarga el dataset de demostración desde este **[enlace](https://neteclabs.blob.core.windows.net/courses/databricks/intermedio/databricks-int-dataset.csv)**.

- **Paso 4.** Haz clic en el menú lateral izquierdo para cargar el archivo con los datos. Luego, en la parte derecha de la pantalla, selecciona **Agregar los datos**.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img23.png)

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img24.png)

- **Paso 5.** Haz clic en la opción **Create or modify table**.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img25.png)

- **Paso 6.** Arrastra o carga el archivo que descargaste previamente, verifica los datos y haz clic en **Create table**.

  ***💡 NOTA:** Puede tardar unos segundos en mostrar los datos.*

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img26.png)

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img27.png)

- **Paso 7.** Haz clic en el **Workspace** desde el menú lateral izquierdo.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img18.png)

- **Paso 8.** Crea un nuevo notebook con el nombre `Lab2_Processing` y selecciona el lenguaje `SQL`.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img19.png)

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img1.png)

- **Paso 9.** Adjunta tu clúster activo al notebook.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img21.png)

- **Paso 10.** Ejecuta el siguiente comando para cambiar el nombre de la tabla a **`ventas`** para mayor facilidad.

  ```sql
  ALTER TABLE databricks_int_dataset RENAME TO ventas;
  ```

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img29.png)

  ---

> **TAREA FINALIZADA**  

### Resultado esperado:
Has preparado tu ambiente para la ejecución de consultas y scripts.

---

### Tarea 2: Evaluar la calidad del dataset  

Antes de transformar los datos, es esencial entender su estado actual: su estructura, la presencia de valores nulos y registros duplicados.

#### Tarea 2.1.

- **Paso 1.** Revisa el esquema de la tabla `ventas`:  

  ```sql
  DESCRIBE TABLE ventas;
  ```

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img2.png)


- **Paso 2** Contar el total de registros:  
    
  ```python
  %python
  spark.table("ventas").count()
  ```

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img3.png)

- **Paso 3.** Contar los valores nulos por columna:
    
  ```python
  %python
  from pyspark.sql.functions import col, sum as _sum
  ventas = spark.table("ventas")
  ventas.select([_sum(col(c).isNull().cast("int")).alias(c) for c in ventas.columns]).show()
  ```

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img4.png)

- **Paso 4.** Contar los valores duplicados exactos:

  ```python
  %python
  ventas.groupBy(ventas.columns).count().filter("count > 1").count()
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img5.png)

> **TAREA FINALIZADA**

### Resultado esperado:
Tendrás claridad sobre si es necesario limpiar valores nulos, eliminar duplicados o corregir tipos de datos.

---

### Tarea 3: Limpieza y transformación con PySpark

En esta tarea, procesarás los datos usando PySpark para generar un nuevo DataFrame enriquecido.

#### Tarea 3.1.

- **Paso 1.** Elimina las filas que contengan valores nulos en columnas críticas.  

  ```python
  %python
  display(ventas.filter(col("PrecioUd").isNull() | col("Unidades").isNull() | col("Fecha").isNull()))
  ventas_limpias = ventas.dropna(subset=["PrecioUd", "Unidades", "Fecha"])
  display(ventas_limpias)
  ```

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img6.png)

- **Paso 2.** Elimina los duplicados exactos:

  ```python
  %python
  from pyspark.sql.functions import count
  ventas.groupBy("NumPed", "Fecha", "Articulo").count().filter("count > 1").show()
  ventas.exceptAll(ventas.dropDuplicates()).show()
  ```

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img7.png)

- **Paso 3.** Convierte la columna `Fecha` a tipo fecha: 

  ```python
  %python
  ventas.select("Fecha").distinct().show(5)
  ventas_limpias.select("Fecha").distinct().show(5)
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img8.png)

#### Tarea 3.2.

- **Paso 1.** Calcula la columna `TotalNeto`:

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


- **Paso 2.** Extrae `Mes` y `Anio`:
    
  ```python
  %python
  from pyspark.sql.functions import month, year
  ventas_limpias = ventas_limpias.withColumn("Mes", month("Fecha")).withColumn("Anio", year("Fecha"))
  ventas_limpias.select("Fecha", "Mes", "Anio").show(10)
  ```

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img10.png)

> **TAREA FINALIZADA**

### Resultado esperado:
Contarás con un DataFrame en PySpark con los datos limpios y enriquecidos, listo para el análisis.

---

### Tarea 4: Guardar versión limpia con PySpark

Guardarás los resultados procesados con PySpark en una nueva tabla Delta llamada `ventas_limpias`.

#### Tarea 4.1.

- **Paso 1.** Guarda la tabla:

  ```python
  %python
  ventas_limpias.write.format("delta").mode("overwrite").saveAsTable("ventas_limpias")
  ```

  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img11.png)


- **Paso 2.** Valida desde SQL:

  ```sql
  SELECT COUNT(*), AVG(TotalNeto), MIN(Fecha), MAX(Fecha) FROM ventas_limpias;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img12.png)

- **Paso 3.** Para validar desde la interfaz gráfica, haz clic en la sección **Catalog** del menú lateral izquierdo.

    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img13.png)

- **Paso 4.** Si es necesario, conecta tu **`cluster al catalog`**, expande el metastore de Hive y verás la tabla que guardaste.

    ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img14.png)

- **Paso 5.** Regresa a tu **notebook** y crea una nueva celda debajo de la última.

- **Paso 6.** Ejecuta el siguiente comando para escribir el contenido como un archivo CSV único en una carpeta con acceso público.

  ```python
  %python
  df = spark.table("ventas_limpias")
  # Ruta correcta para DBFS virtual (dbutils, navegación, descarga)
  df.coalesce(1).write.option("header", "true").mode("overwrite").csv("dbfs:/FileStore/ventas_limpias")
  ```
  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img21.png)

- **Paso 7.** Lista y extrae el nombre real del archivo:

  ```python
  %python
  files = dbutils.fs.ls("dbfs:/FileStore/ventas_limpias")

  for f in files:
      if f.name.endswith(".csv"):
          print(f.name)
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img22.png)

- **Paso 8.** Descarga el archivo fácilmente generando un enlace directo desde el notebook.

  ```python
  %python
  workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
  filename = [f.name for f in dbutils.fs.ls("dbfs:/FileStore/ventas_limpias") if f.name.endswith(".csv")][0]
  displayHTML(f"<a href='https://{workspace_url}/files/ventas_limpias/{filename}' target='_blank'>📥 Descargar CSV</a>")
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img23.png)

- **Paso 9.** Guarda el archivo con el nombre **`ventas_limpias`**.

> **TAREA FINALIZADA**

### Resultado esperado: 
Contar con una tabla optimizada y limpia, con transformaciones aplicadas usando PySpark, lista para análisis posteriores.

---

### Tarea 5: Transformación alternativa usando solo SQL  

Para analistas que prefieren trabajar exclusivamente con SQL, esta tarea ofrece un flujo equivalente a las tareas 2 y 3, usando únicamente consultas SQL.

#### Tarea 5.1.

- **Paso 1.** Crea una vista filtrada que excluya los valores nulos y verifica el resultado.

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

- **Paso 2.** Crea una vista sin duplicados:  

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

#### Tarea 5.2.  

- **Paso 1.** Crea una tabla enriquecida con nuevas columnas: 

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

- **Paso 2.** Valida los registros:

  ```sql
  SELECT COUNT(*), AVG(TotalNeto), MIN(Fecha), MAX(Fecha) FROM ventas_limpias_sql;
  ```

  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img20.png)

- **Paso 3.** Puedes repetir el **Paso 4** de la **Tarea 3** para visualizar la creación de la tabla desde la interfaz gráfica.

- **Paso 4.** Ejecuta el siguiente comando que escribe el CSV como archivo único en una carpeta con acceso público.

  ```python
  %python
  df = spark.table("ventas_limpias_sql")
  # Ruta correcta para DBFS virtual (dbutils, navegación, descarga)
  df.coalesce(1).write.option("header", "true").mode("overwrite").csv("dbfs:/FileStore/ventas_limpias_sql")
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img24.png)

- **Paso 5.** Lista y extrae el nombre real del archivo:

  ```python
  %python
  files = dbutils.fs.ls("dbfs:/FileStore/ventas_limpias_sql")

  for f in files:
      if f.name.endswith(".csv"):
          print(f.name)
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img25.png)

- **Paso 6.** Descarga el archivo cómodamente generando un enlace directo desde el notebook.

  ```python
  %python
  workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
  filename = [f.name for f in dbutils.fs.ls("dbfs:/FileStore/ventas_limpias_sql") if f.name.endswith(".csv")][0]
  displayHTML(f"<a href='https://{workspace_url}/files/ventas_limpias_sql/{filename}' target='_blank'>📥 Descargar CSV</a>")
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab2/img26.png)

- **Paso 7.** Guarda el archivo con el nombre **`ventas_limpias_sql`**.

> **TAREA FINALIZADA**

### Resultado esperado:
Se ha generado una tabla llamada `ventas_limpias_sql` equivalente a la creada con PySpark, construida completamente con SQL y lista para su visualización.

---

> **¡FELICIDADES! HAS COMPLETADO EL LABORATORIO 2.**

## Resultado final

Como analista, ahora cuentas con dos versiones limpias del dataset:

- `ventas_limpias`: transformada con PySpark. 
- `ventas_limpias_sql`: transformada con SQL.

Ambas están listas para visualización, análisis estadístico o integración con dashboards. Con este laboratorio, reforzaste tu comprensión de procesos ETL simples desde dos enfoques distintos, lo que te brinda mayor flexibilidad según tu experiencia técnica.

---
**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab1.html)** | **[Lista General](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo3/lab3.html)**
