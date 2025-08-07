# Práctica 3. Uso de Delta Lake y Datahouses en Azure Databricks

## 🎯 Objetivos:
Al finalizar la práctica, serás capaz de:
- Usar Delta Lake para gestionar grandes volúmenes de datos de manera segura, confiable y auditable.
- Realizar actualizaciones, eliminaciones y auditorías mediante Time Travel, y construir un resumen consolidado de ventas tipo datahouse, que servirá como base para la visualización y exploración analítica.

## 📝 Requisitos previos:
- Haber completado la práctica 2.
- Tener acceso a las tablas `ventas_limpias`.
- Contar con conocimientos intermedios de SQL y operaciones básicas con Delta Lake.

## 🕒 Duración aproximada:
- 45 minutos

---

**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo2/lab2.html)** | **[Lista general](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo4/lab4.html)**

---

## Instrucciones:

### Tarea 1: Creación/verificación del servidor en Databricks y carga de los datos

#### Tarea 1.1.

- **Paso 1.** En caso de haber eliminado el workspace de Databricks y el servidor, **repite la tarea 1 de la creación de tu ambiente**.

> 💡 ***Nota:** Si ya tienes el clúster creado, avanza al **Paso 2**.*

  [Clic Aquí para ir a Práctica: Configurar entorno individual en Azure Databricks](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab0.html)

- **Paso 2.** Accede a tu workspace de Databricks dando clic en el botón **Launch Workspace**

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img12.png)

- **Paso 3.** Haz clic en el menú lateral izquierdo para cargar el archivo con los datos. Luego, haz clic en la parte lateral derecha para **agregar los datos**.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img23.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img24.png)

- **Paso 4.** Da clic en la opción **Create or modify table**.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img25.png)

- **Paso 5.** Arrastra o carga el archivo llamado **`ventas_limpias`** previamente creado, verifica los datos y da clic en **Create table**.

> 💡 ***Nota:** Puede tardar unos segundos en mostrar los datos.*

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img26.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img27.png)

- **Paso 6.** Da clic en el **Workspace** del menú lateral izquierdo.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img18.png)

- **Paso 7.** Crea un nuevo _notebook_ con el nombre `Lab3_Delta_Datahouse` y selecciona el lenguaje `SQL`.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img19.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img31.png)

- **Paso 8.** Adjunta tu clúster activo al _notebook_.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img21.png)

> **TAREA FINALIZADA**  

### Resultado esperado:
Has preparado tu ambiente para la ejecución de las consultas y scripts.

---

### Tarea 2: Verificar que la tabla esté en formato Delta.  

Antes de utilizar funcionalidades específicas de Delta Lake, es importante confirmar que la tabla esté registrada correctamente como una Delta Table.

#### Tarea 2.1.

- **Paso 1.** Usa el comando `DESCRIBE DETAIL` para ver las propiedades internas de la tabla `ventas_limpias`.

  ```sql
  DESCRIBE DETAIL ventas_limpias;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img1.png)

> **TAREA FINALIZADA**

### Resultado esperado:
Ver en la salida el campo `"format": "delta"`, lo cual garantiza que la tabla soporta operaciones ACID y Time Travel.

---

### Tarea 3: Modificar registros existentes con UPDATE.

Actualizar valores existentes es una necesidad común al aplicar lógica de negocio. En esta sección aprenderás a modificar campos y generar información categórica derivada.

#### Tarea 3.1.

- **Paso 1.** Aumenta en 5 puntos el `Descuento` para las ventas realizadas en CDMX durante 2024.

  ```sql
  UPDATE ventas_limpias
  SET Descuento = Descuento + 5
  WHERE Provincia = 'CDMX' AND Anio = 2024;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img2.png)


- **Paso 2.** Aplica una regla de corrección de datos: si el precio unitario es menor o igual a 0, reemplázalo por el promedio general.

  ```sql
  UPDATE ventas_limpias
  SET PrecioUd = (SELECT AVG(PrecioUd) FROM ventas_limpias)
  WHERE PrecioUd <= 0;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img3.png)

- **Paso 3.** Clasifica los descuentos en categorías descriptivas. Primero, crea la columna vacía y después realiza el **UPDATE** correspondiente.

  ```sql
  ALTER TABLE ventas_limpias ADD COLUMNS (CategoriaDescuento STRING);
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img4.png)

  ```sql
  UPDATE ventas_limpias
  SET CategoriaDescuento = 
  CASE 
      WHEN Descuento < 5 THEN 'Bajo'
      WHEN Descuento BETWEEN 5 AND 10 THEN 'Medio'
      ELSE 'Alto'
  END;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img5.png)

- **Paso 4.** Para ver los valores de la nueva columna `CategoriaDescuento` junto con los datos relevantes.

  ```sql
  SELECT NumPed, Descuento, CategoriaDescuento
  FROM ventas_limpias
  LIMIT 20;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img6.png)

- **Paso 5.** Consulta el conteo de registros por categoría.

  ```sql
  SELECT CategoriaDescuento, COUNT(*) AS total
  FROM ventas_limpias
  GROUP BY CategoriaDescuento;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img7.png)

- **Paso 6.** Tip adicional: asegúrate de que no haya **NULLs** inesperados.

  ```sql
  SELECT *
  FROM ventas_limpias
  WHERE CategoriaDescuento IS NULL;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img8.png)

> **TAREA FINALIZADA**

### Resultado esperado:
La tabla reflejará datos corregidos, valores ajustados y clasificación por categorías, lo cual aporta mayor valor analítico.

---

### Tarea 4: Eliminar registros obsoletos o erróneos.

Eliminar datos permite mantener la integridad, especialmente cuando hay registros no válidos o poco útiles.

#### Tarea 4.1.

- **Paso 1.** Verifica cuántos registros serán eliminados sobre `TotalNeto` sea nulo o extremadamente bajo.

  ```sql
  SELECT COUNT(*) FROM ventas_limpias
  WHERE TotalNeto IS NULL OR TotalNeto < 20;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img9.png)

- **Paso 2.** Elimina las filas cuyo valor en `TotalNeto` sea nulo o extremadamente bajo.

  ```sql
  DELETE FROM ventas_limpias
  WHERE TotalNeto IS NULL OR TotalNeto < 20;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img10.png)
    

- **Paso 3.** Elimina las ventas antiguas (anteriores a 2023) registradas en Jalisco.

  ```sql
  SELECT COUNT(*) FROM ventas_limpias
  WHERE Provincia = 'Jalisco' AND Anio < 2023;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img11.png)
  
  ---
  
  ```sql
  DELETE FROM ventas_limpias
  WHERE Provincia = 'Jalisco' AND Anio < 2023;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img12.png)

> **TAREA FINALIZADA**

### Resultado esperado:
Se eliminan datos inconsistentes o fuera del rango de interés, manteniendo la trazabilidad gracias a Delta Lake.

---

### Tarea 5: Auditar cambios con _Time Travel_.

Delta Lake permite consultar versiones anteriores para análisis histórico o recuperación.

#### Tarea 5.1.

- **Paso 1.** Consulta el historial completo de los cambios realizados sobre la tabla.

  ```sql
  DESCRIBE HISTORY ventas_limpias;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img13.png)

- **Paso 2.** Accede a una versión específica de la tabla (por número).

  ```sql
  SELECT COUNT(*) FROM ventas_limpias VERSION AS OF 0;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img14.png)

- **Paso 3.** Compara los registros entre dos versiones distintas (0 y 1).

> 💡 ***Nota:** El resultado puede ser extenso; puedes hacer **scroll** para ver la información.*

  ```sql
  SELECT * FROM ventas_limpias VERSION AS OF 0
  EXCEPT
  SELECT * FROM ventas_limpias VERSION AS OF 1;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img15.png)


- **Paso 4.** Crea una copia de respaldo a partir de una versión anterior.

  ```sql
  CREATE OR REPLACE TABLE ventas_backup AS
  SELECT * FROM ventas_limpias VERSION AS OF 0;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img16.png)

> **TAREA FINALIZADA** 

### Resultado esperado:
El analista puede consultar y recuperar versiones anteriores completas, lo que permite trazabilidad, auditoría y respaldo histórico.

---

### Tarea 6: Simular actualizaciones con MERGE INTO.

El comando `MERGE` permite actualizar o insertar registros de forma condicional, lo cual es útil para flujos de carga incremental o actualizaciones programadas.

#### Tarea 6.1.

- **Paso 1.** Crea una vista temporal con registros modificados o nuevos.

  ```sql
  CREATE OR REPLACE TEMP VIEW nuevas_ventas AS
  SELECT *, TotalNeto + 10 AS TotalNetoMod FROM ventas_limpias LIMIT 5;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img17.png)

- **Paso 2.** Ejecuta el comando `MERGE` para actualizar los registros si ya existen, o insertarlos si no existen.

  ```sql
  MERGE INTO ventas_limpias AS target
  USING nuevas_ventas AS source
  ON target.NumPed = source.NumPed
  WHEN MATCHED THEN UPDATE SET TotalNeto = source.TotalNeto
  WHEN NOT MATCHED THEN INSERT *;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img18.png)

> **TAREA FINALIZADA**

### Resultado esperado:
Delta aplicará los cambios de forma inteligente y transaccional, lo que permite mantener la consistencia de los datos.

---

### Tarea 7: Construir resumen consolidado (datahouse).

Una tabla agregada permite observar métricas clave por región, mes y año, y preparar la base para _dashboards_.

#### Tarea 7.1.

- **Paso 1.** Crea una tabla de resumen por año, mes y provincia.

  ```sql
  CREATE OR REPLACE TABLE resumen_ventas AS
  SELECT 
  Anio,
  Mes,
  Provincia,
  COUNT(*) AS NumVentas,
  ROUND(SUM(TotalNeto), 2) AS TotalVentas,
  ROUND(AVG(Descuento), 2) AS PromDescuento,
  MAX(TotalNeto) AS VentaMayor,
  MIN(TotalNeto) AS VentaMenor
  FROM ventas_limpias
  GROUP BY Anio, Mes, Provincia;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img19.png)

- **Paso 2.** Visualiza los primeros registros de la tabla resumen.

  ```sql
  SELECT * FROM resumen_ventas
  ORDER BY Anio, Mes, Provincia
  LIMIT 10;
  ```
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img20.png)

- **Paso 3.** Consulta cuántas combinaciones únicas existen.

  ```sql
  SELECT COUNT(*) FROM resumen_ventas;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img21.png)

- **Paso 4.** Compara el total general con el detalle por combinación.

  ```sql
  SELECT ROUND(SUM(TotalNeto), 2) AS TotalDetalle FROM ventas_limpias;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img22.png)
  
  ---
  
  ```sql
  SELECT ROUND(SUM(TotalVentas), 2) AS TotalResumen FROM resumen_ventas;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img23.png)
  
  ---

> 💡 ***Nota:** Ambos valores deben coincidir si la tabla **ventas_limpias** no ha cambiado.*

- **Paso 5.** Consulta un resumen por provincia.

  ```sql
  SELECT Provincia, SUM(TotalVentas) AS VentasProvincia
  FROM resumen_ventas
  GROUP BY Provincia
  ORDER BY VentasProvincia DESC;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img24.png)

- **Paso 6.** Observa los valores extremos.

  ```sql
  SELECT MAX(VentaMayor) AS VentaMaxima, MIN(VentaMenor) AS VentaMinima
  FROM resumen_ventas;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img25.png)

> 💡 ***Nota:** Esto te permite confirmar que los valores extremos están dentro de los rangos lógicos esperados.*

- **Paso 7.** Crea una vista comparativa entre regiones.

  ```sql
  CREATE OR REPLACE VIEW comparativo_cdmx_vs_jalisco AS
  SELECT Provincia, Anio, Mes, TotalVentas
  FROM resumen_ventas
  WHERE Provincia IN ('CDMX', 'Jalisco')
  ORDER BY Anio, Mes;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img26.png)

- **Paso 8.** Consulta el contenido de la vista creada

  ```sql
  SELECT * FROM comparativo_cdmx_vs_jalisco
  LIMIT 10;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img27.png)

- **Paso 9.** Valida las ventas por región y año.

  ```sql
  SELECT Provincia, Anio, SUM(TotalVentas) AS TotalAnual
  FROM comparativo_cdmx_vs_jalisco
  GROUP BY Provincia, Anio
  ORDER BY Anio, Provincia;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img28.png)

- **Paso 10.** Crea una vista para detectar provincias con alto volumen:

  ```sql
  CREATE OR REPLACE VIEW provincias_top AS
  SELECT Provincia, SUM(TotalVentas) AS TotalAnual
  FROM resumen_ventas
  GROUP BY Provincia
  ORDER BY TotalAnual DESC;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img29.png)

- **Paso 11.** Esto te mostrará el ranking de provincias por ventas acumuladas:

  ```sql
  SELECT * FROM provincias_top;
  ```
  
  ---

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img30.png)

- **Paso 12.** Ejecuta el siguiente comando para escribir el archivo CSV como un único archivo dentro de una carpeta pública.

  ```python
  %python
  df = spark.table("resumen_ventas")
  # Ruta correcta para DBFS virtual (dbutils, navegación, descarga)
  df.coalesce(1).write.option("header", "true").mode("overwrite").csv("dbfs:/FileStore/resumen_ventas")
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img32.png)

- **Paso 13.** Lista y extrae el nombre real del archivo:

  ```python
  %python
  files = dbutils.fs.ls("dbfs:/FileStore/resumen_ventas")

  for f in files:
      if f.name.endswith(".csv"):
          print(f.name)
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img33.png)

- **Paso 14.** Descárgalo cómodamente generando un enlace desde el _notebook_.

  ```python
  %python
  workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
  
  filename = [f.name for f in dbutils.fs.ls("dbfs:/FileStore/resumen_ventas") if f.name.endswith(".csv")][0]
  displayHTML(f"<a href='https://{workspace_url}/files/resumen_ventas/{filename}' target='_blank'>📥 Descargar CSV</a>")
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab3/img34.png)

- **Paso 15.** Guarda el archivo como **`resumen_ventas`**.

> **TAREA FINALIZADA**

### Resultado esperado:
Disponer de una tabla `resumen_ventas` con vistas derivadas para el análisis de desempeño por región, mes y comparación entre provincias.

---

> **¡FELICIDADES, HAS COMPLETADO EL LABORATORIO 3!**

## Resultado final:

El analista habrá aplicado todo el ciclo de manejo de datos con Delta Lake: modificación, eliminación, auditoría de versiones, integración incremental (`MERGE`) y construcción de una tabla consolidada tipo datahouse con vistas analíticas derivadas. Todo con integridad, trazabilidad y eficiencia operativa, para soportar decisiones de negocio basadas en datos.

---

**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo2/lab2.html)** | **[Lista general](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo4/lab4.html)**
