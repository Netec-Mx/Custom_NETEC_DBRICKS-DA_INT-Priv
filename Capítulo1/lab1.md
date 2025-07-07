# Práctica 1: Consultas con SQL en Azure Databricks

## Objetivo  

Como analista de datos, aprenderás a preparar el conjunto de datos de ventas desde un archivo CSV cargado localmente a Databricks, y la ejecución de múltiples consultas SQL para extraer información clave del negocio. Este laboratorio constituye la base para trabajos posteriores de transformación y visualización de datos.

## Requisitos

- Acceso a Azure Cloud Shell desde el navegador ([Shell de Azure](https://shell.azure.com))  
- Permisos de **colaborador** sobre una suscripción de Azure compartida  
- Conocimientos básicos de SQL  
- Archivo `ventas_dataset.csv` previamente cargado en un contenedor de Azure Storage accesible públicamente

## Duración aproximada  

- 60 minutos

---

**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab0.html)** | **[Lista General](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo2/lab2.html)**

---

## Instrucciones

### Tarea 1: Cargar datos de ventas a Databricks

En esta tarea, utilizarás un archivo CSV para crear una tabla Delta en Databricks y dejarla disponible para consultas SQL.

#### Tarea 1.1

- **Paso 1.** Accede a tu workspace de Databricks dando clic en el boton **Launch Workspace**

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img12.png)

- **Paso 2.** Da clic en el **Workspace** del menu lateral izquierdo.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img18.png)

- **Paso 3.** Crea un nuevo notebook con el nombre `Lab1_SQL_Consultas`. Selecciona el lenguaje `SQL`.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img19.png)
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img20.png)

- **Paso 4.** Adjunta tu clúster activo al Notebook.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img21.png)

- **Paso 5.** Descargar el dataset demostrativo **[Aquí](https://neteclabs.blob.core.windows.net/courses/databricks/intermedio/databricks-int-dataset.csv)**

- **Paso 6.** Ahora da clic en el menu lateral izquierdo para cargar el archivo con los datos. Da clic en la parte lateral derecha para **Agregar los datos**

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img23.png)
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img24.png)

- **Paso 7.** Da clie en la opción **Create or modify table**.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img25.png)

- **Paso 8.** Arrastra o carga el archivo previamente descargado, verifica los datos y da clic en **Create table**.

  **NOTA:** Puede tardar unos segundos en mostrar los datos.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img26.png)
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img27.png)

- **Paso 9.** Ahora regresa a tu **Notebook** para leer la tabla con consultas de SQL. `Workspace - Home - Lab1_SQL_Consultas` 

- **Paso 10.** Ejecuta el siguiente código para validar la carga del archivo CSV:

  ```sql
  SHOW TABLES;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img28.png)

- **Paso 11.** Ejecuta el siguiente comando para cambiar el nombre de la tabla a **`ventas`** para mayor facilidad.

  **NOTA:** Agrega otra celda nueva debajo de la consulta de las tablas.

  ```sql
  ALTER TABLE databricks_int_dataset RENAME TO ventas;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img29.png)
  ---
  **NOTA:** Si es necesario repite el paso anterior para validar la tabla y el nombre.

> **TAREA FINALIZADA**

**Resultado esperado:** Tener una tabla llamada `ventas` almacenada en formato Delta con 10 Mil registros reales aproximadamente, lista para consultas en SQL.

---

### Tarea 2: Ejecutar consultas SQL sobre la tabla de ventas  

En esta tarea realizarás distintas consultas SQL para explorar, segmentar y obtener insights del conjunto de datos cargado.

#### Tarea 2.1: Exploración general

- **Paso 1.** Ejecuta estas consultas para revisar la estructura y contenido de la tabla:

  **NOTA:** Agrega otra celda mas debajo de la que ya existe.

  ```sql
  SELECT * FROM ventas LIMIT 10;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img30.png)
  ```sql
  DESCRIBE TABLE ventas;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img31.png)
  ---
  ```sql
  SELECT COUNT(*) FROM ventas;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img32.png)
  ---
  ```sql
  SELECT DISTINCT FormaPago FROM ventas;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img33.png)
  ---
  ```sql
  SELECT MIN(Fecha), MAX(Fecha) FROM ventas;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img34.png)
  ---

#### Tarea 2.2: Cálculos fila a fila

- **Paso 1.** Ejecuta la siguiente consulta en la celda.

  ```sql
  SELECT NumPed, Unidades, PrecioUd, (Unidades * PrecioUd) AS Total FROM ventas LIMIT 10;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img35.png)
  ---

- **Paso 2.** Ejecuta la siguiente consulta en la celda.

  ```sql
  SELECT NumPed, PrecioUd, Descuento,
        ROUND(PrecioUd * (1 - Descuento / 100), 2) AS PrecioConDescuento
  FROM ventas LIMIT 10;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img36.png)
  ---

#### Tarea 2.3: Agrupaciones y conteo

- **Paso 1.** Ejecuta la siguiente consulta en la celda.
  
  ```sql
  SELECT FormaPago, COUNT(*) AS TotalVentas, AVG(Unidades) AS Promedio FROM ventas GROUP BY FormaPago;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img37.png)
  ---

- **Paso 2.** Ejecuta la siguiente consulta en la celda.

  ```sql
  SELECT Articulo, COUNT(*) AS NumVentas
  FROM ventas
  GROUP BY Articulo
  ORDER BY NumVentas DESC
  LIMIT 10;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img38.png)
  ---

- **Paso 3.** Ejecuta la siguiente consulta en la celda.

  ```sql
  SELECT Provincia, COUNT(*) AS Total
  FROM ventas
  GROUP BY Provincia
  ORDER BY Total DESC;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img39.png)
  ---

#### Tarea 2.4: Análisis temporal

- **Paso 1.** Ejecuta la siguiente consulta en la celda.

  ```sql
  SELECT Fecha, COUNT(*) AS TotalVentas FROM ventas GROUP BY Fecha ORDER BY Fecha;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img40.png)

- **Paso 2.** Ejecuta la siguiente consulta en la celda.

  ```sql
  SELECT Fecha, FormaPago, COUNT(*) AS Total FROM ventas GROUP BY Fecha, FormaPago ORDER BY Fecha;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img41.png)

#### Tarea 2.5: Filtros y subconsultas

- **Paso 1.** Ejecuta la siguiente consulta en la celda.
  
  ```sql
  SELECT * FROM ventas WHERE Provincia = 'CDMX' AND Unidades > 8;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img42.png)


- **Paso 2.** Ejecuta la siguiente consulta en la celda.

  ```sql
  SELECT * FROM ventas WHERE PrecioUd > 100 AND Descuento > 5;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img43.png)


- **Paso 3.** Ejecuta la siguiente consulta en la celda.
  
  ```sql
  SELECT * FROM ventas WHERE PrecioUd > (SELECT AVG(PrecioUd) FROM ventas);
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img44.png)


#### Tarea 2.6: Análisis de clientes

- **Paso 1.** Ejecuta la siguiente consulta en la celda.
  
  ```sql
  SELECT CodCliente, COUNT(*) AS NumPedidos
  FROM ventas
  GROUP BY CodCliente
  ORDER BY NumPedidos DESC
  LIMIT 10;
  ```
  ---
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img45.png)

> **TAREA FINALIZADA**

**Resultado esperado:** Obtener una visión general del comportamiento de las ventas, formas de pago frecuentes, artículos y clientes más activos, así como detectar valores extremos o comportamientos inusuales.

---

> **¡FELICIDADES HAZ COMPLETADO EL LABORATORIO 1!**

## Resultado final

Cada analista habrá cargado un conjunto de datos real a Databricks, y ejecutado más de 15 consultas SQL organizadas por propósito analítico. Esto proporcionará una base sólida para los siguientes pasos del ciclo de análisis: limpieza, transformación, modelado y visualización.

---
**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab0.html)** | **[Lista General](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo2/lab2.html)**