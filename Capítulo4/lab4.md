# Práctica 4. Creación de gráficos y tableros en Azure Databricks

## 🎯 Objetivos:
Al finalizar la práctica, serás capaz de:
- Generar visualizaciones efectivas a partir de consultas SQL en Azure Databricks.
- Construir dashboards interactivos con filtros.
- Publicar tus resultados para su exploración visual.
- Aplicar buenas prácticas en las consultas y verificar la calidad de los datos usados para los gráficos.

## 📝 Requisitos previos:
- Haber completado la práctica 3.
- Tener acceso a la tabla `resumen_ventas`.  
- Contar con conocimientos básicos de SQL y experiencia previa con notebooks de Databricks.

## 🕒 Duración aproximada:
- 60 minutos.

---

**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo3/lab3.html)** | **[Lista general](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab0.html)**

---

## Instrucciones

### Tarea 1: Creación/Verificación del servidor en databricks y carga de los datos.

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
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img1.png)

- **Paso 8.** Adjunta tu clúster activo al _notebook_.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img21.png)

> **TAREA FINALIZADA**  

### Resultado esperado:
Has preparado tu ambiente para la ejecución de las consultas y scripts.

---

### Tarea 2: Verificar los datos fuente.

Verificar que los datos estén disponibles y correctos.

#### Tarea 2.1.  

- **Paso 1.** Verifica que la tabla `resumen_ventas` esté disponible y contenga datos.

> 💡 ***Nota:** Si es necesario, carga los datos de la tabla `resumen_ventas` que exportaste en el laboratorio anterior.*

  ```sql
  SELECT * FROM resumen_ventas LIMIT 10;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img2.png)

- **Paso 2.** Valida que no haya valores nulos en los campos críticos:

  ```sql
  SELECT COUNT(*) FROM resumen_ventas
  WHERE TotalVentas IS NULL OR NumVentas IS NULL;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img3.png)

> **TAREA FINALIZADA**

### Resultado esperado:
Confirmar la disponibilidad y la calidad de los datos para usarlos en visualizaciones.

---

### Tarea 3: Generar visualizaciones desde SQL.

Crear distintos tipos de gráficos usando consultas optimizadas en el editor SQL de Databricks.

#### Tarea 3.1.

- **Paso 1.** Gráfico de barras: Total de ventas por provincia.

  > **Visualización:** tipo **Bar chart**, eje X: Provincia; eje Y: VentasTotales.

  ```sql
  SELECT Provincia, SUM(TotalVentas) AS VentasTotales
  FROM resumen_ventas
  GROUP BY Provincia
  ORDER BY VentasTotales DESC;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img4.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img5.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img6.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img7.png)
      
- **Paso 2.** Gráfico de líneas: Evolución mensual de ventas en CDMX.

  > **Visualización:** tipo **Line chart**, eje X: Periodo; eje Y: VentasMensuales.

  ```sql
  SELECT CONCAT(Anio, '-', LPAD(Mes, 2, '0')) AS Periodo, SUM(TotalVentas) AS VentasMensuales
  FROM resumen_ventas
  WHERE Provincia = 'CDMX'
  GROUP BY Anio, Mes
  ORDER BY Periodo;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img8.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img9.png)    

- **Paso 3.** Gráfico de dispersión: Total vs Promedio de descuento.

  > Visualización: **Scatter plot**, eje X: PromDescuento; eje Y: TotalVentas.

  ```sql
  SELECT TotalVentas, PromDescuento
  FROM resumen_ventas;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img10.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img11.png)

> **TAREA FINALIZADA**

### Resultado esperado:
Tres visualizaciones generadas y configuradas desde el editor de notebook usando buenas prácticas.

### Tarea 4: Crear dashboard con visualizaciones agregadas .

Combinar las visualizaciones en un panel consolidado.

#### Tarea 4.1.

- **Paso 1.** Dirígete al gráfico de barras, haz clic en `Add to notebook dashboard` y, después, en `Add to new notebook dashboard`.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img12.png)

- **Paso 2.** Cambiara la vista a `Dashboard` y escribe el nombre en el cuadro de texto lateral derecho: **`Dashboard_Ventas_Visual`**.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img13.png)

- **Paso 3.** Ahora, da clic en la opcion `Notebook view`.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img14.png)

- **Paso 4.** Agrega la visualización de líneas. Repite el paso anterior para agregarla, pero ahora debe aparecerte el dashboard previamente creado: `Dashboard_Ventas_visual`.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img15.png)

- **Paso 5.** Selecciona el nombre del dashboard y se activará un check.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img16.png)

- **Paso 6.** Agrega la última gráfica: **Scatter**.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img17.png)
    
- **Paso 7.** Visualiza el dashboard completo. Haz clic en el ícono que muestra la imagen para ver el panel.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img18.png)

- **Paso 8.** Ajusta los gráficos según tus necesidades.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img19.png)

> **TAREA FINALIZADA**

### Resultado esperado:
Dashboard creado y visualizaciones organizadas para el análisis visual.

---

### Tarea 5: Agregar filtros interactivos

Permitir al usuario explorar los datos seleccionando valores dinámicamente.

#### Tarea 5.1  

- **Paso 1.** Regresa a tu notebook para crear los filtros.

- **Paso 2.** Agrega esta celda antes de tu consulta del gráfico de barras y crea un filtro de tipo dropdown para `Provincia`. El filtro se agregará en la parte superior del notebook.

  ```python
  %python
  dbutils.widgets.dropdown("provincia", "CDMX", ["CDMX", "Jalisco", "Nuevo León", "Puebla", "Todas"])
  provincia = dbutils.widgets.get("provincia")
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img20.png)

  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img21.png)

- **Paso 3.** Modifica la consulta SQL para aplicar el filtro.

  ```sql
  SELECT Provincia, SUM(TotalVentas) AS VentasTotales
  FROM resumen_ventas
  WHERE ('${provincia}' = 'Todas' OR Provincia = '${provincia}')
  GROUP BY Provincia
  ORDER BY VentasTotales DESC;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img22.png)

- **Paso 4.** Agrega un filtro adicional por `Año`. Antes de la celda de líneas, agrega el filtro.

  ```python
  %python
  dbutils.widgets.dropdown("anio", "2024", ["2022", "2023", "2024", "2025"])
  anio = dbutils.widgets.get("anio")
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img23.png)

- **Paso 5.** Ajusta la consulta:

  ```sql
  SELECT CONCAT(Anio, '-', LPAD(Mes, 2, '0')) AS Periodo,
      SUM(TotalVentas) AS VentasMensuales
  FROM resumen_ventas
  WHERE Provincia = 'CDMX' AND Anio = ${anio}
  GROUP BY Anio, Mes
  ORDER BY Periodo;
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img24.png)

- **Paso 6.** Ve al dashboard donde están todos los gráficos. Ajusta los filtros para la prueba, luego haz clic en el botón `Run All`, y observa los cambios.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img25.png)
    
  ---
    
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img26.png)
  
  ---
    
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab4/img27.png)

> **TAREA FINALIZADA**

### Resultado esperado:
El dashboard es ahora interactivo y permite analizar según filtros dinámicos.

### Tarea 6: Buenas prácticas y publicación 

Optimizar el dashboard y permitir su consulta futura.

#### Tarea 6.1

Los siguientes pasos son de referencia, el paso 3 no podra aplicarse.

- **Paso 1** Revisa los títulos y ejes de cada gráfico; estos deben ser claros y descriptivos.

- **Paso 2.** Ajusta el orden, tamaño y visibilidad del dashboard.

- **Paso 3.** Publica el dashboard o compártelo con el equipo para análisis colaborativo.

> 💡 ***Nota:** El dashboard no puede publicarse ya que el nivel de Databricks usado en el curso es `Standard`.*

> **TAREA FINALIZADA**

### Resultado esperado:

Dashboard claro, útil, optimizado y compartido con otros usuarios del workspace.

---

> **¡FELICIDADES, HAS COMPLETADO EL LABORATORIO 4!**

## Resultado final:

El analista habrá creado visualizaciones efectivas a partir de consultas SQL bien estructuradas, configurado un dashboard consolidado e interactivo, y aplicado buenas prácticas en la presentación de sus resultados analíticos.

---
**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo3/lab3.html)** | **[Lista General](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab0.html)**
