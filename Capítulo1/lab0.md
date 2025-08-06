# Práctica: Configurar entorno individual en Azure Databricks

## 🎯 Objetivos:
Al finalizar la práctica, serás capaz de:
- Configurar tu propio entorno de trabajo en Azure, creando un grupo de recursos, un workspace de Databricks y un clúster Spark individual, listo para análisis y desarrollo con SQL y notebooks.

## 📝 Requisitos previos:

- Tener una cuenta activa en Azure (credenciales asignadas por el instructor).
- Contar con conexión a internet.
- Poseer conocimientos básicos de Bash y del portal de Azure.
- Haber iniciado sesión en el portal de Azure.

## 🕒 Duración aproximada:
- 30 minutos.

---

**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo4/lab4.html)** | **[Lista general](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab1.html)**

---

## Instrucciones:

### Tarea 1: Configurar entorno individual.

En esta tarea, cada analista creará su propio grupo de recursos y su propio workspace de Databricks. Además, se configurará un clúster Spark con recursos mínimos para ejecutar análisis en SQL.

#### Tarea 1.1.

- **Paso 1.** Inicia sesión en el portal de Azure [aquí](https://portal.azure.com/) y coloca las credenciales que se te asignaron para el curso.

> 💡 ***Nota:** Es posible que se te pida registrar la cuenta con MFA. Si es así, continúa con el proceso; si no, avanza al paso 2.*

- **Paso 2.** Da clic en el ícono de **Cloud Shell** para abrir la terminal.

  ![dbricks1](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img1.png)

- **Paso 3.** Cambia a la terminal **`Bash`**. Da clic en el ícono que aparece en la imagen y confirma la ventana emergente.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img2.png)
  
  ---
    
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img3.png)

- **Paso 4.** Ahora, en el modo **Bash** , define las variables únicas que identifiquen tus recursos y pégalas en la terminal:

> 💡 ***Nota:** Cambia las `xxx`por tus iniciales. Por ejemplo: `jjon`, `mperez`. Edítalo en un **Bloc de notas** si es necesario.*

  ```bash
  export USER_ID=xxxxx
  export LOCATION=eastus
  export RESOURCE_GROUP="rg-${USER_ID}"
  export DATABRICKS_WS="dbw-${USER_ID}"
  export CLUSTER_NAME="cluster-${USER_ID}"
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img4.png)

- **Paso 5.** Verifica que las variables se hayan creado correctamente. Ejecuta los siguientes comandos:

  ```bash
  echo $USER_ID
  echo $LOCATION
  echo $RESOURCE_GROUP
  echo $DATABRICKS_WS
  echo $CLUSTER_NAME
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img5.png)

> 💡 ***Nota:** Si se cierra la terminal, las variables se pierden. Repite el paso 4 para crearlas nuevamente.*

- **Paso 6.** Crea un grupo de recursos.

  ```bash
  az group create --name $RESOURCE_GROUP --location $LOCATION --tags user=$USER_ID lab=lab1
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img6.png)

- **Paso 7.** Crea un workspace de Databricks en Azure:

  ```bash
  az databricks workspace create \
    --resource-group $RESOURCE_GROUP \
    --name $DATABRICKS_WS \
    --location $LOCATION \
    --sku standard \
    --tags user=$USER_ID lab=lab1
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img7.png)
  
  ---
  
> 💡 ***Nota:** Si te aparece el siguiente mensaje, escribe **`Y`**y espera de 3 a 5 minutos mientras se instala el `provider`.*
    
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img8.png)

- **Paso 8.** Ve a tu **grupo de recursos** recientemente creado.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img9.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img10.png)

- **Paso 9.** Da clic en el **Databricks Workspace** que creaste.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img11.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img12.png)

- **Paso 10.** Dentro del workspace, da clic en el ícono del **usuario** superior derecho (esquina superior derecha) y luego en **Setings**.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img13.png)

- **Paso 11.** Haz clic en **Developer** y, en la sección de **Access tokens**, da clic en el botón **Manage**.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img14.png)

- **Paso 12.** Haz clic en **`Generate new token`**, escribe **`dev`** en el campo de texto y da clic en el botón **Generate** y guarda el token temporalmente.

  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img15.png)
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img16.png)

- **Paso 13.** Por seguridad, el token no se muestra en una imagen. Guárdalo temporalmente en un lugar seguro durante el curso.

- **Paso 14.** Una vez creado el workspace, obtén la URL del workspace.

  ```bash
  export DATABRICKS_HOST=$(az databricks workspace show \
    --name $DATABRICKS_WS --resource-group $RESOURCE_GROUP \
    --query workspaceUrl -o tsv)
  echo $DATABRICKS_HOST
  ```

- **Paso 15.** Ahora crea la variable con tu token de autenticación. Sustituye las x por el valor de tu token.

> 💡 ***Nota:** Edítalo antes en un **Bloc de notas**.*

  ```bash
  export DATABRICKS_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxx
  ```

- **Paso 16.** Crea un clúster de procesamiento usando la API REST para mayor facilidad.

  ```bash
  curl -X POST https://$DATABRICKS_HOST/api/2.0/clusters/create \
    -H "Authorization: Bearer $DATABRICKS_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "cluster_name": "'"$CLUSTER_NAME"'",
      "spark_version": "13.3.x-scala2.12",
      "node_type_id": "Standard_D4s_v3",
      "num_workers": 1,
      "spark_conf": {
        "spark.databricks.cluster.profile": "singleNode"
      },
      "custom_tags": {
        "user": "'"$USER_ID"'",
        "lab": "labs",
        "ResourceClass": "SingleNode"
      },
      "autotermination_minutes": 30
    }'
  ```
  
  ---
  
  ![dbricks2](/Custom_NETEC_DBRICKS-DA_INT-Priv/images/lab1/img17.png)

> **TAREA FINALIZADA**

---

> **¡Felicidades, has completado la preparación de tu ambiente!**

## Resultado final:

- Se creó exitosamente un clúster Spark de nodo único, bajo el control individual del usuario.
- El clúster quedó disponible en el Workspace de Databricks para ejecutar notebooks y scripts SQL.
- Se confirmó que el entorno quedó etiquetado, aislado y operativo para análisis de datos.

---

**[⬅️ Atrás](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo4/lab4.html)** | **[Lista general](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/)** | **[Siguiente ➡️](https://netec-mx.github.io/Custom_NETEC_DBRICKS-DA_INT-Priv/Capítulo1/lab1.html)**
