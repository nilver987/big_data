# 🌧️ Precipitación — NAYDER

<div class="dim-card blue" markdown>
<div class="dim-card-header"><span>U1 · Batch — Predictiva sin tiempo real</span><span class="integrante">NAYDER</span></div>

<p class="dim-question">¿En qué meses del año hay más de 30% de probabilidad histórica de un día de lluvia significativa (acumulado diario > 2mm) en Juliaca, y qué tan bien se puede predecir ese riesgo a partir de la estacionalidad y el contexto meteorológico del día (humedad, nubosidad, presión)?</p>

<div class="dim-body" markdown>

| Campo | Detalle |
|---|---|
| **Indicador(es)** | Probabilidad diaria de lluvia significativa (`hay_lluvia_intensa`, 0/1, acumulado diario > 2mm — umbral validado con 16 años de datos reales: diciembre-abril superan 60-78% de días con lluvia, junio-agosto menos de 6%); probabilidad mensual histórica y clasificación predicha por el modelo para un día dado. |
| **Decisión que habilita** | Planificar riego o proteger zonas bajas de cultivo según el mes de mayor riesgo de lluvia proyectado. |
| **Fuente batch** | Open-Meteo Historical API — 16 años (2010-2026, ~146,000 registros horarios), agregados a nivel diario con PySpark: acumulado diario de `precipitation`, promedio diario de `relative_humidity_2m`, `cloud_cover` y `surface_pressure`. Salida persistida como capa Gold en Parquet, particionada por año. |
| **Fuente streaming** | Mismo tópico Kafka que U2, acumulado aquí para reentrenamiento periódico. |
| **Modelo predictivo** | Clasificación binaria (`pyspark.ml.classification.LogisticRegression`, `hay_lluvia_intensa` como target 0/1) sobre el día del año codificado de forma cíclica (`seno`/`coseno`). Se comparan dos configuraciones: (A) solo estacionalidad, (B) estacionalidad + humedad/nubosidad/presión promedio del día — humedad y nubosidad resultaron los predictores más fuertes. |
| **Salida** | Notebook PySpark (`02_precipitacion_nayder.ipynb`) — gráfico de probabilidad de lluvia intensa por mes con línea de umbral 30%, boxplot de precipitación diaria por mes, matriz de correlación de variables, matrices de confusión, y modelo ganador guardado, evaluado con AUC y F1 sobre un conjunto de prueba (20%). |
| **Se combina con** | La dimensión de heladas (A) en el tablero final. |

**Resultado obtenido (Config B, ganadora):** AUC 0.943, F1 0.874 — mejora clara sobre la Config A (solo estacionalidad: AUC 0.837, F1 0.773).

**Requisitos mínimos**

1. El sistema debe calcular la probabilidad diaria y mensual de lluvia significativa a partir del histórico, con un umbral validado contra los datos reales (no supuesto).
2. El sistema debe entrenar y evaluar un modelo de clasificación que compare al menos dos configuraciones de predictores, reportando AUC y F1 sobre un conjunto de prueba independiente.
3. El sistema debe documentar y guardar el modelo ganador de la comparación, con evidencia de `explain()` (lazy evaluation) y de la escritura/lectura particionada en Parquet.

</div>
</div>

<div class="dim-card blue" markdown>
<div class="dim-card-header"><span>U2 · Streaming — Predictiva con tiempo real</span><span class="integrante">Integrante B</span></div>

<p class="dim-question">¿Cuánta lluvia se espera en las próximas horas en Juliaca, y en qué momento se proyecta que supere el umbral de riesgo (ej. 5 mm/h)?</p>

<div class="dim-body" markdown>

| Campo | Detalle |
|---|---|
| **Indicador(es)** | Milímetros de lluvia proyectados por hora, próximas 1-6h; hora estimada de cruce del umbral de riesgo. |
| **Decisión que habilita** | Suspender riego programado o alertar sobre posible anegamiento antes de un evento de lluvia intensa. |
| **Fuente batch** | El histórico de U1, usado para entrenar el modelo de series de tiempo. |
| **Fuente streaming** | Tópico Kafka `clima-juliaca` — campos `precipitation`, `timestamp`. |
| **Modelo predictivo** | Suavizado exponencial (Holt-Winters, statsmodels) entrenado sobre el histórico horario de precipitación y corriendo dentro de un job de Spark Structured Streaming que consume el tópico Kafka y actualiza la proyección con cada lectura entrante. |
| **Salida** | Panel de Grafana en vivo — lluvia proyectada próximas horas y alerta visual cuando supera el umbral definido. |
| **Se combina con** | El panel de heladas (A) en el mismo tablero. |

**Requisitos mínimos**

1. El sistema debe ingerir lecturas de precipitación en near-real-time desde el tópico Kafka.
2. El job de Spark Structured Streaming debe aplicar el modelo de series de tiempo a cada micro-lote y actualizar la proyección.
3. El sistema debe disparar una alerta visual en Grafana cuando la proyección supere el umbral de riesgo.

</div>
</div>
