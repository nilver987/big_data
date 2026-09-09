# ❄️ Heladas — NILVER

<div class="dim-card amber" markdown>
<div class="dim-card-header"><span>U1 · Batch — Predictiva sin tiempo real</span><span class="integrante">NILVER</span></div>

<p class="dim-question">¿En qué meses del año hay más de 30% de probabilidad histórica de un día con helada (mínima < 0°C) en Juliaca, y qué tan bien se puede predecir ese riesgo a partir de la estacionalidad y el contexto meteorológico del día (humedad, nubosidad, presión)? Responde directamente a la pregunta central: las heladas son el riesgo agrícola más frecuente de la zona.</p>

<div class="dim-body" markdown>

| Campo | Detalle |
|---|---|
| **Indicador(es)** | Probabilidad diaria de helada (`hay_helada`, 0/1, mínima < 0°C — umbral validado con 16 años de datos reales: mayo-agosto superan 40-84% de días con helada); probabilidad mensual histórica y clasificación predicha por el modelo para un día dado. |
| **Decisión que habilita** | El agricultor decide si cubrir sus cultivos sensibles a heladas según el mes de mayor riesgo y la probabilidad diaria estimada. |
| **Fuente batch** | Open-Meteo Historical API — 16 años (2010-2026, ~146,000 registros horarios), agregados a nivel diario con PySpark: mínima diaria de `temperature_2m`, promedio diario de `relative_humidity_2m`, `cloud_cover` y `surface_pressure`. Salida persistida como capa Gold en Parquet, particionada por año. |
| **Fuente streaming** | Mismo origen que U2 (tópico Kafka `clima-juliaca`), acumulado aquí para reentrenamiento periódico — no se procesa en vivo en esta dimensión. |
| **Modelo predictivo** | Clasificación binaria (`pyspark.ml.classification.LogisticRegression`, `hay_helada` como target 0/1) sobre el día del año codificado de forma cíclica (`seno`/`coseno`). Se comparan dos configuraciones: (A) solo estacionalidad, (B) estacionalidad + humedad/nubosidad/presión promedio del día — confirmado con datos reales que humedad y nubosidad sí distinguen días con y sin helada, viento no. |
| **Salida** | Notebook PySpark (`01_heladas_nilver.ipynb`) — gráfico de probabilidad de helada por mes con línea de umbral 30%, boxplot de temperatura mínima por mes, matriz de correlación de variables, matrices de confusión, y modelo ganador guardado, evaluado con AUC y F1 sobre un conjunto de prueba (20%). |
| **Se combina con** | La dimensión de precipitación (B) en un mismo tablero final de "riesgo climático de Juliaca". |

**Resultado obtenido (Config B, ganadora):** AUC 0.961, F1 0.906 — mejora clara sobre la Config A (solo estacionalidad: AUC 0.913, F1 0.857), confirmando que el contexto meteorológico aporta valor real más allá de la fecha.

**Requisitos mínimos**

1. El sistema debe calcular la probabilidad diaria y mensual de helada a partir del histórico horario de Open-Meteo, con un umbral validado contra los datos reales (no supuesto).
2. El sistema debe entrenar y evaluar un modelo de clasificación que compare al menos dos configuraciones de predictores, reportando AUC y F1 sobre un conjunto de prueba independiente.
3. El sistema debe documentar y guardar el modelo ganador de la comparación, con evidencia de `explain()` (lazy evaluation) y de la escritura/lectura particionada en Parquet.

</div>
</div>

<div class="dim-card amber" markdown>
<div class="dim-card-header"><span>U2 · Streaming — Predictiva con tiempo real</span><span class="integrante">Integrante A</span></div>

<p class="dim-question">¿Cuándo se espera que la temperatura cruce el umbral de helada (0°C) en las próximas horas, según la tendencia actual del pronóstico?</p>

<div class="dim-body" markdown>

| Campo | Detalle |
|---|---|
| **Indicador(es)** | Temperatura proyectada en °C para las próximas 1-6 horas; hora estimada de cruce del umbral de helada. |
| **Decisión que habilita** | Activar cobertura/riego anti-helada la noche en que se proyecta el cruce del umbral. |
| **Fuente batch** | El histórico de U1, usado para entrenar el modelo de series de tiempo antes de dejarlo corriendo sobre el flujo. |
| **Fuente streaming** | Tópico Kafka `clima-juliaca`, alimentado por el script productor que hace *polling* a la Forecast API cada hora — campos `temperature_2m`, `timestamp`. |
| **Modelo predictivo** | Suavizado exponencial (Holt-Winters, statsmodels) entrenado sobre el histórico horario de temperatura y corriendo dentro de un job de Spark Structured Streaming que consume el tópico Kafka y aplica la inferencia a cada lectura entrante, actualizando la proyección en vivo. |
| **Salida** | Panel de Grafana en vivo — temperatura actual, curva de proyección de próximas horas, y alerta visual cuando la proyección cruza 0°C. |
| **Se combina con** | El panel de precipitación (B) en el mismo tablero de Grafana. |

**Requisitos mínimos**

1. El sistema debe ingerir lecturas de temperatura en near-real-time desde el tópico Kafka.
2. El job de Spark Structured Streaming debe aplicar el modelo de series de tiempo a cada micro-lote entrante y actualizar la proyección.
3. El sistema debe disparar una alerta visual en Grafana cuando la proyección cruce el umbral de helada.

</div>
</div>
