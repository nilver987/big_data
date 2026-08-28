# ❄️ Heladas — NILVER

<div class="dim-card amber" markdown>
<div class="dim-card-header"><span>U1 · Batch — Predictiva sin tiempo real</span><span class="integrante">NAYDER</span></div>

<p class="dim-question">¿Cuál fue la temperatura mínima registrada por mes en Juliaca en los últimos 5 años, y qué temperatura mínima se puede esperar el próximo mes? Responde directamente a la pregunta central: las heladas son el riesgo agrícola más frecuente de la zona.</p>

<div class="dim-body" markdown>

| Campo | Detalle |
|---|---|
| **Indicador(es)** | Temperatura mínima mensual (°C); temperatura mínima proyectada del próximo mes mediante un modelo de regresión. |
| **Decisión que habilita** | El agricultor decide si cubrir sus cultivos sensibles a heladas según el mes de mayor riesgo proyectado. |
| **Fuente batch** | Open-Meteo Historical API — campo `temperature_2m` (horario), agregado a mínimo mensual con PySpark (`groupBy(mes).agg(min("temperature_2m"))`). Esquema clave: fecha, temperature_2m. |
| **Fuente streaming** | Mismo origen que U2 (tópico Kafka `clima-juliaca`), acumulado aquí para reentrenamiento periódico — no se procesa en vivo en esta dimensión. |
| **Modelo predictivo** | Regresión lineal (`pyspark.ml.regression.LinearRegression` o scikit-learn) sobre la serie mensual de mínimas, entrenada una sola vez por corrida del pipeline batch. |
| **Salida** | Notebook Jupyter/PySpark — tabla de mínimas por mes, gráfico de tendencia y valor proyectado del próximo mes, incluyendo el error del modelo (MAE) sobre un conjunto de validación. |
| **Se combina con** | La dimensión de precipitación (B) en un mismo tablero final de "riesgo climático de Juliaca". |

**Requisitos mínimos**

1. El sistema debe calcular la temperatura mínima mensual a partir del histórico horario de Open-Meteo.
2. El sistema debe entrenar y evaluar un modelo de regresión que proyecte la temperatura mínima del siguiente mes.
3. El sistema debe documentar el error del modelo (MAE/RMSE) sobre un conjunto de validación.

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
