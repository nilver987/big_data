# 🌧️ Precipitación — NAYDER

<div class="dim-card blue" markdown>
<div class="dim-card-header"><span>U1 · Batch — Predictiva sin tiempo real</span><span class="integrante">NAYDER</span></div>

<p class="dim-question">¿Cuál fue la precipitación acumulada por mes en Juliaca en los últimos 5 años, y qué acumulado se puede esperar el próximo mes según la tendencia histórica?</p>

<div class="dim-body" markdown>

| Campo | Detalle |
|---|---|
| **Indicador(es)** | Milímetros acumulados por mes; acumulado proyectado del próximo mes mediante un modelo de regresión. |
| **Decisión que habilita** | Planificar riego o proteger zonas bajas de cultivo según el mes de mayor acumulado proyectado. |
| **Fuente batch** | Open-Meteo Historical API — campos `precipitation`/`rain` (horario) o `precipitation_sum` (diario), agregados por mes con PySpark. |
| **Fuente streaming** | Mismo tópico Kafka que U2, acumulado aquí para reentrenamiento periódico. |
| **Modelo predictivo** | Regresión lineal sobre la serie mensual de acumulados (mismo enfoque que la dimensión A, por consistencia técnica del equipo), entrenada una sola vez por corrida batch. |
| **Salida** | Notebook — tabla de acumulados por mes, gráfico de tendencia y proyección del próximo mes, con el error del modelo documentado. |
| **Se combina con** | La dimensión de heladas (A) en el tablero final. |

**Requisitos mínimos**

1. El sistema debe calcular la precipitación acumulada mensual a partir del histórico.
2. El sistema debe entrenar y evaluar un modelo de regresión que proyecte el acumulado del siguiente mes.
3. El sistema debe documentar el error del modelo (MAE/RMSE) sobre un conjunto de validación.

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
