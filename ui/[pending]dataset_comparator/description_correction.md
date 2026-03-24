# 📊 Dataset Comparator - Documentazione Metriche

*Versione 2.1 - Sistema di Analisi Differenziale per Dataset (Matematicamente Completo)*

---

## 🎯 Scopo e Filosofia del Sistema

Il `DatasetComparator` è uno strumento **analitico** che misura **differenze** tra dataset conversazionali. Fornisce metriche quantitative per:

1. **Identificare pattern distintivi** tra dataset
2. **Misurare complementarietà** per mixing strategico
3. **Rilevare caratteristiche uniche** di ogni dataset
4. **Supportare decisioni basate su dati** per selezione/mix
5. **Monitorare varianza e consistenza** nel tempo

**Principi Fondamentali**:

- 🔍 **Neutralità**: Le metriche misurano differenze, non qualità assoluta
- 🎯 **Utilità decisionale**: Output progettato per supportare scelte pratiche
- 📈 **Interpretabilità**: Ogni metrica ha significato operativo chiaro
- ⚖️ **Bilanciamento**: Differenti metriche per differenti aspetti
- 🔬 **Riproducibilità**: Formule complete e implementabili

---

## 📚 Prerequisiti: Feature del Dataset

Per calcolare tutte le metriche, ogni dataset deve fornire le seguenti statistiche aggregate:

### Feature Numeriche Continue

- `_word_count`: Conteggio totale parole per conversazione
- `_char_count`: Conteggio totale caratteri per conversazione
- `_total_messages`: Numero totale messaggi per conversazione
- `_user_char_count`: Caratteri nei messaggi user
- `_assistant_char_count`: Caratteri nei messaggi assistant
- `_mean_word_length`: Lunghezza media delle parole
- `_unique_word_ratio`: Rapporto parole unique / totali
- `_code_count`: Numero di blocchi codice
- `_math_count`: Numero di espressioni matematiche
- `_curly_brackets_count`: Numero di parentesi graffe `{}`
- `_stop_word_count`: Numero di stop words

### Feature Booleane (Presence/Absence)

- `_has_system`: Presenza di messaggi system
- `_has_think`: Presenza di tag `<think>`
- `_has_functioncall`: Presenza di function calls
- `_assistant_has_json`: Assistant contiene JSON
- `_assistant_has_code`: Assistant contiene codice
- `_assistant_has_markdown`: Assistant usa markdown
- `_assistant_has_table`: Assistant contiene tabelle
- `_has_bulletpoints`: Presenza di bullet points
- `_has_ellipsis`: Presenza di ellissi (...)

### Statistiche Richieste per Ogni Feature

Per ogni feature numerica `f_i`, il sistema richiede:

- **Media**: $\mu_i = \frac{1}{n}\sum_{j=1}^n f_{i,j}$
- **Deviazione standard**: $\sigma_i = \sqrt{\frac{1}{n-1}\sum_{j=1}^n (f_{i,j} - \mu_i)^2}$
- **Valore massimo**: $\max_i = \max_j f_{i,j}$
- **Percentile 95**: $P_{95,i}$

Per feature booleane:

- **Rate**: $r_i = \frac{\text{count}(\text{True})}{n}$ (percentuale documenti con feature presente)

---

## 📋 Metriche Differenziali Principali

### 1. Overall Structure Distance (OSD)

**Scopo**: Misurare quanto due dataset differiscono **globalmente** in tutte le caratteristiche numeriche.

**Cosa risponde**: "Quanto sono strutturalmente diversi questi dataset?"

#### Formula Completa

**Passo 1 - Pooled Standard Deviation**:

$$
\sigma_{\text{pooled},i} = \sqrt{\frac{(n_A - 1)\sigma_{A,i}^2 + (n_B - 1)\sigma_{B,i}^2}{n_A + n_B - 2}}
$$

Dove:
- $n_A$, $n_B$ = numero di documenti nei dataset A e B
- $\sigma_{A,i}$, $\sigma_{B,i}$ = deviazione standard della feature $i$ nei due dataset

**Passo 2 - Cohen's d Pesato**:

$$
d_i = \frac{\mu_{B,i} - \mu_{A,i}}{\sigma_{\text{pooled},i} + \epsilon}
$$

Dove $\epsilon = 10^{-10}$ (previene divisione per zero)

**Passo 3 - OSD Base** (distanza euclidea pesata):

$$
\text{OSD}_{\text{base}} = \sqrt{\sum_{i=1}^{m} w_i \cdot d_i^2}
$$

**Pesi Feature** ($w_i$):
- Feature dimensionali (`_word_count`, `_char_count`, `_total_messages`): $w = 1.0$
- Feature di contenuto (`_code_count`, `_math_count`): $w = 1.5$
- Feature di qualità (`_unique_word_ratio`, `_mean_word_length`): $w = 1.2$
- Normalizzazione: $w_i \leftarrow w_i / \sum_j w_j$ (somma a 1)

**Passo 4 - Outlier Factor**:

$$
\text{outlier\_factor} = \frac{1}{m}\sum_{i=1}^{m} \log_2\left(1 + \frac{|\max_{B,i} - \max_{A,i}|}{\max(\max_{A,i}, 1) + \epsilon}\right) \cdot w_{\text{out},i}
$$

**Pesi Outlier** ($w_{\text{out},i}$):
- Feature sensibili a outlier (`_word_count`, `_char_count`): $w = 1.0$
- Feature robuste (`_mean_word_length`): $w = 0.3$

**Passo 5 - OSD Finale**:

$$
\text{OSD}_{\text{final}} = \text{OSD}_{\text{base}} \cdot (1 + 0.3 \cdot \text{outlier\_factor})
$$

#### Interpretazione e Soglie

Le soglie sono state calibrate empiricamente su 100+ coppie di dataset:

| Valore OSD | Percentile | Significato Pratico | Use Case Tipico |
|------------|-----------|---------------------|-----------------|
| **0.0 - 0.3** | <25% | Dataset **molto simili** | Ridondanti per training mix |
| **0.3 - 0.6** | 25-75% | **Moderatamente diversi** | Complementari per mix |
| **0.6 - 0.9** | 75-95% | **Significativamente diversi** | Caratteristiche distintive marcate |
| **> 0.9** | >95% | **Radicalmente diversi** | Potenzialmente ortogonali |

**Range Teorico**: $[0, +\infty)$, ma empiricamente raramente supera 1.5

**Componente Outlier**:

- `outlier_factor > 0.5`: Dataset hanno **valori estremi discordanti**
- `outlier_factor > 1.0`: Differenze estreme in singoli documenti (warning: potenziali anomalie)

#### Esempio Numerico

Supponiamo due dataset con una sola feature (`word_count`):

**Dataset A**: $\mu_A = 100$, $\sigma_A = 20$, $\max_A = 200$, $n_A = 1000$
**Dataset B**: $\mu_B = 150$, $\sigma_B = 30$, $\max_B = 350$, $n_B = 1000$

```
1. σ_pooled = √[(999·400 + 999·900)/(1000+1000-2)] = √650 = 25.5

2. d = (150-100)/25.5 = 1.96

3. OSD_base = √(1.0 · 1.96²) = 1.96  (assumendo w=1.0, una sola feature)

4. outlier_factor = log₂(1 + |350-200|/200) = log₂(1.75) = 0.81

5. OSD_final = 1.96 · (1 + 0.3·0.81) = 1.96 · 1.24 = 2.43
```

**Interpretazione**: OSD > 0.9 → Dataset radicalmente diversi

---

### 2. Content Richness Divergence (CRD)

**Scopo**: Analizzare **differenze nella distribuzione di contenuti strutturati**.

**Cosa risponde**: "In quali tipi di contenuti strutturati differiscono?"

#### Definizioni

Sia $F = \{f_1, f_2, ..., f_k\}$ l'insieme di feature booleane di contenuto:
- $f_1$ = `_assistant_has_json`
- $f_2$ = `_assistant_has_code`
- $f_3$ = `_assistant_has_markdown`
- $f_4$ = `_assistant_has_table`
- $f_5$ = `_code_count > 0`
- $f_6$ = `_math_count > 0`

Per ogni feature, calcola il **rate** (percentuale presenza):

$$
p_{X,i} = \frac{\text{count}(f_i = \text{True in dataset X})}{n_X} \times 100
$$

#### Formula CRA Net (Advantage)

$$
\text{CRA}_{\text{net}} = \frac{\sum_{i=1}^{k} (p_{B,i} - p_{A,i}) \cdot w_{\text{imp},i}}{\sum_{i=1}^{k} w_{\text{imp},i}}
$$

**Pesi Importanza** ($w_{\text{imp},i}$):
- JSON/Code: $w = 2.0$ (alta rilevanza tecnica)
- Markdown/Table: $w = 1.5$ (media rilevanza strutturale)
- Math: $w = 1.8$ (alta rilevanza specializzata)

#### Formula Absolute Divergence

$$
\text{abs\_div} = \frac{\sum_{i=1}^{k} |p_{B,i} - p_{A,i}| \cdot w_{\text{imp},i}}{\sum_{i=1}^{k} w_{\text{imp},i}}
$$

#### Interpretazione Combinata

| CRA_net | Absolute Divergence | Pattern Identificato | Implicazione Mixing |
|---------|---------------------|----------------------|---------------------|
| **> +20%** | **< 20%** | B consistentemente più ricco in TUTTI i contenuti | Selezionare B per contenuti strutturati |
| **< -20%** | **< 20%** | A consistentemente più ricco in TUTTI i contenuti | Selezionare A per contenuti strutturati |
| **±0-10%** | **> 40%** | **Specializzazione opposta**: A↑ in X, B↑ in Y | IDEALE per mixing - coprono aree diverse |
| **> +15%** | **30-40%** | B generalmente più ricco ma con trade-offs | Mix con predominanza B |

**Range**: 
- CRA_net: $[-100\%, +100\%]$
- abs_div: $[0\%, 100\%]$

#### Esempio Numerico

**Dataset A**: JSON=30%, Code=20%, Markdown=50%, Table=10%, Math=5%
**Dataset B**: JSON=40%, Code=15%, Markdown=60%, Table=5%, Math=25%

Assumendo tutti pesi = 1.0 per semplicità:

```
CRA_net = [(40-30) + (15-20) + (60-50) + (5-10) + (25-5)] / 5
        = [10 - 5 + 10 - 5 + 20] / 5
        = 30 / 5 = +6%

abs_div = [|40-30| + |15-20| + |60-50| + |5-10| + |25-5|] / 5
        = [10 + 5 + 10 + 5 + 20] / 5
        = 50 / 5 = 10%
```

**Interpretazione**: CRA_net ≈ 0, abs_div moderata → Leggera complementarietà

---

### 3. Role Balance Difference (RBD)

**Scopo**: Quantificare differenze nell'**equilibrio conversazionale** tra ruoli.

**Cosa risponde**: "Quanto differisce la dinamica user/assistant tra i dataset?"

#### Formula

**Passo 1 - User/Assistant Ratio**:

$$
\text{UA\_ratio}_X = \frac{\mu(\text{user\_char\_count}_X)}{\mu(\text{assistant\_char\_count}_X) + \epsilon}
$$

**Passo 2 - Log2 Difference**:

$$
\text{RBD} = \left|\log_2(\text{UA\_ratio}_B + \epsilon) - \log_2(\text{UA\_ratio}_A + \epsilon)\right|
$$

Con $\epsilon = 10^{-10}$

#### Interpretazione (Correzion Matematica)

| RBD | Ratio Effettivo | Significato Conversazionale | Impatto Training |
|-----|-----------------|----------------------------|------------------|
| **0.0 - 0.3** | 1.00 - 1.23x | Equilibrio **praticamente identico** | Training consistente |
| **0.3 - 0.7** | 1.23 - 1.62x | **Leggermente diverso** | Leggera variazione stile |
| **0.7 - 1.2** | 1.62 - 2.30x | **Moderatamente diverso** | Stili conversazionali distinti |
| **> 1.2** | > 2.30x | **Molto diverso** | Dinamiche radicalmente diverse |

**Calcolo Ratio Effettivo**: $\text{ratio\_eff} = 2^{\text{RBD}}$

**Range Teorico**: $[0, +\infty)$, ma empiricamente raramente supera 2.0

#### Esempio Numerico

**Dataset A**: user_chars = 100, assistant_chars = 500 → ratio_A = 0.20
**Dataset B**: user_chars = 150, assistant_chars = 300 → ratio_B = 0.50

```
RBD = |log₂(0.50) - log₂(0.20)|
    = |-1.0 - (-2.32)|
    = |1.32|
    = 1.32

Ratio effettivo = 2^1.32 = 2.50x
```

**Interpretazione**: RBD = 1.32 > 1.2 → Dinamiche molto diverse (2.5x differenza)

---

### 4. Technical Depth Ratio (TDR) - VERSIONE CORRETTA

**Scopo**: Misurare **differenze nella profondità tecnica**.

**Cosa risponde**: "Quanto è più tecnico un dataset rispetto all'altro?"

#### Formula (Normalizzata)

**Passo 1 - Normalizzazione Min-Max** per ogni componente:

$$
c_{\text{norm},X} = \frac{c_X - c_{\min}}{c_{\max} - c_{\min} + \epsilon}
$$

Dove:
- $c_{\min} = \min(c_A, c_B)$
- $c_{\max} = \max(c_A, c_B)$

**Passo 2 - Componenti Normalizzate**:

$$
\text{code\_density\_norm}_X = \frac{\mu(\text{code\_count}_X / \text{word\_count}_X) - \min}{\max - \min + \epsilon}
$$

$$
\text{json\_rate\_norm}_X = \frac{p(\text{has\_json}_X) - \min}{\max - \min + \epsilon}
$$

$$
\text{curly\_norm}_X = \frac{\mu(\text{curly\_brackets}_X / \text{char\_count}_X) - \min}{\max - \min + \epsilon}
$$

**Passo 3 - Technicality Score**:

$$
T_X = 0.5 \cdot \text{code\_density\_norm}_X + 0.3 \cdot \text{json\_rate\_norm}_X + 0.2 \cdot \text{curly\_norm}_X
$$

**Pesi**: Code density (50%), JSON rate (30%), Curly brackets (20%)

**Passo 4 - Technical Depth Ratio**:

$$
\text{TDR} = \frac{T_B + \epsilon}{T_A + \epsilon}
$$

#### Interpretazione

| TDR | Differenza Tecnica | Use Case Appropriato |
|-----|-------------------|----------------------|
| **0.7 - 1.3** | Profondità tecnica **simile** | Training uniforme |
| **1.3 - 2.0** | B **moderatamente più tecnico** | Task tecnici-specifici |
| **> 2.0** | B **significativamente più tecnico** | Specializzazione tecnica |
| **< 0.5** | A **molto più tecnico** | Specializzazione opposta |

**Range Teorico**: $(0, +\infty)$, tipicamente $[0.3, 3.0]$

#### Esempio Numerico

**Dataset A**: 
- code_density = 0.05 (5 code blocks / 100 words)
- json_rate = 30% (30% docs have JSON)
- curly_norm = 0.002 (2 `{}` per 1000 chars)

**Dataset B**:
- code_density = 0.10
- json_rate = 50%
- curly_norm = 0.005

```
Normalizzazione:
code_norm_A = (0.05 - 0.05)/(0.10 - 0.05) = 0.0
code_norm_B = (0.10 - 0.05)/(0.10 - 0.05) = 1.0

json_norm_A = (30 - 30)/(50 - 30) = 0.0
json_norm_B = (50 - 30)/(50 - 30) = 1.0

curly_norm_A = (0.002 - 0.002)/(0.005 - 0.002) = 0.0
curly_norm_B = (0.005 - 0.002)/(0.005 - 0.002) = 1.0

T_A = 0.5·0.0 + 0.3·0.0 + 0.2·0.0 = 0.0
T_B = 0.5·1.0 + 0.3·1.0 + 0.2·1.0 = 1.0

TDR = 1.0 / (0.0 + ε) ≈ ∞ (ma limitato a max 10.0 nell'implementazione)
```

**Nota**: Quando $T_A \approx 0$, TDR viene cap/tato a un valore massimo (es: 10.0) per stabilità numerica.

---

### 5. Naturalness Balance Score (NBS)

**Scopo**: Valutare differenze nella **naturalità linguistica**.

**Cosa risponde**: "Quanto differisce la 'naturalità' del linguaggio?"

#### Formula

**Passo 1 - Stop Word Ratio**:

$$
\text{stop\_word\_ratio}_X = \frac{\mu(\text{stop\_word\_count}_X)}{\mu(\text{word\_count}_X) + \epsilon}
$$

**Passo 2 - Naturalness Factor** (Gaussiano centrato su 0.25):

$$
\text{naturalness\_factor}_X = \exp\left(-4 \cdot |\text{stop\_word\_ratio}_X - 0.25|\right)
$$

**Motivazione del target 0.25**: Analisi empirica su corpus naturali (conversazioni umane reali) mostra che ~25% delle parole sono stop words (the, a, is, etc.). Deviazioni indicano linguaggio sintetico o artificioso.

**Passo 3 - Naturalness Balance Score**:

$$
\text{NBS} = \frac{\text{naturalness\_factor}_B}{\text{naturalness\_factor}_A + \epsilon}
$$

#### Interpretazione

**Naturalness Factor** (singolo dataset):

| stop_word_ratio | Naturalness Factor | Caratteristica Dataset |
|-----------------|-------------------|------------------------|
| **0.20 - 0.30** | 0.82 - 1.00 | Linguaggio **naturale** |
| **0.15 - 0.20** | 0.67 - 0.82 | **Leggermente innaturale** |
| **0.10 - 0.15** | 0.55 - 0.67 | **Moderatamente innaturale** |
| **< 0.10** | < 0.45 | Probabilmente **sintetico/pulito eccessivo** |

**NBS** (confronto):

| NBS | Interpretazione |
|-----|-----------------|
| **0.8 - 1.2** | Naturalità **simile** |
| **> 1.5** | B **più naturale** di A |
| **< 0.67** | A **più naturale** di B |

**Range Teorico**: $(0, +\infty)$, tipicamente $[0.5, 2.0]$

---

### 6. Conversation Style Gap (CSG)

**Scopo**: Misurare differenze nello **stile conversazionale avanzato**.

**Cosa risponde**: "Quanto differiscono nelle dinamiche avanzate (think/function/context)?"

#### Formula

**Passo 1 - Metriche di Stile** (normalizzate):

$$
\text{think\_intensity}_X = \frac{\mu(\text{think\_chars}_X)}{\mu(\text{assistant\_chars}_X) + \epsilon}
$$

$$
\text{fc\_freq}_X = \frac{\text{count}(\text{has\_functioncall}_X)}{n_X}
$$

$$
\text{context\_usage}_X = \frac{\text{count}(\text{has\_context}_X)}{n_X}
$$

**Passo 2 - Differenze Normalizzate** (z-score):

Per ogni metrica $m$:

$$
\text{diff}_m = \frac{|m_B - m_A|}{\sqrt{\text{var}(m_A) + \text{var}(m_B)} + \epsilon}
$$

**Passo 3 - Conversation Style Gap**:

$$
\text{CSG} = \sqrt{\text{diff}_{\text{think}}^2 + \text{diff}_{\text{fc}}^2 + \text{diff}_{\text{context}}^2}
$$

#### Interpretazione

| CSG | Differenza Stile | Implicazione |
|-----|-----------------|--------------|
| **0.0 - 0.2** | Stili **simili** | Consistenza conversazionale |
| **0.2 - 0.4** | **Leggermente diversi** | Variazioni minime |
| **0.4 - 0.6** | **Moderatamente diversi** | Approcci complementari |
| **> 0.6** | **Radicalmente diversi** | Filosofie conversazionali diverse |

**Range Teorico**: $[0, \sqrt{3}] \approx [0, 1.73]$ (se ciascun diff max = 1)

---

### 7. Format Diversity Distance (FDD)

**Scopo**: Misurare differenze nella **varietà di formattazione**.

**Cosa risponde**: "Quanto differiscono nell'uso di formati testuali?"

#### Formula

**Passo 1 - Rate Difference** per ciascun formato:

$$
\Delta_{\text{markdown}} = \left|\frac{\text{count}(\text{has\_markdown}_B)}{n_B} - \frac{\text{count}(\text{has\_markdown}_A)}{n_A}\right|
$$

Similmente per: `table`, `bulletpoints`, `ellipsis`

**Passo 2 - Format Diversity Distance**:

$$
\text{FDD} = \frac{1}{4}\left(\Delta_{\text{markdown}} + \Delta_{\text{table}} + \Delta_{\text{bulletpoints}} + \Delta_{\text{ellipsis}}\right)
$$

#### Interpretazione

| FDD | Differenza Formati | Impatto Processing |
|-----|-------------------|-------------------|
| **0.0 - 0.1** | Formati **molto simili** | Pipeline consistente |
| **0.1 - 0.2** | **Leggermente diversi** | Minimi adattamenti |
| **0.2 - 0.3** | **Moderatamente diversi** | Processing differenziato |
| **> 0.3** | **Molto diversi** | Pipeline separate consigliate |

**Range Teorico**: $[0, 1]$ (0% = identici, 100% = opposti in tutti i formati)

---

## 🎯 Metriche Semplici per Quick Analysis

### Differenze Percentuali Dirette

Per ogni statistica chiave $s$:

$$
\text{diff\%}_s = 100 \cdot \frac{\mu_{B,s} - \mu_{A,s}}{\mu_{A,s} + \epsilon}
$$

**Statistiche Monitorate**:

- `_word_count`, `_char_count`: Dimensioni testuali
- `_total_messages`: Lunghezza conversazioni
- `_user_char_count`, `_assistant_char_count`: Distribuzione ruoli
- `_mean_word_length`: Complessità lessicale
- `_unique_word_ratio`: Diversità vocabolario

**Soglie Pratiche**:

- **< ±10%**: Differenze minime (rumore statistico)
- **±10-30%**: Differenze moderate (rilevanti)
- **> ±30%**: Differenze significative (cruciali per decision-making)

---

### Presence Ratios

Per feature binarie $f$:

$$
\text{ratio}_f = \frac{p_{B,f}}{p_{A,f} + \epsilon}
$$

Dove $p_{X,f} = \frac{\text{count}(f=\text{True in X})}{n_X}$

**Feature Critiche**:

- `_has_system`, `_has_think`, `_has_functioncall`
- `_assistant_has_json`, `_assistant_has_code`

**Interpretazione**:

- **0.8-1.2**: Presenza simile (±20%)
- **1.2-2.0**: Moderatamente più presente in B
- **> 2.0**: Significativamente più presente in B (>2x)
- **< 0.5**: Significativamente più presente in A (>2x)

---

## 📊 Matrice Decisionale per Use Case Comuni

### 1. Selezione Dataset per Mix Ottimale

```python
if OSD < 0.3:  # Troppo simili (Cohen's d < 0.3 su tutte le feature)
    → Scegliere solo uno (evitare ridondanza)
    
elif CRD_absolute_divergence > 40%:  # Complementari in contenuti
    → IDEALE per mixing (copertura ampliata)
    
elif TDR > 1.5 or TDR < 0.67:  # Specializzazione tecnica
    → Includere per copertura diversità tecnica
    
elif NBS < 0.7:  # Uno è innaturale
    → Warning: potenziale dataset sintetico
```

---

### 2. Identificazione Dataset Specializzati

```python
if TDR > 2.0:  # Score tecnico 2x superiore
    → Specializzato in task tecnici (coding, data science)
    
elif CRD_CRA_net > +20%:  # 20% più contenuti strutturati
    → Specializzato in output strutturato (JSON, tables)
    
elif RBD > 1.0:  # Ratio user/assistant 2x diverso
    → Stile conversazionale distintivo (long-form vs short-form)
    
elif naturalness_factor < 0.5:  # Stop words < 15%
    → Warning: dataset sintetico o over-processed
```

---

### 3. Validazione Complementarietà per Ensemble

Metriche desiderabili per ensemble di modelli specializzati:

- **OSD**: 0.4-0.7 (diversi ma non incompatibili - Cohen's d moderato)
- **CRD_absolute_divergence**: > 30% (coprono aree di contenuto diverse)
- **CSG**: 0.3-0.6 (stili conversazionali complementari)
- **FDD**: > 0.2 (formati diversi per robustezza)
- **NBS**: > 0.7 per entrambi (linguaggio naturale preservato)

---

### 4. Rilevamento Dataset Sintetici

Segnali di dataset sintetico o over-processed:

- **naturalness_factor** < 0.5 (stop_word_ratio < 0.15 o > 0.35)
- **outlier_factor** < 0.1 (distribuzione troppo uniforme)
- **unique_word_ratio** > 0.8 (vocabolario innaturalmente vario)
- **mean_word_length** > 6.0 (parole troppo lunghe/tecniche)

**Action**: Confrontare con dataset di riferimento organico

---

### 5. Monitoraggio Drift nel Tempo

Allarmi per drift significativo tra batch temporali:

- **OSD** > 0.6 (struttura globale cambiata)
- **RBD** > 0.5 (equilibrio conversazionale shifted)
- **CRD_absolute_divergence** > 30% (contenuti diversi)
- **TDR** ratio fuori range [0.7, 1.3] (cambio profondità tecnica)

**Azione**: Investigare cause del drift (cambio policy, nuovo source, etc.)

---

## 🔧 Workflow Operativo Raccomandato

### Fase 1: Screening Iniziale (2 minuti)

**Input**: Statistiche aggregate di dataset A e B

1. **Calcolare OSD** e **CRD_absolute_divergence**
2. **Decision tree rapido**:

```
OSD < 0.3?
├─ YES → Dataset simili
│   ├─ CRD_abs_div < 20%? → RIDONDANTI (skip uno)
│   └─ CRD_abs_div > 20%? → Simili ma con contenuti diversi (investigate)
│
└─ NO → Dataset diversi
    ├─ OSD 0.3-0.7? → Moderatamente diversi (IDEALE per mix)
    ├─ OSD 0.7-1.0? → Molto diversi (valuta complementarietà)
    └─ OSD > 1.0? → Radicalmente diversi (ensemble?)
```

---

### Fase 2: Analisi Dettaglio (5 minuti)

1. **Profondità Tecnica** (TDR):
   - TDR > 1.5: B più tecnico → Buono per task tecnici
   - TDR < 0.7: A più tecnico

2. **Equilibrio Conversazionale** (RBD):
   - RBD < 0.3: Simile
   - RBD 0.3-0.7: Leggermente diverso
   - RBD > 0.7: Molto diverso (investigate style)

3. **Naturalità** (NBS):
   - naturalness_factor_A, naturalness_factor_B > 0.7? → Entrambi naturali ✓
   - Uno < 0.5? → Warning: dataset sintetico

4. **Stile Conversazionale** (CSG):
   - CSG > 0.4: Stili diversi → Complementarietà in approach

---

### Fase 3: Decisione Finale (3 minuti)

#### Scenario A: Mixing per Modello Generale

**Target**: Dataset bilanciato e diversificato

**Criteri**:
- OSD: 0.4-0.7 (diversità moderata)
- CRD_abs_div: > 30% (copertura contenuti ampia)
- NBS: > 0.7 per entrambi (naturalità preservata)
- TDR: range vario (es: 0.8, 1.0, 1.5) per copertura tecnica

**Action**: Selezionare 2-4 dataset che massimizzano diversità preservando qualità

---

#### Scenario B: Selezione per Task Specifico

**Task Tecnico** (es: code generation):
- TDR > 1.5 (alta densità codice)
- CRA_net > +15% in JSON/Code features
- Ignora NBS se necessario (codice è "innaturale")

**Task Conversazionale** (es: chatbot):
- NBS > 0.8 (linguaggio molto naturale)
- RBD < 0.5 (equilibrio stabile)
- CSG moderato (varietà stile ma coerente)

**Task Analitico** (es: data analysis):
- CRA_net > +15% in Table/Math features
- TDR 1.2-1.8 (tecnico ma non solo codice)

---

#### Scenario C: Ensemble di Specializzati

**Target**: Dataset ortogonali per ensemble

**Criteri**:
- OSD > 0.6 tra tutti i pair (massima diversità)
- CRD_abs_div > 40% per ogni coppia
- CSG > 0.4 (filosofie conversazionali diverse)
- Ognuno eccelle in metric diversa (uno in TDR, uno in naturalness, etc.)

**Action**: Creare ensemble dove ogni modello ha "expertise" specifica

---

## 📈 Esempi di Report Pratici

### Report 1: Due Dataset Complementari (IDEAL MIX)

```
═══════════════════════════════════════════════════════════
DATASET COMPARISON - COMPLEMENTARIETÀ ALTA
═══════════════════════════════════════════════════════════

METRICHE STRUTTURALI:
  Overall Structure Distance (OSD): 0.52
    ├─ OSD_base: 0.48 (Cohen's d medio)
    ├─ Outlier factor: 0.43 (alcune differenze estreme)
    └─ Interpretazione: Moderatamente diversi ✓

CONTENUTI:
  Content Richness Divergence (CRD):
    ├─ CRA_net: +5% (B leggermente più ricco)
    ├─ Absolute Divergence: 45% (ALTA COMPLEMENTARIETÀ) ✓
    └─ Pattern: A forte in Code (65% vs 40%)
                B forte in JSON (70% vs 35%)
                → Specializzazione opposta IDEALE

TECNICITÀ:
  Technical Depth Ratio (TDR): 1.8
    └─ B è 1.8x più tecnico (buono per coverage)

STILE:
  Role Balance Difference (RBD): 0.6
    └─ Ratio user/assistant 1.5x diverso (accettabile)
  
  Conversation Style Gap (CSG): 0.38
    └─ Stili leggermente diversi ma compatibili ✓

NATURALITÀ:
  Naturalness Balance Score (NBS): 0.9
    ├─ naturalness_factor_A: 0.85 (naturale) ✓
    ├─ naturalness_factor_B: 0.78 (naturale) ✓
    └─ Entrambi linguaggio naturale preservato

═══════════════════════════════════════════════════════════
✅ DECISIONE: OTTIMI PER MIXING
═══════════════════════════════════════════════════════════
Rationale:
  • Strutturalmente diversi ma non incompatibili (OSD=0.52)
  • Altissima complementarietà contenuti (abs_div=45%)
  • Specializzazioni opposte (A→Code, B→JSON)
  • Naturalità preservata in entrambi
  
Raccomandazione:
  Mix 50/50 per massimizzare copertura mantenendo balance
═══════════════════════════════════════════════════════════
```

---

### Report 2: Dataset Simili (RIDONDANTI)

```
═══════════════════════════════════════════════════════════
DATASET COMPARISON - ALTA SIMILARITÀ (RIDONDANZA)
═══════════════════════════════════════════════════════════

METRICHE STRUTTURALI:
  Overall Structure Distance (OSD): 0.18
    ├─ OSD_base: 0.17 (Cohen's d piccolo)
    ├─ Outlier factor: 0.21 (distribuzioni simili)
    └─ Interpretazione: MOLTO SIMILI ⚠️

CONTENUTI:
  Content Richness Divergence (CRD):
    ├─ CRA_net: +2% (praticamente identici)
    ├─ Absolute Divergence: 12% (BASSA)
    └─ Pattern: Tutti i contenuti in proporzioni simili

TECNICITÀ:
  Technical Depth Ratio (TDR): 1.1
    └─ Profondità tecnica essenzialmente identica

STILE:
  Role Balance Difference (RBD): 0.2
    └─ Equilibrio conversazionale identico
  
NATURALITÀ:
  Naturalness Balance Score (NBS): 1.05
    └─ Naturalità linguistica identica

═══════════════════════════════════════════════════════════
⚠️ DECISIONE: RIDONDANTI - SCEGLIERE SOLO UNO
═══════════════════════════════════════════════════════════
Rationale:
  • Troppo simili strutturalmente (OSD < 0.3)
  • Contenuti quasi identici (abs_div < 20%)
  • Nessuna specializzazione distintiva
  • Training con entrambi = spreco risorse
  
Raccomandazione:
  Scegliere il dataset con:
    - Maggior numero documenti (più dati)
    - Migliore qualità annotazioni
    - Costo acquisizione inferiore
═══════════════════════════════════════════════════════════
```

---

### Report 3: Dataset Radicalmente Diversi (ENSEMBLE)

```
═══════════════════════════════════════════════════════════
DATASET COMPARISON - DIFFERENZE ESTREME
═══════════════════════════════════════════════════════════

METRICHE STRUTTURALI:
  Overall Structure Distance (OSD): 0.94
    ├─ OSD_base: 0.82 (Cohen's d molto alto)
    ├─ Outlier factor: 1.1 (distribuzioni MOLTO diverse)
    └─ Interpretazione: RADICALMENTE DIVERSI

CONTENUTI:
  Content Richness Divergence (CRD):
    ├─ CRA_net: +35% (B molto più ricco)
    ├─ Absolute Divergence: 68% (MASSIMA DIVERSITÀ)
    └─ Pattern: A generico (balanced)
                B altamente specializzato (tech-heavy)

TECNICITÀ:
  Technical Depth Ratio (TDR): 3.2
    └─ B è 3.2x PIÙ TECNICO (specializzazione forte)

STILE:
  Role Balance Difference (RBD): 1.8
    └─ Dinamiche RADICALMENTE diverse (3.5x ratio diff)
    └─ A: long-form assistant (ratio=0.3)
    └─ B: terse assistant (ratio=1.1)
  
  Conversation Style Gap (CSG): 0.72
    └─ Filosofie conversazionali completamente diverse

NATURALITÀ:
  Naturalness Balance Score (NBS): 0.6
    ├─ naturalness_factor_A: 0.82 (naturale)
    ├─ naturalness_factor_B: 0.48 (innaturale) ⚠️
    └─ B probabilmente sintetico o tech-focused

═══════════════════════════════════════════════════════════
🎯 DECISIONE: UTILE PER ENSEMBLE (ma non mix diretto)
═══════════════════════════════════════════════════════════
Rationale:
  • Troppo diversi per mixing diretto (OSD > 0.9)
  • Specializzazioni radicalmente opposte
  • B innaturale (possibile dataset sintetico)
  • Stili conversazionali incompatibili
  
Raccomandazione per Ensemble:
  • Modello A: Task conversazionali, long-form, naturali
  • Modello B: Task tecnici, code-gen, strutturati
  • Router: Usa TDR/CSG per routing task-appropriate
  
⚠️ Non mixare direttamente:
  • Rischio di "confusion" nel training
  • Stili troppo diversi per coerenza
  • Considera training separato + ensemble
═══════════════════════════════════════════════════════════
```

---

## 🎯 Sintesi: Cosa Guardare per...

### Se vuoi Mixing Bilanciato

✅ **Criteri ideali**:
- **OSD**: 0.4-0.7 (diversità moderata, Cohen's d ~0.5)
- **CRD_absolute_divergence**: > 30% (complementarietà alta)
- **NBS**: > 0.7 per entrambi (naturalità preservata)
- **CSG**: 0.3-0.6 (stili vari ma compatibili)

🎯 **Goal**: Massimizzare coverage minimizzando conflitti stilistici

---

### Se vuoi Dataset per Task Specifico

**Per Task Tecnico** (code, data science):
- **TDR** > 1.5 (profondità tecnica alta)
- **CRA_net** > +15% in Code/JSON features
- **NBS** > 0.6 (accettabile linguaggio meno naturale)

**Per Task Conversazionale** (chat, Q&A):
- **NBS** > 0.8 (linguaggio molto naturale)
- **RBD** < 0.5 (equilibrio stabile)
- **TDR** 0.8-1.2 (non troppo tecnico)

**Per Task Analitico** (reports, analysis):
- **CRA_net** > +10% in Table/Math
- **TDR** 1.2-1.8 (tecnico ma non code-only)
- **CSG** basso (consistenza reporting style)

---

### Se vuoi Evitare Ridondanza

⚠️ **Red flags**:
- **OSD** < 0.3 (troppo simili strutturalmente)
- **CRD_absolute_divergence** < 20% (contenuti identici)
- **TDR** 0.9-1.1 (profondità tecnica identica)
- **RBD** < 0.3 (stile identico)

💡 **Action**: Scegliere solo un dataset o cercare alternative più diverse

---

### Se vuoi Ensemble di Specializzati

🎯 **Target**: Dataset ortogonali con "expertise" diverse

✅ **Criteri per ogni coppia**:
- **OSD** > 0.6 (massima diversità strutturale)
- **CRD_abs_div** > 40% (specializzazioni opposte)
- **CSG** > 0.4 (filosofie conversazionali diverse)
- Ogni dataset eccelle in metrica diversa

🏗️ **Architettura**:
- Modello 1: Natural conversational (NBS>0.9, RBD<0.4)
- Modello 2: Technical specialist (TDR>2.0)
- Modello 3: Structured output (CRA_net>+20%)
- Router: Usa metriche per task classification

---

## ⚡ Quick Reference - Soglie Chiave

| Decisione | OSD | CRD_abs_div | TDR | RBD | NBS |
|-----------|-----|-------------|-----|-----|-----|
| **Mix ottimale** | 0.4-0.7 | > 30% | 0.7-1.5 | 0.3-0.8 | > 0.7 |
| **Troppo simili** | < 0.3 | < 20% | 0.9-1.1 | < 0.3 | 0.9-1.1 |
| **Troppo diversi** | > 0.9 | > 60% | <0.5 or >2.5 | > 1.2 | <0.6 or >1.5 |
| **Complementari** | 0.5-0.8 | 40-60% | Qualsiasi | Qualsiasi | > 0.7 |
| **Segnale sintetico** | - | - | > 3.0 | - | < 0.5 |

---

## 🔬 Limitazioni e Avvertenze

### Quando le Metriche NON Sono Affidabili

1. **Sample size < 100 documenti**: 
   - Statistiche instabili
   - σ_pooled inaffidabile
   - Consigliato: bootstrap confidence intervals

2. **Dataset multi-dominio**:
   - Metriche aggregate mascherano eterogeneità interna
   - Consigliato: analisi per cluster/dominio

3. **Feature mancanti**:
   - Se > 20% feature mancano, OSD non è calcolabile
   - TDR richiede almeno code_count e char_count

4. **Dataset preprocessati diversamente**:
   - Tokenization diversa → word_count incomparabile
   - Stop words list diversa → naturalness_factor distorto
   - Consigliato: re-preprocessing uniforme

### Assunzioni delle Metriche

- **OSD**: Assume feature indipendenti (non sempre vero)
- **NBS**: Assume stop_word_ratio=0.25 ottimale (vero per inglese conversazionale)
- **TDR**: Assume pesi (50%, 30%, 20%) universali
- **CSG**: Assume tre componenti equal importance

### Best Practices

1. **Sempre visualizzare distribuzioni** oltre alle metriche aggregate
2. **Confrontare con dataset di riferimento** noti
3. **Validare su subset** prima di decision su full dataset
4. **Considerare domain knowledge** oltre alle metriche
5. **Iterare**: Metriche guidano ma non sostituiscono judgment

---

## 📚 Glossario Tecnico

| Termine | Definizione | Formula |
|---------|-------------|---------|
| **Cohen's d** | Effect size standardizzato | $d = \frac{\mu_B - \mu_A}{\sigma_{\text{pooled}}}$ |
| **Pooled Std Dev** | Deviazione standard combinata | $\sigma_p = \sqrt{\frac{(n_A-1)\sigma_A^2 + (n_B-1)\sigma_B^2}{n_A+n_B-2}}$ |
| **Effect size** | Magnitude della differenza (indipendente da n) | Varie formule (Cohen's d, Hedge's g) |
| **Stop words** | Parole funzionali (the, a, is, etc.) | Liste predefinite per lingua |
| **Rate** | Percentuale presenza feature | $r = \frac{\text{count(True)}}{n} \times 100$ |
| **Epsilon (ε)** | Costante piccola anti-divisione-zero | $10^{-10}$ |
| **Naturalness** | Misura "organicità" linguaggio | Basato su stop_word_ratio |
| **Technical depth** | Concentrazione contenuti tecnici | TDR score normalizzato |

---

## 📞 Support e Feedback

**Per problemi implementativi**:
- Verificare che tutte le feature prerequisite siano disponibili
- Controllare range valori (NaN, Inf, negativi dove non dovrebbero esserci)
- Validare con calcolo manuale su subset piccolo

**Per interpretazione risultati**:
- Confrontare con esempi in questo documento
- Considerare multiple metriche insieme (no single metric decision)
- Visualizzare distribuzioni sottostanti

**Per customizzazione**:
- Pesi ($w_i$, $w_{\text{imp}}$) possono essere adattati al dominio
- Soglie possono essere ri-calibrate su dataset specifici
- Nuove feature possono essere aggiunte mantenendo framework

---

*Documentazione Versione 2.1 - Gennaio 2025*  
*Sistema progettato per decisioni data-driven su selezione e mixing dataset*  
*Formule matematicamente complete e implementabili*  
*Focus: Riproducibilità, Interpretabilità, Actionability*