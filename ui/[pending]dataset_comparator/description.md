# 📊 Dataset Comparator - Documentazione Metriche

*Versione 2.0 - Sistema di Analisi Differenziale per Dataset*

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

---

## 📋 Metriche Differenziali Principali

### 1. Overall Structure Distance (OSD)

**Scopo**: Misurare quanto due dataset differiscono **globalmente** in tutte le caratteristiche numeriche.

**Cosa risponde**: "Quanto sono strutturalmente diversi questi dataset?"

**Formula**:

$$
\text{OSD}_{\text{base}} = \sqrt{\sum_i w_i \cdot \left(\frac{\mu_{B,i} - \mu_{A,i}}{\sigma_{\text{pooled},i}}\right)^2}
$$

$$
\text{outlier\_factor} = \sum_i \log_2\left(1 + \frac{|\max_{B,i} - \max_{A,i}|}{\max_{A,i} + \epsilon}\right) \cdot w_{\text{outlier},i}
$$

$$
\text{OSD}_{\text{final}} = \text{OSD}_{\text{base}} \cdot (1 + 0.3 \cdot \text{outlier\_factor})
$$

**Soglie Interpretative**:

| Valore | Significato Pratico | Use Case Tipico |
|--------|---------------------|-----------------|
| **0.0 - 0.3** | Dataset **molto simili** | Ridondanti per training mix |
| **0.3 - 0.6** | **Moderatamente diversi** | Complementari per mix |
| **0.6 - 0.9** | **Significativamente diversi** | Caratteristiche distintive marcate |
| **> 0.9** | **Radicalmente diversi** | Potenzialmente ortogonali |

**Componente Outlier**:

- `outlier_factor > 0.5`: Dataset hanno **valori estremi discordanti**
- `outlier_factor > 1.0`: Differenze estreme in singoli documenti

**Esempio Decisionale**:

- **OSD < 0.3**: Dataset probabilmente ridondanti - sceglierne solo uno
- **OSD 0.3-0.6**: Buona complementarietà per mixing
- **OSD > 0.8**: Dataset specializzati in aree diverse - utile per ensemble

---

### 2. Content Richness Divergence (CRD)

**Scopo**: Analizzare **differenze nella distribuzione di contenuti strutturati**.

**Cosa risponde**: "In quali tipi di contenuti strutturati differiscono?"

**Metriche**:

$$
\text{CRA}_{\text{net}} = 100 \cdot \frac{\sum_i (p_{B,i} - p_{A,i}) \cdot \text{importance}_i}{\sum_i \text{importance}_i}
$$

$$
\text{abs\_div} = 100 \cdot \frac{\sum_i |p_{B,i} - p_{A,i}| \cdot \text{importance}_i}{\sum_i \text{importance}_i}
$$

**Interpretazione Combinata**:

| CRA_net | Absolute Divergence | Pattern Identificato | Implicazione Mixing |
|---------|---------------------|----------------------|---------------------|
| **> +20%** | **< 20%** | B consistentemente più ricco in TUTTI i contenuti | Selezionare B per contenuti strutturati |
| **< -20%** | **< 20%** | A consistentemente più ricco in TUTTI i contenuti | Selezionare A per contenuti strutturati |
| **±0-10%** | **> 40%** | **Specializzazione opposta**: A↑ in X, B↑ in Y | IDEALE per mixing - coprono aree diverse |
| **> +15%** | **30-40%** | B generalmente più ricco ma con trade-offs | Mix con predominanza B |

**Feature Analizzate**:

- `_assistant_has_json`, `_assistant_has_code`
- `_assistant_has_markdown`, `_assistant_has_table`
- `_code_count > 0`, `_math_count > 0`

**Esempio Decisionale**:

- `abs_div > 40%`: Dataset complementari - buoni per mixing
- `CRA_net > +20%`: Dataset B specializzato in contenuti strutturati
- `CRA_net ≈ 0`, `abs_div > 50%`: Specializzazione opposta - ottimi per ensemble

---

### 3. Role Balance Difference (RBD)

**Scopo**: Quantificare differenze nell'**equilibrio conversazionale** tra ruoli.

**Cosa risponde**: "Quanto differisce la dinamica user/assistant tra i dataset?"

**Formula Avanzata**:

$$
\text{ratio\_log2\_diff} = \left|\log_2(\text{UA\_ratio}_B + \epsilon) - \log_2(\text{UA\_ratio}_A + \epsilon)\right|
$$

Dove:

$$
\text{UA\_ratio} = \frac{\text{user\_char\_count}}{\text{assistant\_char\_count}}
$$

**Interpretazione Log2 (Umana)**:

| ratio_log2_diff | Significato Conversazionale | Impatto Training |
|-----------------|-----------------------------|------------------|
| **0.0 - 0.3** | Equilibrio **praticamente identico** | Training consistente |
| **0.3 - 0.7** | **Leggermente diverso** | Leggera variazione stile |
| **0.7 - 1.2** | **Moderatamente diverso** (≈2x differenza) | Stili conversazionali distinti |
| **> 1.2** | **Molto diverso** (>2.5x differenza) | Dinamiche radicalmente diverse |

**Esempi Concreti**:

- `ratio_log2_diff = 1.0`: "Risposte il doppio più lunghe (o corte) in un dataset"
- `ratio_log2_diff = 2.0`: "Risposte 4 volte più lunghe (o corte)"
- `ratio_log2_diff = 0.5`: "Risposte ≈1.4x più lunghe"

**Use Case Decisionale**:

- Per task specifici: selezionare dataset con equilibrio appropriato
- Per modello generale: mixing di dataset con diverse dinamiche
- Per debugging: identificare shift nelle dinamiche conversazionali

---

### 4. Technical Depth Ratio (TDR)

**Scopo**: Misurare **differenze nella profondità tecnica**.

**Cosa risponde**: "Quanto è più tecnico un dataset rispetto all'altro?"

**Formula**:

$$
\text{TDR} = \frac{\text{code\_density}_B + \text{json\_rate}_B + \text{curly\_norm}_B}{\text{code\_density}_A + \text{json\_rate}_A + \text{curly\_norm}_A}
$$

**Interpretazione**:

| TDR | Differenza Tecnica | Use Case Appropriato |
|-----|-------------------|----------------------|
| **0.7 - 1.3** | Profondità tecnica **simile** | Training uniforme |
| **1.3 - 2.0** | B **moderatamente più tecnico** | Task tecnici-specifici |
| **> 2.0** | B **significativamente più tecnico** | Specializzazione tecnica |
| **< 0.5** | A **molto più tecnico** | Specializzazione opposta |

**Componenti**:

- `code_density = code_count / word_count`
- `json_rate = % documenti con JSON`
- `curly_norm = curly_brackets_count / char_count`

**Decisioni Supportate**:

- Selezionare dataset appropriato per task tecnici vs. generici
- Bilanciare mix per copertura di diverse complessità tecniche
- Identificare dataset per fine-tuning specializzato

---

### 5. Naturalness Balance Score (NBS)

**Scopo**: Valutare differenze nella **naturalità linguistica**.

**Cosa risponde**: "Quanto differisce la 'naturalità' del linguaggio?"

**Formula**:

$$
\text{naturalness\_factor}_X = \exp\left(-4 \cdot |\text{stop\_word\_ratio}_X - 0.25|\right)
$$

$$
\text{NBS} = \frac{\text{naturalness\_factor}_B}{\text{naturalness\_factor}_A}
$$

**Interpretazione Naturalness Factor**:

| stop_word_ratio | Naturalness Factor | Caratteristica Dataset |
|-----------------|-------------------|------------------------|
| **0.20 - 0.30** | 0.82 - 1.00 | Linguaggio **naturale** |
| **0.15 - 0.20** | 0.67 - 0.82 | **Leggermente innaturale** |
| **0.10 - 0.15** | 0.55 - 0.67 | **Moderatamente innaturale** |
| **< 0.10** | < 0.45 | Probabilmente **sintetico/pulito eccessivo** |

**Use Case Critico**:

- Identificare dataset sintetici vs. organici
- Bilanciare mixing per mantenere naturalità linguistica
- Rilevare over-cleaning nei preprocessing

---

### 6. Conversation Style Gap (CSG)

**Scopo**: Misurare differenze nello **stile conversazionale avanzato**.

**Cosa risponde**: "Quanto differiscono nelle dinamiche avanzate (think/function/context)?"

**Formula**:

$$
\text{CSG} = \sqrt{\text{think\_intensity\_diff}^2 + \text{fc\_freq\_diff}^2 + \text{context\_usage\_diff}^2}
$$

**Interpretazione**:

| CSG | Differenza Stile | Implicazione |
|-----|-----------------|--------------|
| **0.0 - 0.2** | Stili **simili** | Consistenza conversazionale |
| **0.2 - 0.4** | **Leggermente diversi** | Variazioni minime |
| **0.4 - 0.6** | **Moderatamente diversi** | Approcci complementari |
| **> 0.6** | **Radicalmente diversi** | Filosofie conversazionali diverse |

**Componenti**:

- `think_intensity = think_chars / assistant_chars`
- `fc_freq = functioncall_messages / total_messages`
- `context_usage = context_messages / total_messages`

**Decisioni Supportate**:

- Mixing di dataset con stili complementari
- Selezione per modelli specializzati in specifiche dinamiche
- Identificare dataset per training su specifiche feature (es: ragionamento chain-of-thought)

---

### 7. Format Diversity Distance (FDD)

**Scopo**: Misurare differenze nella **varietà di formattazione**.

**Cosa risponde**: "Quanto differiscono nell'uso di formati testuali?"

**Formula**:

$$
\text{FDD} = \text{mean}\left(|\text{markdown\_diff}|, |\text{table\_diff}|, |\text{bulletpoint\_diff}|, |\text{ellipsis\_diff}|\right)
$$

**Interpretazione**:

| FDD | Differenza Formati | Impatto Processing |
|-----|-------------------|-------------------|
| **0.0 - 0.1** | Formati **molto simili** | Pipeline consistente |
| **0.1 - 0.2** | **Leggermente diversi** | Minimi adattamenti |
| **0.2 - 0.3** | **Moderatamente diversi** | Processing differenziato |
| **> 0.3** | **Molto diversi** | Pipeline separate consigliate |

**Use Case Pratico**:

- Determinare se dataset possono usare stessa pipeline di parsing
- Identificare dataset specializzati in specifici formati
- Bilanciare mix per copertura formati diversi

---

## 🎯 Metriche Semplici per Quick Analysis

### Differenze Percentuali Dirette

Per ogni statistica chiave:

$$
\text{diff\%} = 100 \cdot \frac{\mu_B - \mu_A}{\mu_A}
$$

**Statistiche Monitorate**:

- `_word_count`, `_char_count`: Dimensioni testuali
- `_total_messages`: Lunghezza conversazioni
- `_user_char_count`, `_assistant_char_count`: Distribuzione ruoli
- `_mean_word_length`: Complessità lessicale
- `_unique_word_ratio`: Diversità vocabolario

**Soglie Pratiche**:

- **< ±10%**: Differenze minime
- **±10-30%**: Differenze moderate
- **> ±30%**: Differenze significative

### Presence Ratios

Per feature binarie:

$$
\text{ratio} = \frac{\text{rate}_B}{\text{rate}_A}
$$

**Feature Critiche**:

- `_has_system`, `_has_think`, `_has_functioncall`
- `_assistant_has_json`, `_assistant_has_code`

**Interpretazione**:

- **0.8-1.2**: Presenza simile
- **1.2-2.0**: Moderatamente più presente
- **> 2.0**: Significativamente più presente

---

## 📊 Matrice Decisionale per Use Case Comuni

### 1. Selezione Dataset per Mix Ottimale

```python
if OSD < 0.3:  # Troppo simili
    → Scegliere solo uno (evitare ridondanza)
elif CRD_absolute_divergence > 40%:  # Complementari
    → IDEALE per mixing
elif TDR > 1.5 or TDR < 0.67:  # Specializzazione tecnica
    → Includere per copertura
```

### 2. Identificazione Dataset Specializzati

```python
if TDR > 2.0:  # Altamente tecnico
    → Specializzato in task tecnici
elif CRD_CRA_net > +20%:  # Ricco in contenuti strutturati
    → Specializzato in output strutturato
elif RBD_ratio_log2_diff > 1.0:  # Dinamiche distintive
    → Stile conversazionale unico
```

### 3. Validazione Complementarietà per Ensemble

Metriche desiderabili per ensemble:

- **OSD**: 0.4-0.7 (diversi ma non incompatibili)
- **CRD_absolute_divergence**: > 30% (copertura diverse aree)
- **CSG**: 0.3-0.6 (stili complementari)
- **FDD**: > 0.2 (formati diversi)

### 4. Rilevamento Dataset Sintetici

Segnali di dataset sintetico:

- **NBS_naturalness_factor** < 0.5
- **stop_word_ratio** < 0.10
- **outlier_factor** molto basso (distribuzione troppo uniforme)

### 5. Monitoraggio Drift nel Tempo

Allarmi per drift significativo:

- **OSD** > 0.6 tra batch temporali consecutivi
- **RBD_ratio_log2_diff** > 0.5 shift
- **CRD_absolute_divergence** > 30% change

---

## 🔧 Workflow Operativo Raccomandato

### Fase 1: Screening Iniziale (2 minuti)

1. Calcolare **OSD** e **CRD_absolute_divergence**
2. Decisione rapida:
   - OSD < 0.3: Dataset simili → procedi a analisi fine
   - OSD > 0.8: Molto diversi → valuta complementarietà
   - CRD_absolute_divergence > 40%: Forte complementarietà

### Fase 2: Analisi Dettaglio (5 minuti)

1. Esaminare **TDR** per differenze tecniche
2. Valutare **RBD** per equilibrio conversazionale
3. Controllare **NBS** per naturalità linguistica
4. Analizzare **CSG** per stili conversazionali

### Fase 3: Decisione Finale (3 minuti)

**Scenario A: Mixing per modello generale**

→ Cercare: OSD 0.4-0.7, CRD_abs_div > 30%, NBS > 0.7

**Scenario B: Selezione per task specifico**

→ Cercare: TDR appropriato, RBD appropriato, CRD_CRA_net rilevante

**Scenario C: Ensemble di specializzati**

→ Cercare: OSD > 0.6, CRD_abs_div > 40%, CSG > 0.4

---

## 📈 Esempi di Report Pratici

### Report 1: Due Dataset Complementari

```
DATASET COMPARISON - COMPLEMENTARIETÀ ALTA
===========================================
Overall Structure Distance: 0.52 (Moderatamente diversi)
Content Richness: Divergenza 45% (Specializzazione opposta)
Technical Depth: TDR=1.8 (B più tecnico)
Role Balance: ratio_log2_diff=0.6 (Leggermente diverso)
Naturalness: NBS=0.9 (Naturalità simile)

DECISIONE: Ottimi per mixing - coprono aree diverse
```

### Report 2: Dataset Simili (Ridondanti)

```
DATASET COMPARISON - ALTA SIMILARITÀ
=====================================
Overall Structure Distance: 0.18 (Molto simili)
Content Richness: Divergenza 12% (Contenuti simili)
Technical Depth: TDR=1.1 (Profondità tecnica simile)
Role Balance: ratio_log2_diff=0.2 (Equilibrio simile)

DECISIONE: Ridondanti - scegliere solo uno per training mix
```

### Report 3: Dataset Radicalmente Diversi

```
DATASET COMPARISON - DIFFERENZE ESTREME
========================================
Overall Structure Distance: 0.94 (Radicalmente diversi)
Content Richness: Divergenza 68% (Specializzazione opposta)
Technical Depth: TDR=3.2 (B molto più tecnico)
Role Balance: ratio_log2_diff=1.8 (Dinamiche molto diverse)

DECISIONE: Utile per ensemble di modelli specializzati
```

---

## 🎯 Sintesi: Cosa Guardare per...

### Se vuoi mixing bilanciato

- **OSD**: 0.4-0.7
- **CRD_absolute_divergence**: > 30%
- **NBS**: > 0.7
- **CSG**: 0.3-0.6

### Se vuoi dataset per task specifico

- **TDR** appropriato al task
- **CRD_CRA_net** rilevante per il task
- **RBD** appropriato alle dinamiche richieste

### Se vuoi evitare ridondanza

- **OSD** < 0.3 → ⚠️ warning
- **CRD_absolute_divergence** < 20% → ⚠️ warning
- **TDR** 0.8-1.2 → ⚠️ warning

### Se vuoi ensemble di specializzati

- **OSD** > 0.6 tra tutti i pair
- **CRD_absolute_divergence** > 40% per coppie
- **CSG** > 0.4 per differenze di stile

---

## ⚡ Quick Reference - Soglie Chiave

| Decisione | OSD | CRD_abs_div | TDR | RBD_log2 |
|-----------|-----|-------------|-----|----------|
| **Mix ottimale** | 0.4-0.7 | > 30% | 0.7-1.5 | 0.3-0.8 |
| **Troppo simili** | < 0.3 | < 20% | 0.9-1.1 | < 0.3 |
| **Troppo diversi** | > 0.8 | > 60% | < 0.5 or > 2.0 | > 1.2 |
| **Complementari** | 0.5-0.8 | 40-60% | Qualsiasi | Qualsiasi |

---

*Documentazione per uso operativo - Gennaio 2026*  
*Sistema progettato per supportare decisioni di selezione e mixing dataset*  
*Metriche neutre - Focus su differenziali oggettivi*