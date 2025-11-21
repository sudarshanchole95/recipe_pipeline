# Firebase Recipe Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)
![Firebase](https://img.shields.io/badge/Firebase-Firestore-orange?logo=firebase)
![ETL](https://img.shields.io/badge/Pipeline-ETL-green)
![Status](https://img.shields.io/badge/Status-Production_Ready-success)

---

## 1. Project Overview

This project implements a fully automated, end-to-end **Data Engineering Pipeline** that extracts recipe, user, and interaction data from **Google Firestore**, cleans and normalizes it, validates data quality, and generates analytics-ready insights and visualizations.

The pipeline simulates a real-world production data system with:

- NoSQL → relational normalization  
- Data quality validation and quarantining  
- Automated orchestration via a single command  
- Insight generation with visual analytics  

**Seed Data:** Includes one custom, real recipe (user-provided) as the foundational dataset, along with synthetic supporting data.

---

## 2.Data Model Architecture

Firestore stores semi-structured JSON documents. During ETL, this data is normalized into a clean relational model.

### 🔹 Recipe Model

Each recipe contains:

- Unique ID  
- Title & description  
- Cuisine & difficulty  
- Prep/Cook/Total times  
- Ingredients (array of objects)  
- Steps (array of strings)  
- Metadata: created_by, timestamps  

Ingredients example:

```json
{
  "name": "Onion",
  "quantity": 2,
  "unit": "pcs"
}
```

Steps stored as an ordered list.

---

### 🔹 Users Model

User documents store:

- user_id  
- display name  
- age  
- food preferences  
- account creation timestamp  

---

### 🔹 Interactions Model

Captures user engagement:

- interaction_id  
- user_id  
- recipe_id  
- type: view / like / rating / attempt  
- rating (optional)  
- timestamp  

---

## Entity Relationship Diagram (ERD)

```mermaid
erDiagram
    USERS ||--o{ INTERACTIONS : performs
    RECIPES ||--o{ INTERACTIONS : receives
    RECIPES ||--o{ INGREDIENTS : contains
    RECIPES ||--o{ STEPS : includes

    USERS {
        string id PK
        string display_name
        string city
        string food_preferences
    }

    RECIPES {
        string id PK
        string title
        string cuisine
        string difficulty
        number prep_time_min
        number cook_time_min
        number total_time_min
        string tags
    }

    INGREDIENTS {
        string recipe_id FK
        string ingredient_name
        string quantity
        string unit
    }

    STEPS {
        string recipe_id FK
        number step_number
        string step_text
    }

    INTERACTIONS {
        string id PK
        string recipe_id FK
        string user_id FK
        string type
        string metadata_json
        timestamp timestamp
    }
```

---

## 3. Firebase Setup & Data Seeding

### Firestore Collections

- `recipes`
- `users`
- `interactions`

### Seeder Script Generates:

- 1 real recipe  
- 20 synthetic recipes  
- 10 synthetic users  
- 300–400 interactions  

This creates a realistic dataset for analytics.

---

## 4. ETL Pipeline Overview

### Pipeline Diagram

```mermaid
flowchart LR
    A[Firestore Source] --> B[Extract JSON]

    B --> C[Normalize via ETL]

    subgraph Transformation Logic
    C --> C1[Explode Arrays]
    C --> C2[Type Casting]
    C --> C3[Deduplication]
    C --> C4[Schema Enforcement]
    end

    C --> D[Validated CSV Outputs]
    C --> E[Quarantine Bad Data]
```

### Extraction

Firestore → JSON:

```
export/
├── recipes.json
├── users.json
└── interactions.json
```

### Transformation

ETL normalizes documents into:

```
output/etl/
├── recipe.csv
├── ingredients.csv
├── steps.csv
└── interactions.csv
```

### Quarantine System

Invalid records stored at:

```
output/bad_data/
```

---

## 5. Data Quality & Validation

Validation rules include:

| Rule Type | Description |
|----------|-------------|
| Completeness | Required fields must exist |
| Numeric Validity | Times must be ≥ 0 |
| Difficulty Domain | Only Easy / Medium / Hard |
| Referential Integrity | Valid recipe_id references |
| Structural Integrity | Steps & ingredients must be non-empty |
| Duplicate Detection | Duplicate IDs are quarantined |

Outputs:

```
output/validation/
├── validation_report.md
└── validation_results.json
```

---

## 6. Analytics & Insights

The analytics module (`analytics.py`) generates **15 strategic, consultant-grade visualizations** designed to reveal user behavior, recipe performance, operational risks, and business opportunities.

Charts cover **four strategic categories**:

---

### 🧠 Category 1 — User Psychology & Behavior

1. **Instagram Trap** — high views ≠ conversions  
2. **Step Fatigue** — drop-off after ~15 steps  
3. **30-Minute Cliff** — 2× engagement for fast recipes  
4. **Effort vs Reward** — complexity vs rating  

---

### 💰 Category 2 — Content Strategy & ROI

5. **ROI Landscape** — rating / minute  
6. **Skill Gap** — variance by difficulty  
7. **Cuisine Conversion Power**  
8. **Batch Cooking Demand**  

---

### 📦 Category 3 — Supply Chain & Operations

9. **Critical Ingredients**  
10. **Ingredient Barrier**  
11. **Inventory Efficiency Risk**  

---

### 🚀 Category 4 — App Growth & Monetization

12. **Onboarding Heroes**  
13. **Viral Vectors**  
14. **Prep vs Cook Time Distribution**  
15. **Correlation Matrix**  

---

### 📄 Analytics Report Generation

A full report is auto-generated:

```
output/analytics/analytics_summary.md
```

Includes:

- Strategic insights  
- Chart explanations  
- Timestamped metadata  

---

### 📁 Analytics Outputs

```
output/analytics/charts/
```

Examples:

```
1_instagram_trap.png
2_step_fatigue.png
5_roi_landscape.png
10_ingredient_barrier.png
15_correlation_matrix.png
```




## 7. Orchestration System

The entire workflow is automated using a single orchestrator:

```
python pipeline.py
```

Pipeline stages triggered:

1. Export Firestore  
2. ETL normalization  
3. Validation  
4. Analytics  

A complete manifest is stored:

```
output/logs/
```

This ensures reproducibility and traceability—similar to real-world Airflow or Prefect workflows, but implemented in pure Python.

---

## 8. Setup & Execution Instructions

### Install Requirements

```
pip install -r requirements.txt
```

### Add Firebase Credentials

Place Firestore key:

```
serviceAccountKey.json
```

### Run Entire Pipeline

```
python pipeline.py
```

### View Outputs

```
output/
    ├── etl/
    ├── validation/
    ├── analytics/
    └── bad_data/
```

---

## 9. Directory Structure

```
project/
├── pipeline.py
├── export/
├── output/
│   ├── etl/
│   ├── analytics/
│   ├── validation/
│   └── bad_data/
├── serviceAccountKey.json
├── requirements.txt
└── README.md
```

---

## 10. Known Limitations

- Synthetic recipe data may not reflect real-world distributions.
- Pandas-based ETL is optimized for local scale.
- Firestore export is full-dump (not incremental).
- Orchestration is Python-based, not Airflow/Prefect.

---

## Final Statement

This project demonstrates a strong production-style data engineering workflow with:

- Automated orchestration  
- Reliable validation  
- Scalable transformation  
- Insightful analytics  

A complete end-to-end Firestore → ETL → Validation → Analytics pipeline.

