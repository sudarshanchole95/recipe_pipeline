# Recipe Analytics Pipeline – Final Project Documentation

## 1. Overview
This project implements a complete end‑to‑end **Data Engineering pipeline** for a recipe application using:
- **Firestore** (NoSQL source)
- **Python ETL** (Extraction → Transformation → Normalization)
- **Analytics & Visualization**
- **Data Quality Validation**
- **Exported CSV datasets** for downstream analysis

The goal is to simulate a real production-grade data engineering workflow suitable for ThinkBridge assignment evaluations.

---

## 2. Data Model (Firestore → JSON → Normalized CSV)

### 2.1 Collections in Firestore
The raw data is stored in three collections:

### **A) recipes**
Each recipe document contains:
- `id` – unique slug + UUID  
- `title`  
- `author`  
- `cuisine`  
- `difficulty`  
- `occasion` (array)  
- `category_type`  
- `texture`  
- `served_temperature`  
- `tags` (array)  
- `nutrition_groups` (array)  
- `prep_time_min`, `cook_time_min`, `total_time_min`  
- `ingredients` (array of objects)  
- `steps` (array of objects)  

---

### **B) users**
Used to simulate real application interaction patterns.

 contains:
- `id`
- `username`
- `joined_at`

---

### **C) interactions**
Simulated behavioral actions for analytics:
- `view`
- `like`
- `rating`
- `attempt`

Each interaction contains:
- `id`
- `user_id`
- `recipe_id`
- `timestamp`
- `type`
- `metadata` (device, rating stars, etc.)

---

## 2.2 Normalized CSV Outputs
After ETL, the dataset is transformed into fully normalized relational tables:

### **1. recipe.csv**
| Column            | Description |
|--------           |-------------|
| id                | Unique recipe ID |
| title             | Recipe name |
| author            | Who created it |
| cuisine           | Indian/Italian/etc. |
| difficulty        | Easy/Medium/Hard |
| servings          | Number of people served |
| prep_time_min     | Minutes |
| cook_time_min     | Minutes |
| total_time_min    | Minutes |
| category_type     | Comfort Food / Main Course |
| texture           | Soft / Crispy / Creamy |
| served_temperature | Hot / Cold |
| occasion          | Stored as comma-separated string |
| tags              | Comma-separated string |
| nutrition_groups  | Carbs, Protein, etc. |

---

### **2. ingredients.csv**
| recipe_id | ingredient_name | quantity | unit |

Each ingredient becomes 1 row → supports SQL joins & analytics.

---

### **3. steps.csv**
| recipe_id | step_number | step_text |

Maintains the sequence of the cooking workflow.

---

### **4. interactions.csv**
| id | user_id | recipe_id | type | timestamp | metadata_json |

Dynamic values (e.g., rating vs. device info) are preserved using a JSON string.

---

## 3. ETL Pipeline Overview

### **Extraction**
- Firestore is exported into:
  - output/recipes.json  
  - output/users.json  
  - output/interactions.json  

### **Transformation (Python)**
- Flatten nested arrays
- Convert arrays → comma-separated strings
- Parse metadata_json into Python dict
- Normalize ingredients & steps into their own tables
- Convert Firestore timestamp → ISO format
- Fix inconsistent fields (strip whitespace, handle missing values)

### **Loading**
- Save final CSVs into:
```
output/etl/
  ├── recipe.csv
  ├── ingredients.csv
  ├── steps.csv
  └── interactions.csv
```

---

## 4. Running the Pipeline

### **1. Seed Firestore**
```
python seed_firestore.py
```

### **2. Export data**
```
python export_firestore.py
```

### **3. Run ETL**
```
python etl_pipeline.py
```

### **4. Validate data**
```
python validation.py
```

### **5. Run analytics**
```
python analytics.py
```

Charts will be generated in:
```
output/analytics/charts/
```

---

## 5. Analytics Summary (Highlights)

The analytics module calculates:

### 📌 **Derived Metrics**
- **Engagement Rate = likes / views**
- **ROI = avg_rating / total_time_min**
- **Complexity Score = total_time_min/10 + num_ingredients**

### 📌 **Visualizations**
- Top 15 Ingredients (bar chart)
- Word Cloud of ingredients *(if library installed)*
- Correlation Heatmap (views, likes, rating, time, complexity)
- Prep Time vs Rating (scatter)
- Steps Count vs Engagement
- Cuisine Popularity
- Difficulty Distribution
- Rating Histogram

### 📌 **Insights Example**
- Recipes with **fewer than 30 minutes** cooking time received **higher engagement**.
- **Indian cuisine** dominated total views.
- **High ROI dishes** (Rating per minute) included quick comfort dishes.
- Recipes with **more steps** showed **lower engagement rate** (user drop-off).
- Missing metadata categories can be enriched to improve segmentation.

---

## 6. Known Constraints / Limitations
- Data is synthetic; interaction patterns may not reflect real user behavior.
- Metadata quality varies (e.g., some recipes may not have full fields).
- Ratings are simulated (averages may not reflect real user sentiment).
- Firestore export is snapshot-based; not a real-time streaming pipeline.
- Some advanced analytics like predictive modeling are out of scope.

---

## 7. Deliverables Summary

### ✔ Source Scripts
- `seed_firestore.py`
- `export_firestore.py`
- `etl_pipeline.py`
- `validation.py`
- `analytics.py`

### ✔ Normalized CSV Output
Located in: `output/etl/`

### ✔ Analytics
- Markdown summary → `output/analytics/analytics_summary.md`
- Charts → `output/analytics/charts/`

### ✔ README.md
(Current file)

---

## 8. Architecture Diagram

```
       Firestore
           │
           ▼
   export_firestore.py
           │
           ▼
      JSON Files
           │
           ▼
    etl_pipeline.py
   (Flatten + Normalize)
           │
           ▼
     Normalized CSVs
           │
           ▼
    analytics.py (EDA)
           │
           ▼
 Charts + Summary Report
```

---


