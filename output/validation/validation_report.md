# Validation Report

**Run:** 2025-11-25T10:27:24.161871
**Total recipes processed:** 23
**Total issues found:** 6360

## Issues by Severity

- 🔥 **Critical** — `orphan_interactions` : 6356 issues
  - Examples: `[{"id": "int-004652dc48", "user_id": "sofia_romano", "recipe_id": "kheer-12334d", "type": "view", "timestamp": "2025-10-06T04:11:45.367874+00:00", "metadata_json": "{\"device\": \"desktop\"}"}, {"id": "int-0048e7966b", "user_id": "ishita_pandey_cb13", "recipe_id": "pasta-alfredo-7764e3", "type": "like", "timestamp": "2025-09-11T04:11:39.385834+00:00", "metadata_json": "{}"}, {"id": "int-00662c48fd", "user_id": "noodleninja", "recipe_id": "aloo-paratha-6d7a6c", "type": "like", "timestamp": "2025-08-20T04:11:31.011183+00:00", "metadata_json": "{}"}]`
- ⚠️ **High** — `negative_time_values` : 0 issues
- 🟡 **Medium** — `invalid_steps` : 4 issues
  - Examples: `[{"recipe_id": "test-paneer-9593", "step_number": NaN, "step_text": "Heat oil in a pan."}, {"recipe_id": "test-paneer-9593", "step_number": NaN, "step_text": "Add tomatoes, cook and add paneer."}, {"recipe_id": "test-soup-6973", "step_number": NaN, "step_text": "Chop vegetables."}]`
- 🟢 **Low** — `missing_recipe_columns` : 0 issues
- 🟢 **Low** — `invalid_ingredients` : 0 issues
- 🟢 **Low** — `duplicate_recipe_ids` : 0 issues
- 🟢 **Low** — `invalid_difficulty_values` : 0 issues
