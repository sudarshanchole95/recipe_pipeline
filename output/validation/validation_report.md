# Validation Report

**Run:** 2025-11-24T14:00:48.414324
**Total recipes processed:** 65
**Total issues found:** 4

## Issues by Severity

- ⚠️ **High** — `negative_time_values` : 0 issues
- 🟡 **Medium** — `invalid_steps` : 4 issues
  - Examples: `[{"recipe_id": "test-paneer-9593", "step_number": NaN, "step_text": "Heat oil in a pan."}, {"recipe_id": "test-paneer-9593", "step_number": NaN, "step_text": "Add tomatoes, cook and add paneer."}, {"recipe_id": "test-soup-6973", "step_number": NaN, "step_text": "Chop vegetables."}]`
- 🟢 **Low** — `missing_recipe_columns` : 0 issues
- 🟢 **Low** — `invalid_ingredients` : 0 issues
- 🟢 **Low** — `orphan_interactions` : 0 issues
- 🟢 **Low** — `duplicate_recipe_ids` : 0 issues
- 🟢 **Low** — `invalid_difficulty_values` : 0 issues
