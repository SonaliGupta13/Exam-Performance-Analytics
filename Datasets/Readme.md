This project uses three primary datasets that together support exam performance analysis and AI-driven insights:

#### students_realworld_big.csv
Contains detailed student-level exam data including marks, attendance status, exam attempts, categories, cities, and exam dates. This is the core dataset used for performance evaluation, ranking, and KPI generation.

#### category_cutoff.csv
Stores category-wise cutoff criteria used to determine pass/fail status. This dataset is joined with student records to apply business rules consistently across categories.

#### feedback_big.csv
Includes qualitative feedback provided by students after the exam. This dataset is used for AI and NLP-based analysis to extract sentiment and identify improvement areas.
