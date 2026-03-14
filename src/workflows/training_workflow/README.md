


Add the following sections to the README to extend **Workflow Responsibilities** and **Tasks** with precise definitions aligned to the actual task modules.

---

# Workflow Responsibilities

The training workflow orchestrates the full model training lifecycle from dataset extraction to model registration. Each responsibility is implemented as an isolated **Flyte task**, enabling deterministic execution, retry isolation, and artifact lineage tracking.

Primary responsibilities:

1. **Dataset Extraction**

   Retrieve historical training data from the offline data store or data lake.

   * Read partitioned datasets.
   * Load feature columns and labels.
   * Construct a consistent dataset snapshot.

2. **Raw Data Validation**

   Validate structural integrity of the dataset before transformations.

   * Schema validation
   * Null detection
   * Duplicate primary key checks
   * Timestamp sanity checks
   * Partition completeness verification

3. **Distributed Feature Engineering**

   Transform raw datasets into model-ready features using distributed data processing.

   * Feature joins
   * Aggregations and rolling windows
   * Feature normalization
   * Categorical encoding
   * Label construction

4. **Feature Validation**

   Ensure feature correctness after preprocessing.

   * Value range validation
   * Distribution sanity checks
   * Domain validation for categorical features
   * Label balance checks

5. **Model Training and Evaluation**

   Train candidate models and evaluate their performance.

   * Hyperparameter optimization
   * Model training
   * Validation metric computation
   * Model selection decision

6. **Artifact Export**

   Convert trained models into deployable inference artifacts.

   * Export trained model to ONNX
   * Persist artifacts to object storage

7. **Model Registration**

   Register model artifacts and metadata in the model registry.

   * Create MLflow model version
   * Store evaluation metrics
   * Attach training metadata

---

# Tasks

Each task represents a deterministic unit of computation within the workflow DAG.

Tasks operate on explicit inputs and outputs to ensure reproducibility.

---

## extract_load_data

**Module**

```
tasks/extract_load_data.py
```

**Purpose**

Load historical data required for model training.

**Responsibilities**

* Read partitions from the offline dataset.
* Select required feature columns.
* Load label columns.
* Construct the raw dataset snapshot.

**Inputs**

* dataset time range
* dataset identifiers
* feature schema version

**Outputs**

* raw dataset reference
* dataset metadata

---

## raw_data_validation

**Module**

```
tasks/raw_data_validation.py
```

**Purpose**

Verify dataset integrity before transformations.

**Responsibilities**

Execute data validation checks including:

* schema validation
* null detection
* duplicate key checks
* timestamp validation
* partition completeness

**Implementation**

Validation is executed using **Great Expectations**.

**Failure Behavior**

If validation fails, the workflow terminates and the failure is recorded in Flyte execution logs.

**Outputs**

* validation report
* validated dataset reference

---

## ray_data_preprocessing

**Module**

```
tasks/ray_data_preprocessing.py
```

**Purpose**

Perform distributed dataset transformation and feature engineering.

**Responsibilities**

* feature joins
* rolling aggregations
* normalization
* categorical encoding
* label construction
* dataset splitting

**Implementation**

Distributed processing is executed using **Ray Data**.

**Outputs**

* engineered dataset
* training split
* validation split

---

## feature_validation

**Module**

```
tasks/feature_validation.py
```

**Purpose**

Ensure correctness of engineered features before training.

**Responsibilities**

Validation checks include:

* feature range validation
* categorical domain verification
* label distribution checks
* statistical sanity validation

**Implementation**

Validation is implemented using **Great Expectations** combined with statistical checks.

**Failure Behavior**

If validation fails, training does not proceed.

**Outputs**

* validated feature dataset
* feature validation report

---

## train_and_evaluate_model

**Module**

```
tasks/train_and_evaluate_model.py
```

**Purpose**

Train candidate models and evaluate their performance.

**Responsibilities**

* perform hyperparameter search
* train models on training dataset
* evaluate on validation dataset
* compare performance against baseline

**Implementation**

Model training stack:

* **FLAML** – hyperparameter optimization
* **LightGBM** – gradient boosting model

**Evaluation Metrics**

Typical metrics include:

* AUC
* Accuracy
* Precision / Recall
* RMSE

**Outputs**

* trained model artifact
* evaluation metrics
* promotion decision flag

---

## export_and_register_model

**Module**

```
tasks/export_and_register_model.py
```

**Purpose**

Produce deployable model artifacts and register them.

**Responsibilities**

* convert trained model to ONNX
* upload artifact to object storage
* register model version in MLflow
* attach training metadata

**Implementation**

Artifacts are exported to **ONNX format** to enable runtime compatibility with inference services.

**Outputs**

* ONNX model artifact URI
* MLflow model version
* training run metadata

---

Pro tip: maintain strict task boundaries so each Flyte task performs **one atomic responsibility**. This keeps the workflow DAG stable and prevents cache invalidation from propagating across unrelated pipeline stages.

Confidence: 97%

