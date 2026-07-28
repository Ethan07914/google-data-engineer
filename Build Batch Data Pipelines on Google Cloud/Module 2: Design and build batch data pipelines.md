# Module 2: Design and build batch data pipelines

## Large scale data transformations

### Google Cloud data processing engines

- **Dataflow**: unified programming model (Apache Beam) for batch and streaming, defines pipelines as transformation DAGs.
- **Dataflow when**: scalable cloud-native pipelines, ideal for complex ETL, new robust data solutions and real-time streaming.
- **Serverless for Apache Spark**: managed environment for Apache Spark.
- **Spark when**: migrating existing Spark/Hadoop workloads, strong for data science/ML workflows relying on Spark libraries, or existing team expertise around Spark.

### Principles

- **Immutability**: once data records are created they should not be changed.
- **Idempotency**: repeated execution of a transformation yields the same result.
- **Schema enforcement**: data must conform with a predefined structure.

## Dataflow

- PCollections and Transforms are used to implement the above principles.
- **ParDo**: general purpose parallel processing transform that applies a user-defined function (UDF) to each element in a PCollection, producing zero or many elements.
- ParDo is ideal for data cleansing (filtering, parsing complex fields and type conversion) or enrichment (adding calculated fields).
- **GroupByKey**: groups elements in a key-value PCollection by their key, essential for preparing data for aggregation by bringing related records together on a single worker or set of workers.
- **Combine**: aggregates values in a PCollection, used after GroupByKey (as Combine.PerKey) to compute sums, averages, counts or custom aggregations for a group, or globally (as Combine.Globally) across an entire PCollection.
- **CoGroupByKey**: performs a multi-input join, grouping elements from two or more key-value PCollections by their common keys.
- CoGroupByKey is used in scenarios that require more complex data integration, where you need to combine related data from disparate sources.
- Implement Dataflow transformations via the Job Builder UI and templates (encapsulate underlying Beam Transforms).
- You can define input/output paths, specify filtering rules or map data fields directly in the UI.
- You can also write custom Beam pipeline code and deploy it as a custom Dataflow template.

![dataflow_join.png](dataflow_join.png)

### Apache Beam

- Batch or streaming pipelines with the language of your choice.
- PCollection (Parallel Collections) is an immutable unordered collection of values.
- PCollection is a distributed dataset.
- PTransform is an operation in a pipeline performed to one or more PCollections.
- PTransforms take PCollections as input and output PCollections allowing for them to be chained.
- Pipeline is a Directed Acyclic Graph of data transformations, the entire workflow.
- Windows group elements by timestamp (fixed, sliding, session).

### Dataflow design principles

- **Distributed processing**: tasks are distributed across multiple worker nodes.
- **Optimal batch sizing**: determine the right volume of data to process in a single Dataflow job to balance latency and efficiency.
- **Data partitioning**: consider how data is partitioned to minimize shuffling across the network.
- **I/O optimization**: choose efficient file formats (Avro, Parquet) when dealing with Cloud Storage to reduce read/write times and reduce storage cost.
- **Native connectors**: utilise native connectors (TextIO, BigQueryIO).
- **Resource tuning**: design to allow for appropriate worker machine types and autoscaling settings, enough compute resources without over-provisioning.
- **Schema enforcement and evolution**: plan for defining and enforcing data schemas, consider how you will handle changes to input schemas over time without breaking the pipeline.
- **Error handling and dead letter queues**: implement robust mechanisms to catch and route erroneous records to a DLQ (backup queue for data that cannot be processed correctly) so that the pipeline can continue to process valid data.
- Break down transformations into smaller, more manageable steps, increasing reusability and maintainability.

![distributed_processing.png](distributed_processing.png)

**DLQ**

![DLQ.png](DLQ.png)

## Transforms with Serverless for Apache Spark

- Define transformations with PySpark (DataFrame API) or Spark SQL.
- Data cleansing includes `filter`, `dropDuplicates`, `withColumn` (add or modify a column), and `na.fill` (handle missing values).
- Data enrichment is done via join operations between DataFrames and aggregations using `groupBy`.
- Custom code in languages like Python can be written, or Dataproc templates can be used.
- Instead of writing transformation logic directly for each job, you design parameterized Spark applications that are submitted as templates.
- In these templates you specify input/output, transformation rules, and aggregation keys.
- Templates promote reusability and operational efficiency.
- Handles infrastructure so you can focus on code; automatically scales compute resources depending on job needs.

```python
df1.join(df2, on="key", how="inner")
```

## Apache Spark concepts

- **DataFrames**: Structured as rows and columns.
- **Dataset**: DataFrame structure with type safety.
- **Transforms**: create a new abstraction from an existing one, lazy transformations only executed when an action is called.
- **Action**: trigger execution of the transformations and return the result.
- **Driver program**: runs the main() function and creates the SparkContext, coordinates distributed operations on the cluster.
- **Executors**: worker processes that run tasks and store data.
- **Tasks**: individual units of work performed by executors on partitions of data.
- **Stages**: physical execution plan broken into sets of tasks that can run in parallel, a shuffle operation typically marks the boundary between stages.
- To ensure scalability, Serverless for Apache Spark utilises distributed processing by automatically managing and provisioning Spark clusters.
- You should also partition your data in Cloud Storage or BigQuery.

### Reusability

- Dataproc templates can be shared between colleagues.
- Parameterize applications rather than hardcoding values.
- Make Spark applications modular, break down into smaller self-contained units rather than one massive script.


