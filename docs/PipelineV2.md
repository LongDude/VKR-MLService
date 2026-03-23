# Pipeline V2

`PipelineManager` запускает цепочку этапов:

1. `ingest`
2. `acquire`
3. `extract`
4. `keywords`
5. `publish`

Конфигурация: `core/pipeline.yaml`.

## Ingress record (`pipeline_ingress_v2`)

Минимальный payload:

```json
{
  "source_type": "hot_arxiv",
  "source_ref": "pdf-raw/2401.01234.tar.gz",
  "article_id": "2401.01234",
  "metadata": {
    "id": "2401.01234"
  }
}
```

Поддерживаемые `source_type`:

- `hot_arxiv`
- `cold_object`
- `cold_dataset`

## FastAPI endpoints

- `POST /pipeline/v2/run-once`
- `POST /pipeline/v2/run-continuous`

Параметр `profile` можно передать в query-string, иначе используется `PIPELINE_PROFILE`.

## Environment

См. `core/.env.example`:

- `PIPELINE_PROFILE`
- `PIPELINE_CONFIG_PATH`
- `COLD_MINIO_*`
- `COLD_DATASET_ROOT`

