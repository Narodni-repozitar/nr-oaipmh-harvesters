# NR OAI-PMH harvesters

This package provides oai-pmh transformers for harvesting external repositories.
The following repositories are currently supported (other might work with the same transformers):

* NUSL - national repository of grey literature

```bash
invenio oarepo oai harvester add nusl \
    --name "NUSL harvester" \
    --url http://invenio.nusl.cz/oai2d/ \
    --set global \
    --prefix marcxml \
    --loader sickle \
    --transformer marcxml \
    --transformer nusl \
    --writer 'service{service=nr_documents}'
```