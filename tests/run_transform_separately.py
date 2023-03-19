from nr_oaipmh_harvesters.nusl.transformer import NUSLTransformer, vocabulary_cache
from oarepo_oaipmh_harvester.readers.oai_dir import OAIDirReader
from nr_metadata.documents.services.records.schema import NRDocumentRecordSchema
import tqdm
import json
from invenio_app.factory import create_app
import click
import yaml
import traceback


@click.command
@click.option("--from-record", type=int, default=0)
def run(from_record=None):
    app = create_app()
    with open("/tmp/errors.yaml", "w") as errors:
        with app.app_context():
            loader = OAIDirReader(source="../oai-data")
            for entry in tqdm.tqdm(iter(loader)):
                if from_record > 0:
                    from_record -= 1
                    continue
                try:
                    transformer = NUSLTransformer()
                    transformer.apply_batch([entry])
                    # validate it here !!!
                    schema = NRDocumentRecordSchema()
                    schema.load(entry.entry)
                except Exception as e:
                    print(json.dumps(entry.entry, indent=4, ensure_ascii=False))
                    traceback.print_exc()
                    print("---", file=errors)
                    yaml.safe_dump(
                        {
                            "entry": entry.entry,
                            "context": entry.context,
                            "transformed": getattr(entry, "transformed", None),
                            "error": str(e),
                            "stack": traceback.format_exc(),
                        },
                        errors,
                    )


if __name__ == "__main__":
    run()
