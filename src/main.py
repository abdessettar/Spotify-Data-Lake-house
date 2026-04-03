import typer
from typing import Annotated

# Initialize the Typer app then define the commands
app = typer.Typer()


@app.command()
def ingest():
    """
    Run the main data ingestion pipeline to fetch recent data from Spotify.
    """
    from src.pipelines.ingestion import IngestionPipeline

    IngestionPipeline().run()


@app.command()
def backfill(
    data_dir: Annotated[
        str, typer.Option(help="Path to the folder containing Spotify export JSONs")
    ],
):
    """
    Upload historical Spotify export files to the Bronze data lake.
    """
    from src.pipelines.backfill import BackfillPipeline

    BackfillPipeline().run(data_dir=data_dir)


@app.command()
def transform():
    """
    Transform raw Bronze JSON files into clean Silver Parquet tables.
    """
    from src.pipelines.silver import SilverPipeline

    SilverPipeline().run()


@app.command()
def enrich(
    batch_size: int = typer.Option(50, help="Number of tracks to search in this run"),
):
    """
    Search Spotify to find missing track IDs for historical export data.
    This one runs locally and is not part of the scheduled Azure jobs.
    It makes a lot of API calls and can run for days for our decade-long historical data.
    Spotify's restrictions on API rate limits make it almost useless.
    """
    from src.pipelines.enrichment import EnrichmentPipeline

    EnrichmentPipeline().run(batch_size=batch_size)


@app.command(name="enrich-lastfm")
def enrich_lastfm(
    batch_size: int = typer.Option(200, help="Number of tracks to fetch in this run"),
):
    """
    Fetch Last.fm tags and popularity data for tracks in the Silver catalog.
    Last.fm's API is very reliable and can scrap everything (10000s requests) in one run.
    """
    from src.pipelines.lastfm_enrichment import LastfmEnrichmentPipeline

    LastfmEnrichmentPipeline().run(batch_size=batch_size)


@app.command(name="enrich-dumps")
def enrich_dumps():
    """
    Enrich Silver tables from local Spotify catalogue dumps (no API calls).
    Reads data/dumps/*.parquet and writes enriched Silver tables to Azure.
    Not part of the scheduled Azure jobs: run locally after receiving new dumps.
    We can move it to the cloud, but the dumps are very large (almost 100GB all).
    """
    from src.pipelines.dump_enrichment import DumpEnrichmentPipeline

    DumpEnrichmentPipeline().run()


@app.command(name="gold")
def gold():
    """
    Build the Gold analytics layer from Silver tables.
    Produces wide fact and dimension tables plus pre-computed aggregations.
    """
    from src.pipelines.gold import GoldPipeline

    GoldPipeline().run()


@app.command(name="transform-gold")
def transform_gold():
    """
    Run transform followed by gold (for scheduled execution in Azure).
    Rebuilds Silver tables from Bronze, then materialises the Gold analytics layer.
    """
    from src.pipelines.silver import SilverPipeline
    from src.pipelines.gold import GoldPipeline

    SilverPipeline().run()
    GoldPipeline().run()


@app.command(name="run-all")
def run_all():
    """
    Run ingest followed by transform (for scheduled execution).
    """
    from src.pipelines.ingestion import IngestionPipeline
    from src.pipelines.silver import SilverPipeline

    IngestionPipeline().run()
    SilverPipeline().run()


if __name__ == "__main__":
    app()
