from dagster import asset

@asset
def orders(context) -> None:
    context.log.info("Checking the freshness of the average orders asset")