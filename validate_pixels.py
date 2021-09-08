import asyncio
import time
from tasks import (
    ingest_file,
    validate_pixels,
    create_df_url,
    print_results,
    write_results_to_csv,
)
import sys


if __name__ == "__main__":

    # read file from the path passed as the argument
    df = ingest_file(file_path=sys.argv[1])

    # clean impression_pixel_json field in the data frame
    df_url = create_df_url(df)

    # start the time to measure duration of the process
    start_time = time.time()

    # validate pixels by firing them up using concurrency via asyncio
    asyncio.run(validate_pixels(df_url))

    # end the time and print duration in seconds
    end_time = time.time()
    duration = end_time - start_time
    # print summary of results and save failed and valid urls in a csv
    print_results(duration)
    write_results_to_csv()
