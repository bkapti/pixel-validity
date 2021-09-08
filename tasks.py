import pandas as pd
import aiohttp
import asyncio

number_of_urls_processed = 0
number_ok = 0
number_failed = 0
number_exceptions = 0
df_failed = pd.DataFrame(columns={"tactic_id", "url"})
df_valid = pd.DataFrame(columns={"tactic_id", "url"})
df_exceptions = pd.DataFrame(columns={"tactic_id", "url", "exception"})


def ingest_file(file_path: str) -> pd.DataFrame:
    """
    read csv file from the file_path passed as argument, take only relevant columns.

    This function will stop the program if required columns are not available in the dataset.

    :param file_path: File path needs to be a csv file that must have "tactic_id" and "impression_pixel_json"
    columns available exactly as given here.
    :return: A dataframe with "tactic_id" and "impression_pixel_json" columns will be returned.
    """

    try:
        pd.read_csv(file_path)
    except FileNotFoundError:
        print("Argument passed as the file_path doesn't point to a csv file.")
        exit()

    tactic_df = pd.read_csv(file_path)

    try:
        tactic_df = tactic_df[["tactic_id", "impression_pixel_json"]]
    except KeyError:
        print(
            f"The csv in the passed file path doesn't have required columns.\nExiting the application."
        )
        exit()

    return tactic_df


def clean_url(url: str) -> list:
    """
    This function is specifically designed to clean the "impression_pixel_json" column in tactic datasets.

    It catches NULL, nan values, empty strings and converts them to empty list. For all other values, this function
    will remove backslashes and double quotes. It will remove the first and last character as they are brackets "[", "]"
    and then it will split the value by comma "," and save urls in a list.

    :param url: "impression_pixel_json" column of the tactic dataset.
    :return: will return a list of urls.
    """

    # handle nan values and create an empty list
    if str(url) == "nan" or str(url) == "[]" or str(url) == "" or str(url) == "NULL":
        return []
    # handle backslashes, double quotes, brackets and create a list by splitting
    # remove brackets if they exist at the beginning and/or end of the url variable
    else:
        if str(url)[0] == "[" and str(url[len(url) - 1]) == "]":
            return (str(url).replace("\\", "").replace('"', ""))[1:-1].split(",")
        else:
            return (str(url).replace("\\", "").replace('"', "")).split(",")


async def fire_up_pixel(session, url, tactic_id) -> None:
    """
    This function makes an HTTP GET request for provided URL and updates the global variables based on the response.

    :param session: This is the shared session that's created for all tasks with aiohttp.ClientSession.
    :param url: This is the url to be used for HTTP get request.
    :param tactic_id: This is the tactic_id for each url
    :return: None. This function updates global variables as it's execution is completed.
    """

    global number_ok, number_failed, number_exceptions, number_of_urls_processed, df_failed, df_valid, df_exceptions
    try:
        async with session.get(url) as response:
            number_of_urls_processed += 1
            if 200 <= response.status < 400:
                number_ok += 1
                df_valid = df_valid.append(
                    {"tactic_id": tactic_id, "url": url}, ignore_index=True
                )
            elif 400 <= response.status < 600:
                number_failed += 1
                df_failed = df_failed.append(
                    {"tactic_id": tactic_id, "url": url}, ignore_index=True
                )
                print(f"Failed url tactic_id: {tactic_id} | url: {url}")
            else:
                print(f"Response received is not between 200 and 599 for url: {url}")
    except Exception as e:
        number_exceptions += 1
        df_exceptions = df_exceptions.append(
            {"tactic_id": tactic_id, "url": url, "exception": e}, ignore_index=True
        )


async def validate_pixels(df: pd.DataFrame) -> None:
    """
    This function uses concurrency with the help of asyncio module and creates and initiates the tasks to be executed.

    :param df: This dataframe needs to have "url" and "tactic_id" columns to be processed.
    :return: None
    """

    # create a single session. This will be shared across all tasks.
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in df.iterrows():
            # list of tasks is created using asyncio.ensure_future(). Each task is to fire up the pixels.
            task = asyncio.ensure_future(
                fire_up_pixel(session, row["url"], row["tactic_id"])
            )
            tasks.append(task)
        # session is kept alive until all the tasks are completed by asyncio.gather function.
        await asyncio.gather(*tasks, return_exceptions=True)


def create_df_url(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function processes the tactic data set and creates a url dataframe that has cleaned up urls.

    :param df: This is the tactic dataframe. It needs to have "impression_pixel_json" and "tactic_id" columns.
    :return: A dataframe that has "url" and "tactic_id" columns will be returned.
    """

    # clean "impression_pixel_json" column
    df["pixel_list"] = df["impression_pixel_json"].apply(lambda x: clean_url(x))
    # initiate an empty dataframe for urls and tactic_ids
    df_url = pd.DataFrame(columns=["tactic_id", "url"])
    # iterate the tactic df and create a url df from the list of urls
    for index, tactic in df.iterrows():
        for url_in_list in tactic["pixel_list"]:
            df_url = df_url.append(
                {"tactic_id": tactic["tactic_id"], "url": url_in_list},
                ignore_index=True,
            )
    return df_url


def print_results(duration: float) -> None:
    """
    This function receives duration of the code and it uses global variables to print out the results.

    :param duration: float number as duration of the requests
    :return: doesn't return any value. It prints out results.
    """
    global number_ok, number_failed, number_exceptions, number_of_urls_processed
    print(
        f"{number_of_urls_processed} urls are processed in {duration} seconds.\nHere are the summary of them:"
    )
    print(f"number_ok: {number_ok}")
    print(f"number_failed: {number_failed}")
    print(f"number_of_urls_raising_exceptions: {number_exceptions}")


def write_results_to_csv() -> None:
    """
    This function saves failed and valid urls in the results folder in a csv format.

    :return: None. This function will create csv files in the local results folder.
    """
    global df_valid, df_failed, df_exceptions

    df_valid.to_csv("results/valid_urls.csv")
    df_failed.to_csv("results/failed_urls.csv")
    df_exceptions.to_csv("results/exception_raised_urls.csv")
