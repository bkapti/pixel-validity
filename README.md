# Validate Pixels

This program will:
* read urls from a dataset, 
* make an HTTP GET request for each url using concurrency, 
* print out the summary of the results,
* print out failed urls with respective tactic_ids,
* create three new csv files in the local /results folder:
  * failed_urls.csv file will include urls that received a response between 400 and 599 (both included),
  * valid_urls.csv file will include urls that received a response between 200 and 399 (both included),
  * na_urls.csv file will include urls that raised an exception during HTTP GET request.

# Purpose: 

You have a tactic file with "impression_pixel_json" and "tactic_id" field, and you want to check if all the pixels are
valid

# Setup Instructions:
In order to run this code, you need to have Python install. The version used building this program is 3.8.2.
You also need to have pip installed. The version used building this program is 21.1.2.
Then create a folder where you want to have this code installed and run below command:

```git clone https://github.com/bkapti/pixel-validity.git```

It's recommended to create a virtual environment to install all required modules. 
For this run:

```python3 -m venv ENV_DIR```

```source ./ENV_DIR/bin/activate```

Once you run above code, you'll see the name of the ENV_DIR appear at the beginning of the path in terminal.

```(ENV_DIR)$ ```

install required packages by running: (cd into /pixel-validity)

```pip install -r requirements.txt```

To deactivate the virtual environment just run: 

```$ deactivate```

Note: Anytime you're using tools, you should be running them from the virtual environment you created.

# How to run the program
You should run the program from terminal with a system parameter which is the file path for the tactic file you'd like
to check for pixel's validness.

For example: 

if you're in ~/some-path/pixel-validity directory and you have your tactic.csv file in your Desktop and you're on mac:

```python validate_pixels.py ~/Desktop/tactic.csv```

# Additional Functionalities
* This program will check the duration of the code for making requests and print it out.
* This program will create csv files for valid, failed and exception raised urls for further debugging.

# Future Developments
* Introduce logging for exceptions instead of csvs
* Introduce validity checking for urls that raise an exception
