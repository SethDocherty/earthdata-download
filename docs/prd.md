# EarthData Download Tool

## Overview

The EarthData Download Tool is a Python-based application designed to facilitate the downloading of large datasets from EarthData. A user will be need to specify a collection shortname and version number to download the data. The tool will handle the authentication, querying, and downloading of the data, while also providing logging and error handling capabilities. It's expected that the user will have a `.netrc` file in their home directory for authenticating their EarthData account.

## General Process steps

1. Sign-in with Earthaccess account. Requires a `.netrc` file in the home directory. This file should contain the following:
   ```
   machine urs.earthdata.nasa.gov
   login <username>
   password <password>
   ```
   > Note: The username and password should be replaced with your actual EarthData credentials.

2. Query collection using shortname
   > Note: It will be important to pass in the version number. Specify the latest
3. Since the query result will be quite large, we should look into saving it down as a pickle file.
   > Note: This will allow us to avoid re-querying the collection each time we run the script. The pickle file can be loaded back into memory for further processing.
4. We'll need to build collection payload that's a dictionary of granules with a list of https links to download.

The collection payload will be a dictionary of granules with a list of HTTPS links to download. The structure of the payload will be as follows:

   ```json
   {
       "<collection shortname>": {
           "<granule name>": [
               "<URL 1>",
               "<URL 2>",
               ...
           ]
       }
   }
   ```
   
5. Loop through each of the results and grab the download links
   > `results[0].render_dict['umm']["RelatedUrls"]`
    - There are 3 items from this dictionary that we need to grab that match the following types: `GET DATA`, `VIEW RELATED INFORMATION`, `GET RELATED VISUALIZATION`. We will be referencing the HTTPS links; ignore s3
    - Each URL link will be appended to a list.
6. Create dictionary where the key `<granule name>` comes from `results[0].render_dict['umm']['GranuleUR']` and the value assigned is the URL list.
7. After we create our `collection_payload` dictionary, we'll need to donwload all the items by loop through each dictionary.
    - We should save the `collection_payload` dictionary down to disk as a pickle file.  This way we can always access it in between the download sessions.
    - Bouus points if we can paralle process x-number of dictionary items at a time.
8. We'll need to keep a log of all the granules that have been downloaded. This is so we can understand what may need to be reprocessed if there are any failures.
    - Bouns points for creating a dashboard view that gives us some insight into the overall progress; total granules downloaded, total granules left to download, total size of content downloaded in TBs
9. When we need to pause, we should save the `collection_payload` down to disk as a pickle file.  This represents our save state that we can start off where we last were.
     - In the case of failures we should log the event and append the granule dictionary down to disk to a json file.
10. When we need to restart our download process, there are a couple of strategies we can take to restart the process.
    - Check if folder exists and contains 3 files and skip <--- This requires a conditional check as well loop through granules. Will be an issue when we clear the download drive of content.  
    - Remove <granule name> dictionary from `collection_payload` once all files have been downloaded. <---- This may be the best approach as it's just an extra line of code to run at the end of the loop.
    - Maintain a json file of successful downloads. When we loop through the `collection_payload`, we just do a conditional check to see if <granule name> is in the json file list.

## Logging Strategy

* **Handlers**: Console output by default; file-based logging is optional.
* **Verbosity Control**: Configurable logging level per source database from the YAML configuration. By default log level is set to `INFO` for all sources.  This can be overridden in the YAML config file.
* **Log Format**: Timestamped entries in the format:
  `%(asctime)s | %(name)s | %(levelname)s | %(message)s`
* **Log Rotation**: Implement log rotation to manage file sizes and retention.
* **Error Handling**: Capture and log exceptions with stack traces for debugging.
* **Performance Monitoring**: Log download duration for each granule for performance analysis.
* **Log Format**: Use JSON format for structured logging, making it easier to parse and analyze logs.

