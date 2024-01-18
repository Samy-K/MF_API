# MeteoFrance API Client

## Description
This Python project includes a client for interacting with the MeteoFrance API. It allows users to authenticate, retrieve lists of weather stations by department, select a specific station, order and download **hourly** weather data, and view station information. The code is structured around a `Client` class that handles API requests and responses.

## Features
- OAuth2 authentication with the MeteoFrance API.
- Retrieve a list of weather stations for a specified department.
- Select and view detailed information about a weather station.
- Order and download weather data for a specified time period.

## Installation
To use this project, clone the repository and install the required dependencies.

```bash
git clone https://github.com/Samy-K/MF_API_CLIENT.git
cd MF_API_CLIENT
pip install -r requirements.txt
```

## Usage
Update the **API_config.txt** file with your MeteoFrance API credentials.
Run the script:
```bash
python main.py
```
Follow the on-screen prompts to select a department, weather station, and data period.

## Dependencies
- Python 3.11
- requests - For making HTTP requests to the API.
- pandas - For data manipulation and analysis.
- datetime - For handling date and time objects.
- configparser - For reading configuration files.

## Configuration
Before running the script, ensure you have a valid **API_config.txt** file in your project directory with the following format:
```css
[Parameters]
APPLICATION_ID = [Your Application ID]
DATA_SERVER = [API Base URL]
TOKEN = [Your API Key]
```

## Contributing
Contributions to this project are welcome. Please fork the repository and submit a pull request with your proposed changes.

## License
This project is licensed under the Apache-2.0 license. See the LICENSE file for more details.
