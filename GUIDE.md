# Selfbot Setup Guide

This guide explains how to set up and run the selfbot.

## Prerequisites

- Python 3.8 or higher
- MongoDB (running locally or a connection string)

## Installation

1.  **Extract the files**: Unzip the `selfbot_clean.zip` file to a directory of your choice.
2.  **Install dependencies**: Open a terminal in the directory and run:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

1.  **MongoDB**: Ensure you have MongoDB running. The bot defaults to `mongodb://localhost:27017` and database `discord_bot`.
    - If you need to change this, check `utils/database/manager.py` or `mongodb.config.json` (if present).
2.  **Config**: The bot uses `config.json` (or `selfbot.config.json`).
    - Edit `config.json` to add your token if not using the dashboard/hosting manager.
    - If running standalone, you might need to manually set up your `config.json` with:
      ```json
      {
          "token": "YOUR_TOKEN_HERE",
          "prefix": ".",
          "auto_delete": {"enabled": false, "delay": 5},
          "developer_entries": []
      }
      ```
      *(Note: The bot is designed to work with a manager config, but can run standalone)*

## Running the Bot

Run the following command in the terminal:

```bash
python main.py
```

## Features

- **Snipe**: Tracks deleted/edited messages (`.snipe`, `.editsnipe`).
- **Tools**: Various utility commands.
- **Fun**: Fun commands.

## Notes

- **Tracking Removed**: User tracking and Kilo X integration have been completely removed.
- **Message Tracking**: Message sniping functionality is preserved locally in your MongoDB.
