# QSAutomate

Orchestration platform for algorithmic trading strategies with functionality for workflow automation, task scheduling, monitoring, and deployment.

<img width="1631" height="1315" alt="Screenshot 2025-08-11 at 08 54 31" src="https://github.com/user-attachments/assets/1abed6ca-67f6-41e0-941d-c14abc9577c3" />

# 🚨 Disclaimer

This software is for educational purposes only. Use your paper trading account. Do not risk money which you are afraid to lose. USE THE SOFTWARE AT YOUR OWN RISK. THE AUTHORS AND ALL AFFILIATES ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS.

# ⬆️ Installation

## Clone the repo and install the dependencies

> Note: QSAutomate is not designed to be installed like other libraries. It is a collection of files and directories you can use as the foundation for your algorithmic trading strategies. This makes installation even easier.

``` bash
git clone https://github.com/quant-science/QSAutomate.git
```

Then navigate into the `QSAutomate` directory (which is now your working directory) and install the dependencies:

```
pip install -r requirements.txt
```
You can now use the `QSAutomate` directory as your working directory.

# 💸 Paper Trading Account

**Important: Make sure to use a paper trading account to trade risk-free.** 

Here's how to find you account number from inside IB Trader Workstation. 

![image](https://github.com/quant-science/omega/assets/13734662/eba33283-ec31-4287-944f-4e9dff7cbe14)

# 🏃 Quick start

If you know what you're doing, navigate to `qsautomate/trading` and run the following command:

```
python qsmomentum.py
```

If you get an error, read on.

# 🏁 Getting Set up

## Running MLFlow

MLFlow is the tool we use to track experiments. QSAutomate uses QSResearch to automate the end to end research process. As such, we want to make sure MLFlow is running to collect strategy diagnostics. The easiest way to start MLFlow is through the command line. Make sure you have your Quant Lab activiated and MLFlow installed.

To point to a common MLFlow directory where artifacts are stored (recommended):

```bash
mlflow server \
  --port 8031 \
  --backend-store-uri ~/.qsresearch/mlflow/runs \
  --default-artifact-root ~/.qsresearch/mlflow/artifacts
```

This will start MLFlow and point to the directories based on the instructions for QSResearch. You will see some logging print out including the URL and port where the MLFlow server is listenting. If you followed the above instructions, it's here:

[http://127.0.0.1:8031](http://127.0.0.1:8031)

[You can find more options here.](https://mlflow.org/docs/latest/api_reference/cli.html#mlflow-server)

## Running Prefect

[Prefect](https://docs.prefect.io/v3/get-started) is an open-source orchestration engine that turns your Python functions into production-grade data pipelines with minimal friction. You can build and schedule workflows in pure Python—no DSLs or complex config files—and run them anywhere you can run Python. Prefect handles the heavy lifting for you out of the box: automatic state tracking, failure handling, real-time monitoring, and more.

Start the local Prefect server in a new terminal window with your Quant Stack activated:

```
prefect server start
```

This will start Prefect. You will see some logging preint out including the URL and port where the Prefect server is listening. If you followed the above instructions, it's here:

[http://127.0.0.1:4200/](http://127.0.0.1:4200/)

## Viewing and cancelling _flows_

You can review all flow runs in the UI available at the link above. If you want to cancel a _flow_ run, I find using the CLI is the most efficient.

Step 1. List all flow runs.

```prefect flow-run ls```

You'll see a nice table print out with the ID, Flow, Name, State, and when it started. Copy the ID of the flow you want to cancel.

Step 2. Cancel the flow run.

```prefect flow-run cancel <FLOW_RUN_ID>```

Prefect will schedule the flow run for cancellation.

# 💵 Using QSAutomate

QSAutomate is actually a collection of _tasks_ and _flow_ that together orchestrate our end to end trading strategy. Here are the important directories:

```
QSAutomate/                                   - Project root
├─ .env.example                                - Example environment variables
├─ qsautomate/                                 - Main Python package
│  ├─ backtest/                                - Backtesting runners/integration
│  │  └─ zipline_runner.py                     - Zipline-based backtest launcher
│  ├─ data/                                    - Data ingestion and bundling
│  │  ├─ bundle.py                             - Zipline data bundle registration/IO
│  │  └─ fmp.py                                - Prefect tasks to fetch/store FMP data
│  ├─ strategies/                              - Strategy definitions and artifacts
│  │  ├─ qsmomentum.py                         - Momentum strategy config/entrypoint
│  ├─ trading/                                 - Live/exec utilities and portfolio ops
└─ -  └─ rebalance.py                          - Portfolio rebalancing logic
```

QSAutomate is designed to be run at the command line. The entry point are the files in the `strategies` folders (if you want to follow the convention). These files define the Prefect _flow_ which actually orchestrates the _tasks_.

# 🚨 Some Gotchas

Issue: I'm getting an error that says data for my benchmark symbol is not available.
Explanation: You have a cached Zipline bundle that has market data that starts after the start date of your backtest.
Solution: Delete the directories in `~/.zipline/data/historical_prices_fmp` and rerun the bundling process.
